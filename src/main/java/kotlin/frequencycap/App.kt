package frequencycap

import com.mongodb.client.model.IndexOptions
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.conversions.Bson
import org.litote.kmongo.*
import org.litote.kmongo.coroutine.CoroutineClient
import org.litote.kmongo.coroutine.CoroutineCollection
import org.litote.kmongo.coroutine.CoroutineDatabase
import java.util.concurrent.TimeUnit
import kotlin.frequencycap.KmogoVertxManager
import kotlin.concurrent.schedule
import org.litote.kmongo.coroutine.*
import kotlinx.coroutines.channels.*
import java.time.LocalDateTime
import java.util.Calendar
import io.vertx.core.shareddata.AsyncMap
import io.vertx.core.impl.VertxInternal
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
import io.vertx.core.shareddata.SharedData
import kotlin.frequencycap.UserAdLogHolder
import java.util.concurrent.ConcurrentHashMap
import io.vertx.core.Vertx
import io.vertx.kotlin.redis.*
import io.vertx.redis.RedisClient
import io.vertx.redis.RedisOptions

class App : CoroutineVerticle() {

	private lateinit var mongoClient: CoroutineClient
	private lateinit var adCon: CoroutineCollection<Advertisement>
	private lateinit var userAdLogExpCon: CoroutineCollection<UserAdLogExp>
	private lateinit var adList: List<Advertisement>
	private val findAdTaskDelay = 60000L
	private final val LOCAL_MAP_NAME: String = "__vertx.localMap"
	private final val USER_AD_LOG_HOLDER_NAME: String = "__vertx.userAdLogHolder"


	data class Advertisement(
		@BsonId val _id: Id<Advertisement> = newId(),
		val title: String,
		val url: String,
		val capIntervalMin: Int,
		val capNum: Int
	) {
		constructor(title: String, url: String, capIntervalMin: Int, capNum: Int) :
				this(newId(), title, url, capIntervalMin, capNum)
	}

	data class UserAdLogExp(
		@BsonId val _id: Id<Advertisement> = newId(),
		val adId: Id<Advertisement>,
		val userId: String,
		val expireAt: Calendar
	) {
		constructor(adId: Id<Advertisement>, userId: String, expireAt: Calendar) :
				this(newId(), adId, userId, expireAt)
	}

	data class UserAdLog(val adId: Id<Advertisement>, val currNum: Int)

	override suspend fun start() {

		mongoClient = KmogoVertxManager(vertx, "ds").createShared()
		val database: CoroutineDatabase = mongoClient.getDatabase("test")
		adCon = database.getCollection<Advertisement>()
		userAdLogExpCon = database.getCollection<UserAdLogExp>()

		//測試資料生成
		this.generatorAdvertisement(amount = 10000, maxCapIntervalMin = 30, maxCapNum = 3)
		//每1分鐘重撈廣告資料，將廣告資料暫存記憶體，減少 io 存取
		val tickerChannel = ticker(delayMillis = findAdTaskDelay, initialDelayMillis = 0)
		launch {
			for (event in tickerChannel) {
				adList = adCon.find().toList()
			}
		}

		val router = Router.router(vertx)
		router.route().handler(BodyHandler.create());
		router.post("/advertisement").coroutineHandler { ctx -> advertisement(ctx) }

		// Start the server
		vertx.createHttpServer()
			.requestHandler(router)
			.listenAwait(config.getInteger("http.port", 8080))

	}

	// get advertisement
	suspend fun advertisement(ctx: RoutingContext) {
		try {
			val userId: String = ctx.getBodyAsJson().getString("userId")

			//取出 shardData 中的 UserAdLogHolder
			val userAdLogHolder: UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>> =
				getUserAdLogHolder()

			//以使用者找出所有使用者廣告期限紀錄
			val userAdLogExpList: List<UserAdLogExp> = userAdLogExpCon.find(UserAdLogExp::userId eq userId).toList()

			//將使用者廣告期限紀錄轉換成廣告 Id List
			val adLogExpAdIds: List<Id<Advertisement>> =
				userAdLogExpList.map { userAdLogExp -> userAdLogExp.adId }.toList()

			var userAdLogByUserIdMap: ConcurrentHashMap<Id<Advertisement>, Int> = ConcurrentHashMap()

			//判斷 userAdLogHolder 中是否已有這位 userId
			if (userAdLogHolder.containsKey(userId)) {
				userAdLogByUserIdMap = userAdLogHolder.get(userId)
			}
			
			//先將有此 adId ，而使用者廣告期限紀錄卻已過期的使用者廣告行為記錄移除(重新計算)
			userAdLogByUserIdMap.forEach { entry ->
				if (!adLogExpAdIds.contains(entry.key)) {
					userAdLogByUserIdMap.remove(entry.key)
				}
			}

			//1.如使用者廣告期限紀錄不含此 adId ， 使用者廣告行為記錄不含有此 adId 表示該廣告可以用[x,x -> o]
			//2.如使用者廣告期限紀錄含此 adId ， 使用者廣告行為記錄含有此 adId 表示該廣告可以用[o,o -> o]
			//3.如使用者廣告期限紀錄含此 adId ， 使用者廣告行為記錄不含有此 adId 表示該廣告不可以用[x,o -> x]
			//因為使用者廣告行為記錄超過該廣告使用次數會被移除，但使用者廣告期限紀錄並還沒到時間，故上面條件3是不可使用的狀況
			val resultList: List<Advertisement> =
				adList.filter { ad ->
					!(adLogExpAdIds.contains(ad._id).xor(userAdLogByUserIdMap.containsKey(ad._id)))
				}.toList()

			//如果沒廣告可播放回傳404
			if (resultList.size == 0) {
				ctx.response().setStatusCode(404).end()
				return
			}

			//亂數取一筆廣告
			val ad: Advertisement = resultList.get((0..resultList.size - 1).random())
			val adId = ad._id
			var reachedCapLimit = ad.capNum <= 1

			//判別此使用者是否已有此則使用者廣告行為記錄
			//如有，將 currentCapNum + 1 及 判斷是否超過可使用量
			//如沒有，新增此則使用者廣告行為記錄
			if (userAdLogHolder.containsKey(userId)) {
				var innerMap = userAdLogHolder.get(userId)
				if (innerMap.containsKey(adId)) {
					val currNum = innerMap.get(adId)!! + 1
					innerMap.put(adId, currNum)
					reachedCapLimit = currNum == ad.capNum
				} else {
					innerMap.put(adId, 1)
				}
				if (reachedCapLimit) {
					innerMap.remove(adId)
				}
				if (innerMap.count() == 0) {
					userAdLogHolder.remove(userId)
				}
			} else if (ad.capNum > 1) {
				val cMap = ConcurrentHashMap<Id<Advertisement>, Int>()
				cMap.put(adId, 1)
				putUserAdLogToUserAdLogHolder(userId, cMap)
			}

			//如使用者廣告期限紀錄沒有此筆使用者對應的廣告資料，新增此筆使用者廣告期限紀錄
			if (!adLogExpAdIds.contains(adId)) {
				val cal: Calendar = Calendar.getInstance()
				cal.add(Calendar.MINUTE, ad.capIntervalMin)
				userAdLogExpCon.insertOne(UserAdLogExp(ad._id, userId, cal))
			}

			ctx.response().end(json {
				obj("title" to ad.title, "url" to ad.url).encode()
			})
		} catch (e: Exception) {
			e.printStackTrace()
			ctx.response().setStatusCode(404).end()
		}
	}

	suspend fun getShardData(): MutableMap<String, UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>>> {
		val sd: SharedData = vertx.sharedData()
		val shardData =
			sd.getLocalMap<String, UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>>>(
				LOCAL_MAP_NAME
			)

		return shardData
	}

	suspend fun getUserAdLogHolder(): UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>> {
		var shardData = getShardData()
		if (shardData.containsKey(USER_AD_LOG_HOLDER_NAME)) {
			return shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		} else {
			shardData.put(USER_AD_LOG_HOLDER_NAME, UserAdLogHolder())
			return shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		}
	}

	suspend fun putUserAdLogToUserAdLogHolder(key: String, value: ConcurrentHashMap<Id<Advertisement>, Int>) {
		val shardData = getShardData()
		val holder = shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		holder.put(key, value)
		shardData.put(USER_AD_LOG_HOLDER_NAME, holder)
	}


	//generator ad test data
	suspend fun generatorAdvertisement(amount: Long, maxCapIntervalMin: Int, maxCapNum: Int) {
		adCon.drop()
		userAdLogExpCon.drop()
		//設置過期index，在mongodb中每分鐘會檢查一次
		val indexOption: IndexOptions = IndexOptions().expireAfter(0, TimeUnit.MICROSECONDS)
		userAdLogExpCon.createIndex("{ expireAt: 1 }", indexOption)
		userAdLogExpCon.createIndex("{ userId : 1 }")
		for (i in 1..amount) {
			adCon.insertOne(
				Advertisement(
					"Go check it out " + i,
					"https://domain.com/landing_page" + i,
					(1..maxCapIntervalMin).random(),
					(1..maxCapNum).random()
				)
			)
		}
	}


	/**
	 * An extension method for simplifying coroutines usage with Vert.x Web routers
	 */
	fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit) {
		handler { ctx ->
			launch(ctx.vertx().dispatcher()) {
				try {
					fn(ctx)
				} catch (e: Exception) {
					ctx.fail(e)
				}
			}
		}
	}
}
