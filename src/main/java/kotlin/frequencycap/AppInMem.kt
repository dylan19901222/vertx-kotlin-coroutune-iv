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

class AppInMem : CoroutineVerticle() {

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
//		this.generatorAdvertisement(amount = 10000, maxCapIntervalMin = 20, maxCapNum = 3)
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
			.listenAwait(config.getInteger("http.port", 8081))

	}

	// get advertisement
	suspend fun advertisement(ctx: RoutingContext) {
		try {
			val userId: String = ctx.getBodyAsJson().getString("userId")
			var exprUserIdkey: String = userId + "expr"

			//取出 shardData 中的 UserAdLogHolder
			val userAdLogHolder: UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Long>> =
				getUserAdLogHolder()

			var userAdLogByUserIdMap: ConcurrentHashMap<Id<Advertisement>, Long> = ConcurrentHashMap()

			//判斷 userAdLogHolder 中是否已有這位 userId 的期限資訊
			if (userAdLogHolder.containsKey(exprUserIdkey)) {
				userAdLogByUserIdMap = userAdLogHolder.get(exprUserIdkey)
			}

			//如使用者廣告期限紀錄已過期，移除此期限紀錄
			userAdLogByUserIdMap.forEach { entry ->
				if (entry.value.compareTo(System.currentTimeMillis()) < 1) {
					userAdLogByUserIdMap.remove(entry.key)
				}
			}

			val adLogExpAdIds: List<Id<Advertisement>> =
				userAdLogByUserIdMap.map { entry -> entry.key }.toList()

			//過濾目前不可用的廣告
			val resultList: List<Advertisement> = adList.filter { ad -> !adLogExpAdIds.contains(ad._id) }.toList()

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
					reachedCapLimit = currNum.compareTo(ad.capNum) == 0
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
				val cMap = ConcurrentHashMap<Id<Advertisement>, Long>()
				cMap.put(adId, 1)
				putUserAdLogToUserAdLogHolder(userId, cMap)
			}

			//如使用者廣告期限紀錄沒有此筆使用者對應的廣告資料，新增此筆使用者廣告期限紀錄
			if (reachedCapLimit) {
				val cal: Calendar = Calendar.getInstance()
				cal.add(Calendar.MINUTE, ad.capIntervalMin)
				userAdLogByUserIdMap.put(adId, cal.timeInMillis)
				putUserAdLogToUserAdLogHolder(exprUserIdkey,userAdLogByUserIdMap)
			}
			
			ctx.response().end(json {
				obj("title" to ad.title, "url" to ad.url).encode()
			})
		} catch (e: Exception) {
			e.printStackTrace()
			ctx.response().setStatusCode(404).end()
		}
	}

	suspend fun getShardData(): MutableMap<String, UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Long>>> {
		val sd: SharedData = vertx.sharedData()
		val shardData =
			sd.getLocalMap<String, UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Long>>>(
				LOCAL_MAP_NAME
			)

		return shardData
	}

	suspend fun getUserAdLogHolder(): UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Long>> {
		var shardData = getShardData()
		if (shardData.containsKey(USER_AD_LOG_HOLDER_NAME)) {
			return shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		} else {
			shardData.put(USER_AD_LOG_HOLDER_NAME, UserAdLogHolder())
			return shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		}
	}

	suspend fun putUserAdLogToUserAdLogHolder(key: String, value: ConcurrentHashMap<Id<Advertisement>, Long>) {
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
