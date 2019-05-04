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
import io.vertx.core.json.JsonArray

class AppRedis : CoroutineVerticle() {

	private lateinit var client: CoroutineClient
	private lateinit var adCon: CoroutineCollection<Advertisement>
	private lateinit var userAdLogExpCon: CoroutineCollection<UserAdLogExp>
	private lateinit var adList: List<Advertisement>
	private val findAdTaskDelay = 60000L
	private lateinit var redisClient : RedisClient
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
		createRedisClient()
		client = KmogoVertxManager(vertx, "ds").createShared()
		val database: CoroutineDatabase = client.getDatabase("test")
		adCon = database.getCollection<Advertisement>()
		userAdLogExpCon = database.getCollection<UserAdLogExp>()

		//測試資料生成
		this.generatorAdvertisement(3, 5, 5)
		adList = adCon.find().toList()
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

			//redis 改法
			val mutableList = findUserAdLogExpire(userId)
			val values: List<String> = mutableList.map { x -> x.toString().split("∥")[1] }.toList()
			val resultList: List<Advertisement> = adList.filter { ad -> !values.contains(ad._id.toString()) }.toList()

			//如果沒廣告可播放回傳404
			if (resultList.size == 0) {
				ctx.response().setStatusCode(404).end()
				return
			}
			val ad: Advertisement = resultList.get((0..resultList.size - 1).random())
			val adId = ad._id
			var enable = ad.capNum <= 1

			//判別此使用者是否已有此則廣告使用狀況，如有，將 currentCapNum + 1 及判斷是否超過可使用量，如沒有，新增使用者對應廣告log
			if (getUserAdLogMap().containsKey(userId) && ad.capNum > 1) {
				var innerMap = getUserAdLogMap().get(userId)
				if (innerMap.containsKey(adId)) {
					val currNum = innerMap.get(adId)!! + 1
					innerMap.put(adId, currNum)
					enable = currNum == ad.capNum
				} else {
					innerMap.put(adId, 1)
				}
				if (enable) {
					innerMap.remove(adId)
				}
			} else {
				val cMap = ConcurrentHashMap<Id<Advertisement>, Int>()
				cMap.put(adId, 1)
				putUserAdLogMap(userId, cMap)
			}

			//如已不能使用，將此筆 log 加入資料庫
			if (enable) {
				//redis寫法
				setUserAdLogExpire(userId +"∥"+ ad._id.toString(), ad.capIntervalMin * 60L)
			}

			ctx.response().end(json {
				obj("title" to ad.title, "url" to ad.url).encode()
			})
		} catch (e: Exception) {
			e.printStackTrace()
			ctx.response().setStatusCode(404).end()
		}
	}

	suspend fun createRedisClient() {
		// If a config file is set, read the host and port.
		var host = Vertx.currentContext().config().getString("host")
		if (host == null) {
			host = "127.0.0.1"
		}

		// Create the redis client
		redisClient = RedisClient.create(
			vertx, RedisOptions(
				host = host
			)
		)
		
	}

	suspend fun findUserAdLogExpire(userId: String): JsonArray {
		return redisClient.keysAwait(userId + "*")
	}

	suspend fun setUserAdLogExpire(key: String, sec: Long) {
		redisClient.setAwait(key, key)
		redisClient.expireAwait(key, sec)
	}

	suspend fun getShardData(): MutableMap<String, UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>>> {
		val sd: SharedData = vertx.sharedData()
		val shardData =
			sd.getLocalMap<String, UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>>>(
				LOCAL_MAP_NAME
			)

		return shardData
	}

	suspend fun getUserAdLogMap(): UserAdLogHolder<String, ConcurrentHashMap<Id<Advertisement>, Int>> {
		var shardData = getShardData()
		if (shardData.containsKey(USER_AD_LOG_HOLDER_NAME)) {
			return shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		} else {
			shardData.put(USER_AD_LOG_HOLDER_NAME, UserAdLogHolder())
			return shardData.get(USER_AD_LOG_HOLDER_NAME)!!
		}
	}

	suspend fun putUserAdLogMap(key: String, value: ConcurrentHashMap<Id<Advertisement>, Int>) {
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
					(3..maxCapIntervalMin).random(),
					(3..maxCapNum).random()
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
