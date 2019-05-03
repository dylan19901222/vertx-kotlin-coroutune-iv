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

class App : CoroutineVerticle() {

	private lateinit var client: CoroutineClient
	private lateinit var adCon: CoroutineCollection<Advertisement>
	private lateinit var userAdLogExpCon: CoroutineCollection<UserAdLogExp>
	private lateinit var adList: List<Advertisement> 
	private val findAdTaskDelay = 60000L
	private lateinit var userAdLogMap: MutableMap<String, MutableMap<Id<Advertisement>, Int>>

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
		userAdLogMap = mutableMapOf()
		client = KmogoVertxManager(vertx, "ds").createShared()
		val database: CoroutineDatabase = client.getDatabase("test")
		adCon = database.getCollection<Advertisement>()
		userAdLogExpCon = database.getCollection<UserAdLogExp>()

		this.generatorAdvertisement(10000, 5, 3)
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
			.listenAwait(config.getInteger("http.port", 8080))

	}

	// get advertisement
	suspend fun advertisement(ctx: RoutingContext) {
		try {
			val userId: String = ctx.getBodyAsJson().getString("userId")

			//以使用者找出所有使用者對應廣告log
			val userAdLogExpList: List<UserAdLogExp> = userAdLogExpCon.find(UserAdLogExp::userId eq userId).toList()

			//過濾掉已不能使用的廣告並轉換成 id List
			val values: List<Id<Advertisement>> = userAdLogExpList.map { userAdLogExp -> userAdLogExp.adId }.toList()
			
			val resultList: List<Advertisement> = adList.filter { ad -> !values.contains(ad._id) }.toList()

			//如果沒廣告可播放回傳404
			if (resultList.size == 0) {
				ctx.response().setStatusCode(404).end()
				return
			}
			val ad: Advertisement = resultList.get((0..resultList.size - 1).random())
			val adId = ad._id
			var enable = ad.capNum <= 1
			
			//判別此使用者是否已有此則廣告使用狀況，如有，將 currentCapNum + 1 及判斷是否超過可使用量，如沒有，新增使用者對應廣告log
			if (userAdLogMap.contains(userId) && ad.capNum > 1) {
				var innerMap = userAdLogMap.get(userId)!!
				if (innerMap.contains(adId)) {
					val currNum = innerMap.get(adId)!! + 1
					innerMap.put(adId, currNum)
					enable = currNum == ad.capNum
				} else {
					innerMap.put(adId, 1)
				}
				if(enable){
					innerMap.remove(adId)
				}
			} else {
				userAdLogMap.put(userId, mutableMapOf<Id<Advertisement>, Int>(adId to 1))
			}
			
//			println(userAdLogMap)
//			println(enable)
			//如已不能使用，將此筆 log 加入資料庫
			if (enable) {
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
