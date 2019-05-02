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
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.frequencycap.KmogoVertxManager


class App : CoroutineVerticle() {

	private lateinit var client: CoroutineClient
	private lateinit var adCon: CoroutineCollection<Advertisement>
	private lateinit var userAdLogCon: CoroutineCollection<UserAdLog>

	data class Advertisement(
		@BsonId val _id: Id<Advertisement> = newId(),
		val title: String,
		val url: String,
		val capIntervalMin: Int,
		val capNum: Int
	) {
		constructor(title: String, url: String, capIntervalMin: Int, capNum: Int) : this(
			newId(),
			title,
			url,
			capIntervalMin,
			capNum
		)

	}

	data class UserAdLog(
		@BsonId val _id: Id<Advertisement> = newId(),
		val adId: Id<Advertisement>,
		val userId: String,
		val currNum: Int,
		val expireAt: Calendar,
		val enable: Boolean
	) {
		constructor(
			adId: Id<Advertisement>,
			userId: String,
			currNum: Int,
			expireAt: Calendar,
			enable: Boolean
		) : this(
			newId(),
			adId,
			userId,
			currNum,
			expireAt,
			enable
		)

	}

	override suspend fun start() {
		client = KmogoVertxManager(vertx, "ds").createShared()
		val database: CoroutineDatabase = client.getDatabase("test")
		adCon = database.getCollection<Advertisement>()
		userAdLogCon = database.getCollection<UserAdLog>()

		this.generatorAdvertisement(10,5,3)
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
			val userAdLogList: List<UserAdLog> = userAdLogCon.find(UserAdLog::userId eq userId).toList()

			//轉換成以廣告id作為key鍵分群
			val groupByAdIdMap = userAdLogList.groupBy { ua -> ua.adId }

			//過濾掉已不能使用的廣告並轉換成 id List
			val values: List<Id<Advertisement>> =
				userAdLogList.filter { userAdLog -> !userAdLog.enable }.map { userAdLog -> userAdLog.adId }.toList()

			//亂數取一則廣告，不包含不可取用的廣告
			/** mongo command sample
			db.advertisement.aggregate(
			{$match: { _id : { $nin : [ObjectId("5cca97ac3bdd2e014cab256c")] }}},
			{$sample: { size: 1 }}
			)
			 **/
			var bsonList: MutableList<Bson> = mutableListOf<Bson>()
			bsonList.add(match(Advertisement::_id nin values))
			bsonList.add(sample(1))
			val resultList: List<Advertisement> = adCon.aggregate<Advertisement>(bsonList).toList()

			//如果沒廣告可播放回傳404
			if (resultList.size == 0) {
				ctx.response().setStatusCode(404).end()
				return
			}
			val ad: Advertisement = resultList.get(0)
			val adId = ad._id

			//判別此使用者是否已有此則廣告使用狀況，如有，將 currentCapNum + 1 及判斷是否超過可使用量，如沒有，新增使用者對應廣告log
			if (groupByAdIdMap.contains(adId)) {
				val userAdLog: UserAdLog = groupByAdIdMap.get(adId)!!.get(0)
				val userAdLogId = userAdLog._id
				val currNum = userAdLog.currNum + 1
				val enable = currNum < ad.capNum
				userAdLogCon.updateOne(
					"{ _id : { \$eq : ObjectId(\"$userAdLogId\") } }",
					"{ \$set : { enable : $enable , currNum : $currNum }}"
					, upsert()
				)
			} else {
				val cal: Calendar = Calendar.getInstance()
				cal.add(Calendar.MINUTE, ad.capIntervalMin)
				userAdLogCon.insertOne(UserAdLog(ad._id, userId, 1, cal, ad.capNum > 1))
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
		//設置過期index，在mongodb中每分鐘會檢查一次
		val indexOption: IndexOptions = IndexOptions().expireAfter(0, TimeUnit.MICROSECONDS)
		userAdLogCon.createIndex("{ expireAt: 1 }", indexOption)
		userAdLogCon.createIndex("{ userId : 1 }")
		
		adCon.drop()
		userAdLogCon.drop()
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
