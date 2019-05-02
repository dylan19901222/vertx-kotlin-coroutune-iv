package movierating

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.frequencycap.KmogoVertxManager

@RunWith(VertxUnitRunner::class)
class AppTest {

	@Rule
	@JvmField
	val rule = RunTestOnContext()

	private lateinit var vertx: Vertx
	private lateinit var client: WebClient

	@Before
	fun before() {
		vertx = rule.vertx()
		client = WebClient.create(vertx);
	}


	@Test
	fun `test advertisement API`(testContext: TestContext) {
		val async: Async = testContext.async()
		vertx.deployVerticle("frequencycap.App",
			DeploymentOptions().setWorker(true),
			testContext.asyncAssertSuccess {
				client.post(8080, "localhost", "/advertisement")
					.sendJson(json { obj("userId" to "b8143b3a-4815-47aa-8b10-8086d3330ade") }, { ar ->
						assertTrue(ar.succeeded())
						var response: HttpResponse<Buffer> = ar.result();
						assertEquals(response.statusCode(), 200)
						async.complete()
					})
			})
	}

//	@Test
//	fun `gen advertisement data`(testContext: TestContext) {
//		val async: Async = testContext.async()
//		vertx.deployVerticle("frequencycap.App",
//			DeploymentOptions().setWorker(true),
//			testContext.asyncAssertSuccess { 
//				.generatorAdvertisement(3,3,3)
//				async.complete()
//			})
//	}


}