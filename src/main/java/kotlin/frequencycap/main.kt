package frequencycap


import io.vertx.core.Vertx
import io.vertx.kotlin.core.deployVerticleAwait

suspend fun main() {
	val vertx = Vertx.vertx()
	try {
		vertx.deployVerticleAwait("frequencycap.App")
		vertx.deployVerticleAwait("frequencycap.AppInMem")
		vertx.deployVerticleAwait("frequencycap.AppRedis")
		println("Application started")
	} catch (exception: Throwable) {
		println("Could not start application")
		exception.printStackTrace()
	}
}

