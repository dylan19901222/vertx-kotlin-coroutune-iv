package frequencycap


import io.vertx.core.Vertx
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.core.VertxOptions

suspend fun main() {
	val mgr: ClusterManager = HazelcastClusterManager()
	val options: VertxOptions = VertxOptions().setClusterManager(mgr)
	Vertx.clusteredVertx(options, { cluster ->
		if (cluster.succeeded()) {
			cluster.result().deployVerticle("frequencycap.App", { res ->
				if (res.succeeded()) {
					println(res.result())
				} else {
					println("App Deployment failed!")
				}
			});
			cluster.result().deployVerticle("frequencycap.App2", { res ->
				if (res.succeeded()) {
					println(res.result())
				} else {
					println("App2 Deployment failed!")
				}
			});
		} else {
			println("Cluster up failed: " + cluster.cause())
		}
	});
//	val vertx = Vertx.vertx()
//	try {
//		vertx.deployVerticleAwait("frequencycap.App")
//		vertx.deployVerticleAwait("frequencycap.App2")
//		println("Application started")
//	} catch (exception: Throwable) {
//		println("Could not start application")
//		exception.printStackTrace()
//	}
}

