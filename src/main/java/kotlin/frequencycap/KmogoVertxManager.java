package kotlin.frequencycap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.litote.kmongo.coroutine.CoroutineClient;
import org.litote.kmongo.reactivestreams.KMongo;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;

public class KmogoVertxManager implements Closeable {

	private static final String DS_LOCAL_MAP_NAME = "__vertx.MongoClient.datasources";

	private final Vertx vertx;
	protected final MongoHolder holder;
	public CoroutineClient mongo;

	public CoroutineClient createShared() {
		return mongo;
	}

	public KmogoVertxManager(Vertx vertx, String dataSourceName) {
		Objects.requireNonNull(vertx);
		Objects.requireNonNull(dataSourceName);
		this.vertx = vertx;
		this.holder = lookupHolder(dataSourceName);
		this.mongo = holder.mongo();
	}

	private MongoHolder lookupHolder(String datasourceName) {
		synchronized (vertx) {
			LocalMap<String, MongoHolder> map = vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
			MongoHolder theHolder = map.get(datasourceName);
			if (theHolder == null) {
				theHolder = new MongoHolder(() -> removeFromMap(map, datasourceName));
				map.put(datasourceName, theHolder);
			} else {
				theHolder.incRefCount();
			}
			return theHolder;
		}
	}

	private void removeFromMap(LocalMap<String, MongoHolder> map, String dataSourceName) {
		synchronized (vertx) {
			map.remove(dataSourceName);
			if (map.isEmpty()) {
				map.close();
			}
		}
	}

	private static class MongoHolder implements Shareable {
		CoroutineClient mongo;
		Runnable closeRunner;
		int refCount = 1;

		public MongoHolder(Runnable closeRunner) {
			this.closeRunner = closeRunner;
		}

		synchronized CoroutineClient mongo() {
			if (mongo == null) {
				mongo = new CoroutineClient(KMongo.INSTANCE.createClient());
			}
			return mongo;
		}

		synchronized void incRefCount() {
			refCount++;
		}

		synchronized void close() {
			if (--refCount == 0) {
				if (mongo != null) {
					mongo.close();
				}
				if (closeRunner != null) {
					closeRunner.run();
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		holder.close();
	}
}
