package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

public class Saver {

	private final ExecutorService executor = Executors.newCachedThreadPool();
	private final HBaseDatastore ds;

	Saver(HBaseDatastore schema) {
		if (schema == null)
			throw new IllegalArgumentException();
		this.ds = schema;
	}

	Key save(Entity entity) throws IOException {
		List<Key> fetched = put(Collections.singleton(entity));
		if (fetched.size() < 1)
			return null;
		else
			return fetched.get(0);
	}

	List<Key> put(Iterable<Entity> entities) throws IOException {

		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * Does NOT check arugments.
	 * 
	 * @param txn
	 * @param entities
	 * @return
	 * @throws IOException
	 */
	List<Key> put(final HBaseTransactionOld txn, final Iterable<Entity> entities)
			throws IOException {

		// store entities
		final List<Key> keys = EntitiesTableUtils.store(ds, txn, entities);

		// asynchronously populate necessary indexes
		executor.execute(new Runnable() {

			@Override
			public void run() {
				while (true) {
					long timeout = 100;
					try {
						SimpleIndex.index(ds, entities);
						break;
					} catch (IOException e) {
						try {
							TimeUnit.MILLISECONDS.wait(timeout);
							timeout *= 2;
						} catch (InterruptedException e1) {
						}
					}
				}
			}

		});

		// TODO store composite indexes

		return keys;
	}

}
