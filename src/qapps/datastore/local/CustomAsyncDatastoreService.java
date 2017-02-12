package qapps.datastore.local;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.DatastoreAttributes;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Index;
import com.google.appengine.api.datastore.Index.IndexState;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;

public class CustomAsyncDatastoreService implements AsyncDatastoreService {

	private DatastoreService sync;
	private static ExecutorService executor = Executors.newCachedThreadPool();

	/**
	 * @param sync
	 */
	public CustomAsyncDatastoreService(DatastoreService service) {
		sync = service;
	}

	@Override
	public Collection<Transaction> getActiveTransactions() {
		return sync.getActiveTransactions();
	}

	@Override
	public Transaction getCurrentTransaction() {
		return sync.getCurrentTransaction();
	}

	@Override
	public Transaction getCurrentTransaction(Transaction returnedIfNoTxn) {
		return sync.getCurrentTransaction(returnedIfNoTxn);
	}

	@Override
	public PreparedQuery prepare(Query query) {
		return sync.prepare(query);
	}

	@Override
	public PreparedQuery prepare(Transaction txn, Query query) {
		return sync.prepare(txn, query);
	}

	@Override
	public Future<KeyRange> allocateIds(final String kind, final long num) {
		return executor.submit(new Callable<KeyRange>() {

			@Override
			public KeyRange call() throws Exception {
				return sync.allocateIds(kind, num);
			}

		});
	}

	@Override
	public Future<KeyRange> allocateIds(final Key parent, final String kind,
			final long num) {
		return executor.submit(new Callable<KeyRange>() {

			@Override
			public KeyRange call() throws Exception {
				return sync.allocateIds(parent, kind, num);
			}

		});
	}

	@Override
	public Future<Transaction> beginTransaction() {
		return executor.submit(new Callable<Transaction>() {

			@Override
			public Transaction call() throws Exception {
				return sync.beginTransaction();
			}

		});
	}

	@Override
	public Future<Transaction> beginTransaction(final TransactionOptions options) {
		return executor.submit(new Callable<Transaction>() {

			@Override
			public Transaction call() throws Exception {
				return sync.beginTransaction(options);
			}

		});
	}

	@Override
	public Future<Void> delete(final Key... keys) {
		return executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				sync.delete(keys);
				return null;
			}

		});
	}

	@Override
	public Future<Void> delete(final Iterable<Key> keys) {
		return executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				sync.delete(keys);
				return null;
			}

		});
	}

	@Override
	public Future<Void> delete(final Transaction txn, final Key... keys) {
		return executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				sync.delete(txn, keys);
				return null;
			}

		});
	}

	@Override
	public Future<Void> delete(final Transaction txn, final Iterable<Key> keys) {
		return executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				sync.delete(txn, keys);
				return null;
			}

		});
	}

	@Override
	public Future<Entity> get(final Key key) {
		return executor.submit(new Callable<Entity>() {

			@Override
			public Entity call() throws Exception {
				return sync.get(key);
			}

		});
	}

	@Override
	public Future<Map<Key, Entity>> get(final Iterable<Key> keys) {
		return executor.submit(new Callable<Map<Key, Entity>>() {

			@Override
			public Map<Key, Entity> call() throws Exception {
				return sync.get(keys);
			}

		});
	}

	@Override
	public Future<Entity> get(final Transaction txn, final Key key) {
		return executor.submit(new Callable<Entity>() {

			@Override
			public Entity call() throws Exception {
				return sync.get(txn, key);
			}

		});
	}

	@Override
	public Future<Map<Key, Entity>> get(final Transaction txn,
			final Iterable<Key> keys) {
		return executor.submit(new Callable<Map<Key, Entity>>() {

			@Override
			public Map<Key, Entity> call() throws Exception {
				return sync.get(txn, keys);
			}

		});
	}

	@Override
	public Future<DatastoreAttributes> getDatastoreAttributes() {
		return executor.submit(new Callable<DatastoreAttributes>() {

			@Override
			public DatastoreAttributes call() throws Exception {
				return sync.getDatastoreAttributes();
			}

		});
	}

	@Override
	public Future<Map<Index, IndexState>> getIndexes() {
		return executor.submit(new Callable<Map<Index, IndexState>>() {

			@Override
			public Map<Index, IndexState> call() throws Exception {
				return sync.getIndexes();
			}

		});
	}

	@Override
	public Future<Key> put(final Entity entity) {
		return executor.submit(new Callable<Key>() {

			@Override
			public Key call() throws Exception {
				return sync.put(entity);
			}

		});
	}

	@Override
	public Future<List<Key>> put(final Iterable<Entity> entities) {
		return executor.submit(new Callable<List<Key>>() {

			@Override
			public List<Key> call() throws Exception {
				return sync.put(entities);
			}

		});
	}

	@Override
	public Future<Key> put(final Transaction txn, final Entity entity) {
		return executor.submit(new Callable<Key>() {

			@Override
			public Key call() throws Exception {
				return sync.put(txn, entity);
			}

		});
	}

	@Override
	public Future<List<Key>> put(final Transaction txn,
			final Iterable<Entity> entities) {
		return executor.submit(new Callable<List<Key>>() {

			@Override
			public List<Key> call() throws Exception {
				return sync.put(txn, entities);
			}

		});
	}
}
