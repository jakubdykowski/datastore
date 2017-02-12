package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

public class Getter extends BaseModule {


	public Getter(HBaseDatastore ds) {
		super(ds);
	}

	/**
	 * Register the provided future with the provided txn so that we know to
	 * perform a {@link java.util.concurrent.Future#get()} before the txn is
	 * committed.
	 * 
	 * @param txn
	 *            The txn with which the future must be associated.
	 * @param future
	 *            The future to associate with the txn.
	 * @param <T>
	 *            The type of the Future
	 * @return The same future that was passed in, for caller convenience.
	 */
	private <T> Future<T> registerInTransaction(Transaction txn,
			Future<T> future) {
		if (txn != null) {
			defaultTxnProvider.addFuture(txn, future);
			return new FutureHelper.TxnAwareFuture<T>(future, txn,
					defaultTxnProvider);
		}
		return future;
	}

	public Map<Key, Entity> get(HBaseTransactionOld txn, Iterable<Key> keys)
			throws IOException {

		Map<Key, Entity> entities = new HashMap<>();
		Result[] results = null;

		if (txn != null) {
			// read commited
			results = ds.getEntities().read(keys).access();

		} else {
			// read transactional
			throw new UnsupportedOperationException("Not supported yet.");
		}

		return new EntityResultMap(results);

	}

	/**
	 * Fetches keys but does not deserialize entities, what is done while
	 * itarating trough the result.
	 * 
	 * @param keys
	 * @return not serializable iterator.
	 * @throws IOException
	 */
	public Iterator<Entity> getAsIterator(Iterable<Key> keys)
			throws IOException {

		final List<Get> gets = new LinkedList<>();

		for (Key key : keys) {

			try {

				gets.add(EntitiesTable.prepareGet(BytesHelper.serialize(key)));

			} catch (IOException | NullPointerException e) {
				throw new IllegalArgumentException(
						"Given key to obtain entities is corrupted or equals NULL. Cannot serailize it.");
			}
		}

		// access to hbase
		Result[] results = new HBaseAccess<Result[]>(ds, Table.ENTITIES) {

			@Override
			protected Result[] doWork(HTableInterface table) throws IOException {
				return table.get(gets);
			}

		}.access();

		return new EntityResult(results);

	}

	/*
	 * Deserializes entities 'on demand', mean while iterating.
	 */
	@Deprecated
	private static class EntityResult implements Iterator<Entity> {

		private Result[] rows;
		private int pos = 0;

		public EntityResult(Result[] result) {
			this.rows = result;
		}

		@Override
		public boolean hasNext() {
			return pos < rows.length;
		}

		@Override
		public Entity next() {

			if (pos >= rows.length) {
				throw new NoSuchElementException();
			}

			Result result = rows[pos++];

			if (result == null) {
				if (hasNext())
					return next();
				else
					throw new NoSuchElementException();
			}

			Entity next;
			try {

				next = EntitiesTable.extractEntity(result);

			} catch (IOException e) {
				throw new DatastoreFailureException(
						"Cannot deserialize entity from query result.", e);
			}
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supprted yet.");
		}

	}
}
