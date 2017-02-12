package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

public class EntitiesTableUtils {

	/**
	 * Try to store transactionally entities only once. Does NOT check
	 * arguments.
	 * 
	 * @param ds
	 * @param txn
	 * @param entities
	 * @throws IOException
	 *             if any IOException occurs
	 * @throws ConcurrentModificationException
	 *             if entity group was modified since txn started
	 */
	public static List<Key> store(HBaseDatastore ds,
			final HBaseTransactionOld txn, Iterable<Entity> entities)
			throws IOException {

		final List<Key> result = new LinkedList<>();
		final List<Put> puts = new LinkedList<>();

		for (Entity entity : entities) {

			byte[] key = BytesHelper.serialize(entity.getKey());
			byte[] data = BytesHelper.serialize(entity);

			puts.add(EntitiesTable.prepareEntity(key, data));
			result.add(entity.getKey());
		}

		if (!new HBaseAccess<Boolean>(ds, Table.ENTITIES) {

			@Override
			protected Boolean doWork(HTableInterface table) throws IOException {
				return EntitiesTable.transactionPut(txn, puts, table);
			}

		}.access()) {
			throw new ConcurrentModificationException(
					"Entity group was midified since txn started.");
		}

		return result;
	}

	public static void store(HBaseDatastore ds, Iterable<EntitiesTableUtils> entities) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public static Map<Key, Entity> readTransactional(HBaseDatastore ds, HBaseTransactionOld txn,
			Iterable<Key> keys) throws IOException {

		

		Map<Key, Entity> entities = new HashMap<>();
		final List<Get> gets = new LinkedList<>();

		for (Key key : keys) {

			byte[] keyBytes;
			try {

				keyBytes = BytesHelper.serialize(key);

			} catch (IOException e) {
				throw new IOException("Cannot serialize key '" + key + "'.", e);
			}

			gets.add(EntitiesTable.prepareGet(txn, keyBytes));
		}

		Result[] results = new HBaseAccess<Result[]>(ds, Table.ENTITIES) {

			@Override
			protected Result[] doWork(HTableInterface table) throws IOException {
				return table.get(gets);
			}

		}.access();

		for (Result result : results) {

			if (!result.isEmpty()) {

				Entity entity = EntitiesTable.extractEntity(result);
				entities.put(entity.getKey(), entity);
			}
		}
		return entities;
	}
}
