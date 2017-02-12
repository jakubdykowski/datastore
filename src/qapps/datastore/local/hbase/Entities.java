package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.berkeley.Utils;
import qapps.datastore.local.hbase.Schema.Access;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

public final class Entities {

	final static String ENTITY = "e", COMMITED = "c";
	public final static byte[] ENTITY_B = Bytes.toBytes(ENTITY);
	public final static byte[] COMMITED_B = Bytes.toBytes(COMMITED);
	private final static byte[] SLASH = Bytes.toBytes("/");
	private static final byte[] EMPTY = new byte[] {};
	final static String[] COLUMNS = { "c", "j" };
	public static final byte[] FAMILY = Bytes.toBytes(COLUMNS[0]);
	static final byte[] JOURNAL = Bytes.toBytes(COLUMNS[1]);

	private final Schema schema;
	private final Journal journal;

	protected Entities(Schema schema) {
		this.schema = schema;
		this.journal = new Journal(schema);
	}

	Journal getJournal() {
		return this.journal;
	}

	void commit(HBaseTransactionOld txn) throws IOException {
		
//		schema.new Access<>
		// TODO implement
		throw new UnsupportedOperationException("Not supported yet.");
	}

	Access<Result[]> read(Iterable<Key> keys, final HBaseTransactionOld txn)
			throws IOException {

		if (txn == null)
			return read(keys);

		final List<Get> gets = new LinkedList<>();

		for (Key key : keys)
			gets.add(prepareReadEntity(key, txn.getStartTime()));

		return schema.new Access<Result[]>(Table.ENTITIES) {

			@Override
			public Result[] doWork(HTableInterface conn) throws IOException {
				return conn.get(gets);
			}

		};
	}

	Access<Result[]> read(Iterable<Key> keys) throws IOException {
		return read(createGroups(keys));
	}

	Access<Result[]> read(final Map<Key, ? extends Iterable<Key>> map)
			throws IOException {

		validateEntityGroups(map);

		return schema.new Access<Result[]>(Table.ENTITIES) {

			@Override
			public Result[] doWork(HTableInterface conn) throws IOException {

				long[] timestamps = readTimestamps(map.keySet(), conn);

				final List<Get> gets = new LinkedList<>();
				int i = 0;
				for (Iterable<Key> keys : map.values()) {
					for (Key key : keys) {
						gets.add(prepareReadEntity(key, timestamps[i]));
					}
					i++;
				}
				return conn.get(gets);
			}

		};
	}

	void writeLocking(final Map<Key, Iterable<Key>> groups) {

		// TODO ensure that validating is necessary
		validateEntityGroups(groups);

		// TODO implement
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * reads without of locking
	 * 
	 * @param roots
	 * @param table
	 * @return
	 * @throws IOException
	 */
	long[] readTimestamps(Iterable<Key> roots, HTableInterface table)
			throws IOException {

		validateTable(table);

		// TODO only while developing
		for (Key root : roots) {
			if (root.getParent() != null)
				throw new IllegalArgumentException(root
						+ " is not an entity root.");
		}

		final List<Get> gets = new LinkedList<>();

		for (Key root : roots) {
			gets.add(prepareReadTimestamp(root, null));
		}

		Result[] result = table.get(gets);

		long[] timestamps = new long[result.length];

		for (int i = 0; i < result.length; i++) {
			timestamps[i] = extractTimestamp(result[i]);
		}
		return timestamps;
	}

	/**
	 * reads on or without given lock
	 * 
	 * @param root
	 * @param table
	 * @param lock
	 *            can be null
	 * @return
	 */
	long readTimestamp(Key root, HTableInterface table, RowLock lock)
			throws IOException {

		return table.get(prepareReadTimestamp(root, lock))
				.getColumnLatest(FAMILY, COMMITED_B).getTimestamp();
	}

	private static Map<Key, ? extends Iterable<Key>> createGroups(
			Iterable<Key> keys) {

		Map<Key, List<Key>> groups = new HashMap<>();
		for (Key key : keys) {
			Key root = Utils.getEntityGroup(key);
			List<Key> group = groups.get(root);
			if (group == null) {
				group = new LinkedList<Key>();
				groups.put(root, group);
			}
			group.add(key);
		}
		return groups;
	}

	private static Get prepareReadTimestamp(Key root, RowLock lock)
			throws IOException {
		if (root.getParent() != null)
			throw new IllegalArgumentException("Argument is not root entity.");
		return new Get(BytesHelper.serialize(root), lock).addColumn(FAMILY,
				COMMITED_B);
	}

	private static long extractTimestamp(Result result) {
		return result.getColumnLatest(FAMILY, COMMITED_B).getTimestamp();
	}

	private static Get prepareReadEntity(Key key, long timestamp)
			throws IOException {
		return new Get(BytesHelper.serialize(key)).addColumn(FAMILY, ENTITY_B);
	}

	public static Entity extractEntity(Result result) throws IOException {

		// ensure that entity has proper key
		Entity created = new Entity(BytesHelper.toKey(result.getRow()));
		Entity fetched = BytesHelper
				.toEntity(result.getValue(FAMILY, ENTITY_B));
		created.setPropertiesFrom(fetched);
		if (!created.getKey().equals(fetched.getKey()))
			System.err
					.println("WARN: "
							+ "fetched entity has different key than the one it was fetched by. ["
							+ created.getKey() + "] != [" + fetched.getKey()
							+ "]");
		return created;
	}

	private void validateTable(HTableInterface table) {
		if (!table.getTableName().equals(
				Bytes.toBytes(schema.getTableName(Table.ENTITIES))))
			throw new IllegalArgumentException("Incorrect table.");
	}

	private static void validateEntityGroups(
			Map<Key, ? extends Iterable<Key>> map) {

		if (map == null || map.size() < 1)
			throw new IllegalArgumentException(
					"No groups given (NULL, or zero-size).");

		for (Entry<Key, ? extends Iterable<Key>> entry : map.entrySet()) {
			for (Key entity : entry.getValue()) {
				if (!entry.getKey().equals(Utils.getEntityGroup(entity))) {
					throw new IllegalArgumentException("'" + entity
							+ "' is not descandant of '" + entry.getKey() + "'");
				}
			}
		}
	}

	/**
	 * check whether all key belong to the same entity group.
	 * 
	 * @param keys
	 */
	private static void validateEntityGroups(Iterable<Key> keys) {

		Iterator<Key> i;
		if (keys == null || !(i = keys.iterator()).hasNext())
			throw new IllegalArgumentException("No elements given.");

		Key group = i.next();

		for (Key key : keys) {
			if (!group.equals(Utils.getEntityGroup(key)))
				System.err
						.println("WARN: "
								+ "fetching '"
								+ key
								+ "' within txn which entity group does NOT match the key ones. Result does NOT be consistient anymore.");
			// throw new IllegalArgumentException(
			// "Keys does not benlong to the same entity group.");
		}
	}

}
