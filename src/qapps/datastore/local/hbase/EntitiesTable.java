package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.berkeley.DataType;
import qapps.datastore.local.hbase.io.ByPropertyDescIndex;
import qapps.datastore.local.hbase.io.KindIndex;
import qapps.datastore.local.hbase.io.SinglePropertyIndex;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

class EntitiesTable {

	public final static String ENTITY = "e", COMMITED = "c";
	public final static byte[] ENTITY_B = Bytes.toBytes(ENTITY);
	public final static byte[] COMMITED_B = Bytes.toBytes(COMMITED);
	private final static byte[] SLASH = Bytes.toBytes("/");
	private static final byte[] EMPTY = new byte[] {};
	public final static String[] COLUMNS = { "c", "j" };
	private static final byte[] FAMILY = Bytes.toBytes(COLUMNS[0]);
	private static final byte[] JOURNAL = Bytes.toBytes(COLUMNS[1]);

	// public static Put indexByKind(Entity entity) throws IOException {
	// return new Put(BytesHelper.concat(
	// Bytes.toBytes(entity.getKind() + "/"),
	// BytesHelper.serialize(entity.getKey()))).add(
	// Bytes.toBytes("a"), null, BytesHelper.empty);
	// }

	public static Put prepareEntity(byte[] key, byte[] entity)
			throws IOException {

		Put put = new Put(key);
		put.add(FAMILY, ENTITY_B, entity);
		put.add(FAMILY, COMMITED_B, EMPTY);
		return put;
	}

	public static Entity extractEntity(Result result) throws IOException {

		Entity created = new Entity(BytesHelper.toKey(result.getRow()));
		Entity fetched = (Entity) BytesHelper.toEntity(result.getValue(FAMILY,
				ENTITY_B));
		created.setPropertiesFrom(fetched);
		return created;
	}

	/**
	 * 
	 * @param lastCommited
	 * @param put
	 *            prepared by this.prepareEntity(byte[],Entity).
	 * @param entityGroup
	 * @param table
	 * @return
	 * @throws IOException
	 */
	public static boolean transactionPut(HBaseTransactionOld txn, Put put,
			HTableInterface table) throws IOException, IllegalStateException {

		return transactionPut(txn, Collections.singletonList(put), table);
	}

	public static boolean transactionPut(HBaseTransactionOld txn, List<Put> puts,
			HTableInterface table) throws IOException, IllegalStateException {

		byte[] entityGroup = BytesHelper.serialize(txn.getEntityGroup());
		byte[] lastCommited = Bytes.toBytes(txn.getStartTime());
		byte[] id = Bytes.toBytes(txn.getId());

		for (Put put : puts) {

			if (!transactionPut(entityGroup, lastCommited, id, put, table, txn)) {
				return false;
			}
		}
		return true;
	}

	private static boolean transactionPut(byte[] entityGroup,
			byte[] lastCommited, byte[] id, Put put, HTableInterface table,
			HBaseTransactionOld txn) throws IOException, IllegalStateException {

		Put transactional = new Put(entityGroup);
		transactional.add(JOURNAL, BytesHelper.concat(id, SLASH, put.getRow()),
				put.get(FAMILY, ENTITY_B).get(0).getValue());
		txn.verify();
		return table.checkAndPut(entityGroup, FAMILY, COMMITED_B, lastCommited,
				transactional);
	}

	public static long getLastCommited(Key entityGroup, HTableInterface table)
			throws IOException {

		if(entityGroup.getParent() != null) throw new IllegalArgumentException("Argument can be only root entity");
		final Get get = new Get(BytesHelper.serialize(entityGroup));
		get.addColumn(FAMILY, COMMITED_B);

		return table.get(get).getColumnLatest(FAMILY, COMMITED_B)
				.getTimestamp();
	}
	
	public static long getLastCommited(byte[] groupKey,HBaseDatastore ds) throws IOException {
		final Get get = new Get(groupKey);
		get.addColumn(FAMILY, COMMITED_B);
		return new HBaseAccess<Long>(ds, Table.ENTITIES) {

			@Override
			protected Long doWork(HTableInterface table) throws IOException {
				
				return table.get(get).getColumnLatest(FAMILY, COMMITED_B).getTimestamp();
			}
			
		}.access();
	}

	public static boolean commit(HBaseTransactionOld txn)
			throws IOException {

		byte[] row = BytesHelper.serialize(txn.getEntityGroup());
		Put commit = new Put(row);
		commit.add(FAMILY, COMMITED_B, EMPTY);
		final Get get = new Get(row);
		RowLock lock = null;
		HTableInterface table = null;
		try {

			table = schema.connect(Table.ENTITIES);
			try {
				table.lockRow(row);

				get.addColumn(FAMILY, COMMITED_B);

				long freshTimestamp = table.get(get)
						.getColumnLatest(FAMILY, COMMITED_B).getTimestamp();

				if (freshTimestamp <= txn.getStartTime()) {

					table.put(commit);
					return true;

				} else
					return false;
			} finally {
				try {
					if (lock != null)
						table.unlockRow(lock);
				} catch (IOException e) {
					System.err
							.println("WARNING: Error while unlocking row not neccessarily after transaction: "
									+ e.getLocalizedMessage());

				}
			}
		} finally {
			// TODO something, stop propagating error i think.
			HBaseAccess
					.close(table,
							"WARNING: Error while closing connection to hbase when performing txn commit.");
		}
	}

	@Deprecated
	public static Put prepareCommitChanges(HBaseTransactionOld txn)
			throws IOException {
		Put put = new Put(BytesHelper.serialize(txn.getEntityGroup()));
		put.add(FAMILY, COMMITED_B, EMPTY);
		return put;
	}

	@Deprecated
	public static Put storeEntity(Entity entity, List<Put> byKind,
			List<Put> byPropertyAsc, List<Put> byPropertyDesc)
			throws IOException {

		byte[] keyBytes = BytesHelper.serialize(entity.getKey());
		Put put = new Put(keyBytes);

		System.out.println("putting: " + entity.getKey() + " => "
				+ entity.getProperties().keySet());

		for (Entry<String, Object> entry : entity.getProperties().entrySet()) {

			String family = entry.getValue().getClass().getSimpleName();
			DataType type = DataType.valueOf(family);
			if (entry.getValue() != null) {
				try {
					byte[] familyBytes = Bytes.toBytes(family);
					byte[] nameBytes = Bytes.toBytes(entry.getKey());
					byte[] valueBytes = type.serialize(entry.getValue());
					byte[] kindBytes = Bytes.toBytes(entity.getKind());

					// prepare properties
					put.add(familyBytes, nameBytes, valueBytes);

					// prepare index byKind
					byKind.add(KindIndex.prepareIndex(kindBytes, keyBytes));

					// prepare byProperty indexes
					if (!entity.isUnindexedProperty(entry.getKey())) {
						byPropertyAsc.add(SinglePropertyIndex.prepareIndex(
								kindBytes, keyBytes, nameBytes, valueBytes,
								false));
						byPropertyDesc.add(ByPropertyDescIndex.prepareIndex(
								kindBytes, keyBytes, nameBytes, valueBytes));
					}

				} catch (IOException e) {
					throw new IOException("Error while serializing property '"
							+ entry.getKey(), e);
				}
			}

			System.out.println("	putting property "
					+ entry.getValue().getClass().getSimpleName()
					+ ": \'"
					+ entry.getKey()
					+ "\' class="
					+ (entry.getValue() == null ? null : entry.getValue()
							.getClass().getName()) + " => " + entry.getValue());

		}
		return put;
	}

	@Deprecated
	public static Entity c(Result result) throws IOException {
		Entity fetched = new Entity((Key) BytesHelper.toKey(result.getRow()));
		// for each data type
		for (DataType type : DataType.values()) {
			NavigableMap<byte[], byte[]> props = result.getFamilyMap(Bytes
					.toBytes(type.name()));
			// for each property of current type
			for (Entry<byte[], byte[]> entry : props.entrySet()) {
				Object value = type.deserialize(entry.getValue());
				fetched.setProperty(Bytes.toString(entry.getKey()), value);
			}
		}
		return fetched;
	}

	
	public static Get prepareGet(byte[] key) {
		return new Get(key);
	}

	@Deprecated
	public static Get prepareGet(HBaseTransactionOld txn, byte[] key) {
		if (txn.getStartTime() == 0)
			throw new RuntimeException(
					"Implementation error, txn has not had timestamp set yet.");
		return new Get(key).setTimeStamp(txn.getStartTime());
	}
}
