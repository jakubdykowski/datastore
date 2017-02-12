package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.berkeley.DataType;
import qapps.datastore.local.hbase.io.KindIndex;
import qapps.datastore.local.hbase.io.SinglePropertyIndex;

import com.google.appengine.api.datastore.Entity;

public class SimpleIndex {
	/**
	 * Try to store simple indices only once.
	 * 
	 * @param entities
	 * @throw IOException
	 */
	public static void index(HBaseDatastore ds, Iterable<Entity> entities)
			throws IOException {

		List<Put> byKind = new LinkedList<Put>();
		List<Put> byPropertyAsc = new LinkedList<Put>();
		List<Put> byPropertyDesc = new LinkedList<Put>();

		for (Entity entity : entities) {

			byte[] keyBytes = null;
			try {
				keyBytes = BytesHelper.serialize(entity.getKey());
			} catch (IOException e) {
				throw new IOException("Cannot serialize key '"
						+ entity.getKey() + "'");
			}
			byte[] kindBytes = Bytes.toBytes(entity.getKey().getKind());

			// prepare kind index
			byKind.add(KindIndex.prepareIndex(keyBytes, kindBytes));

			for (Entry<String, Object> entry : entity.getProperties()
					.entrySet()) {

				// check whether property should NOT be indexed
				if (entity.isUnindexedProperty(entry.getKey())) {
					continue;
				}

				DataType type = DataType.valueOf(entry.getValue().getClass()
						.getSimpleName());
				byte[] nameBytes = Bytes.toBytes(entry.getKey());
				byte[] valueBytes;
				try {
					valueBytes = type.serialize(entry.getValue());
				} catch (IOException e) {
					throw new IOException("Cannot serialize property '"
							+ entry.getKey() + "' of entity '"
							+ entity.getKey() + "'", e);
				}

				// prepare ascending single property index
				byPropertyAsc.add(SinglePropertyIndex.prepareIndex(keyBytes,
						kindBytes, nameBytes, valueBytes, false));

				// prepare descending single property index (simply reversed
				// value by the last boolean value == true)
				byPropertyDesc.add(SinglePropertyIndex.prepareIndex(keyBytes,
						kindBytes, nameBytes, valueBytes, true));
			}
		}

		// store indexes
		storeIndex(ds, Table.BY_KIND, byKind);
		storeIndex(ds, Table.BY_PROPERTY_ASC, byPropertyAsc);
		storeIndex(ds, Table.BY_PROPERTY_DESC, byPropertyDesc);
	}

	private static void storeIndex(HBaseDatastore ds, Table table, final List<Put> puts)
			throws IOException {
		new HBaseAccess<Void>(ds, table) {

			@Override
			protected Void doWork(HTableInterface table) throws IOException {
				table.put(puts);
				return null;
			}

		}.access();
	}
}
