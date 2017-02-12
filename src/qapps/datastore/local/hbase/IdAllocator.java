package qapps.datastore.local.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.appengine.api.datastore.Key;

public class IdAllocator {

	private static final byte[] EMPTY = new byte[] {};
	private static final byte[] FAMILY = Bytes.toBytes("n");
	public static final String[] COLUMNS = { Bytes.toString(FAMILY) };

	private final HBaseDatastore schema;

	IdAllocator(HBaseDatastore schema) throws IOException {

		this.schema = schema;

		final Put put = new Put(EMPTY).add(FAMILY, EMPTY, Bytes.toBytes(1L));

		// TODO consider synchronizing the access
		new HBaseAccess(schema, Table.ID_SEQUENCES) {

			@Override
			protected Object doWork(HTableInterface table) throws IOException {

				// check for non-existence root id sequence
				table.checkAndPut(EMPTY, FAMILY, EMPTY, null, put);
				return null;
			}
		}.access();
	}

	long generateRoot() throws IOException {
		return generateRoot(1L);
	}

	/**
	 * 
	 * @param key
	 *            without id.
	 * @return
	 */
	long generateRoot(final long amount) throws IOException {

		System.out.print("generating new id for root entity: ");

		return (Long) new HBaseAccess(schema, Table.ID_SEQUENCES) {

			@Override
			protected Object doWork(HTableInterface table) throws IOException {

				long result = table.incrementColumnValue(EMPTY, FAMILY, EMPTY,
						amount);
				System.out.println(result);
				return result;
			}

		}.access();
	}

	long generateGroup(Key parent) throws IOException {
		return generateGroup(parent, 1L);
	}

	long generateGroup(final Key parent, final long amount) throws IOException {

		// TODO ensure that root is not necessary
		System.out.println("generating new id for entity group: " + parent);

		final byte[] row = BytesHelper.serialize(parent);

		return (Long) new HBaseAccess(schema, Table.ID_SEQUENCES) {

			@Override
			protected Object doWork(HTableInterface table) throws IOException {

				// check if group has been initialized
				if (!table.get(new Get(row)).isEmpty()) {

					long l = table.incrementColumnValue(
							BytesHelper.serialize(parent), FAMILY, EMPTY,
							amount);
					System.out.println("generated: " + l);
					return l;
				}

				// lock, check again and initialize
				RowLock lock = table.lockRow(row);
				try {

					Result result = table.get(new Get(row));

					if (result.isEmpty()) {

						table.put(new Put(BytesHelper.serialize(parent)).add(
								FAMILY, EMPTY, Bytes.toBytes(1L)));
						System.out.println("generated: " + 1L);
						return 1L;

					} else
						return table.incrementColumnValue(
								BytesHelper.serialize(parent), FAMILY, EMPTY,
								amount);

				} finally {
					if (lock != null)
						table.unlockRow(lock);
				}
			}

		}.access();

	}
}
