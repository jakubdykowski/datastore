package qapps.datastore.local.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;


// TODO ensure using generics
public abstract class HBaseAccess<E> {

	private final Schema schema;
	private final Table table;

	protected HBaseAccess(Schema schema, Table table) {
		this.schema = schema;
		this.table = table;
	}

	/**
	 * Should return as quickly as possible.
	 * 
	 * @param table
	 * @return
	 * @throws IOException
	 */
	protected abstract E doWork(HTableInterface table) throws IOException;

	public final E access() throws IOException {

		HTableInterface access = schema.connect(table);
		try {

			return doWork(access);

		} finally {
			if (access != null)
				access.close();
		}
	}

	@Deprecated
	public static void close(HTableInterface table) {
		close(table, null);
	}

	@Deprecated
	public static void close(HTableInterface table, String warning) {

		try {
			if (table != null)
				table.close();

		} catch (IOException e) {
			if (warning != null)
				System.err.println("WARNING: " + warning + " cause: "
						+ e.getLocalizedMessage());
		}
	}
}
