package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

final class Schema {

	private final HTablePool pool;
	private final Map<Table, String> customNames;

	public Schema(Configuration conf, int maxSize) throws IOException {

		pool = new HTablePool(conf, maxSize);

		// TODO support custom table names
		customNames = null;

		HBaseAdmin admin = null;

		try {

			admin = new HBaseAdmin(conf);

			// initialize all necessary hbase tables
			for (Table table : Table.values()) {

				String custom = customNames != null ? customNames.get(table)
						: null;
				table.setUp(admin, custom != null ? custom : table.getName());
			}

		} finally {
			if (admin != null)
				admin.close();
		}
	}

	String getTableName(Table table) {

		if (customNames == null)
			return table.getName();
		else {
			String custom = customNames.get(table);
			return custom != null ? custom : table.getName();
		}
	}

	public HTableInterface connect(Table table) throws IOException {

		// try {

		return pool.getTable(getTableName(table));

		// } catch (RuntimeException ex) {
		// throw new IOException(
		// "Error while initializing hbase table connection.");
		// }
	}

	interface Operation<T> {
		T doWork(HTableInterface conn) throws IOException;
	}

	abstract class Access<T> implements Operation<T> {

		private final Table table;

		public Access(Table table) {
			this.table = table;
		}

		public abstract T doWork(HTableInterface conn) throws IOException;

		public final T access() throws IOException {

			HTableInterface access = connect(table);
			try {

				return doWork(access);

			} finally {
				if (access != null)
					access.close();
			}
		}
//		public void 
	}
	
	class CustomAccess<T> extends Access<T> {

		private Operation<T> operation;

		public CustomAccess(Operation<T> operation, Table table) {
			super(table);
			this.operation = operation;
		}

		@Override
		public T doWork(HTableInterface conn) throws IOException {
			return this.operation.doWork(conn);
		}
	}

}
