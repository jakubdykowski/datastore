package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;

import qapps.datastore.local.hbase.io.Request;
import qapps.datastore.local.hbase.io.Response;

final class HBaseDatastore {

	private final Entities entities;
	private final Indexes indexes;
	private final Schema schema;
	private final ThreadStack stack;

	public HBaseDatastore(Configuration conf, int maxSize) throws IOException {

		this.schema = new Schema(conf, maxSize);
		this.stack = new ThreadStack();

		// setup functionality objects
		this.entities = new Entities(schema);
		this.indexes = new Indexes(schema);
	}

	public Entities getEntities() {
		return this.entities;
	}

	Schema getSchema() {
		return this.schema;
	}

	public Indexes getIndexes() {
		return this.indexes;
	}

	public <T> T call(Request request, Table table, Response<T> response)
			throws IOException {
		return FutureHelper.quietGet(callAsync(request, table, response));
	}

	public <T> Future<T> callAsync(final Request request, final Table table,
			final Response<T> response) throws IOException {
		return stack.call(new Callable<T>() {

			@Override
			public T call() throws Exception {
				HTableInterface conn = null;
				try {
					conn = schema.connect(table);
					return response.extract(request.request(conn));
				} finally {
					if (conn != null)
						conn.close();
				}
			}

		});
	}
}
