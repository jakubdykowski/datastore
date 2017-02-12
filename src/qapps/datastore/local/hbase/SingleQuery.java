/**
 * 
 */
package qapps.datastore.local.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;

import com.google.appengine.api.datastore.Key;

/**
 * @author qba
 * 
 */
public class SingleQuery implements KeyQuery {

	private HBasePreparedQuery query;

	public SingleQuery(HBasePreparedQuery prepared) {
		this.query = prepared;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * qapps.datastore.local.hbase.KeyQuery#fetchKeys(qapps.datastore.local.
	 * hbase.HBaseDatastore)
	 */
	@Override
	public Iterable<Key> fetchKeys(HBaseDatastore datastore) throws IOException {

		return new HBaseAccess<Iterable<Key>>(datastore, query.getTable()) {

			@Override
			protected Iterable<Key> doWork(HTableInterface table)
					throws IOException {
				return new KeyResultIterable(table.getScanner(query.getScan()),
						query.getKeyExtractor());
			}

		}.access();
	}

}
