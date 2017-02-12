package qapps.datastore.local.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

import qapps.datastore.local.hbase.io.KeyExtractor;

class ByKeyQuery implements HBasePreparedQuery {

	private static final KeyExtractor extractor = new KeyExtractor() {

		@Override
		public Key extractKey(Result row) {
			return BytesHelper.toKey(row.getRow());
		}

	};

	private final Query query;

	public ByKeyQuery(Query query) {
		this.query = query;
	}

	@Override
	public Scan getScan() {

		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public KeyExtractor getKeyExtractor() {
		return extractor;
	}

	@Override
	public Table getTable() {
		return Table.ENTITIES;
	}

}
