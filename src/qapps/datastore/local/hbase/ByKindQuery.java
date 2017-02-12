package qapps.datastore.local.hbase;

import org.apache.hadoop.hbase.client.Scan;

import com.google.appengine.api.datastore.Query;

import qapps.datastore.local.hbase.io.KeyExtractor;
import qapps.datastore.local.hbase.io.KindIndex;

public class ByKindQuery implements HBasePreparedQuery {

	private final String kind;

	public ByKindQuery(Query query) {
		if (query.getKind() == null)
			throw new IllegalArgumentException();
		this.kind = query.getKind();
	}

	@Override
	public Scan getScan() {
		return KindIndex.prepareScan(kind);
	}

	@Override
	public KeyExtractor getKeyExtractor() {
		return KindIndex.keyExtractor();
	}

	@Override
	public Table getTable() {
		return Table.BY_KIND;
	}
	

}
