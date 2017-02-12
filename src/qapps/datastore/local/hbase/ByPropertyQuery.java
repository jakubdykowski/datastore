package qapps.datastore.local.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.berkeley.DataType;
import qapps.datastore.local.hbase.io.KeyExtractor;
import qapps.datastore.local.hbase.io.SinglePropertyIndex;

import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterPredicate;

public class ByPropertyQuery implements HBasePreparedQuery {
	private final FilterPredicate filter;
	private final Query.SortDirection sort;
	private final byte[] kind;
	private final byte[] property;
	private final byte[] value;

	public ByPropertyQuery(Query query) throws IOException {

		this.filter = query.getFilterPredicates().get(0);
		this.kind = Bytes.toBytes(query.getKind());
		this.property = Bytes.toBytes(filter.getPropertyName());
		this.value = DataType.valueOf(
				filter.getValue().getClass().getSimpleName()).serialize(
				filter.getValue());

		// TODO ensure that this is correct
		if (query.getSortPredicates().size() == 1
				&& query.getSortPredicates().get(0).getPropertyName()
						.equals(filter.getPropertyName()))
			sort = query.getSortPredicates().get(0).getDirection();
		else
			sort = Query.SortDirection.ASCENDING;
	}

	@Override
	public Scan getScan() {

		if (sort == Query.SortDirection.DESCENDING) {

			// descending order
				return SinglePropertyIndex.prepareScan(kind, property, value,
						QueryBuilder.extract(filter.getOperator()), true);

		} else {

			// ascending order - defaults
			return SinglePropertyIndex.prepareScan(kind, property, value,
					QueryBuilder.extract(filter.getOperator()), false);
		}

	}

	@Override
	public KeyExtractor getKeyExtractor() {

		return SinglePropertyIndex.getKeyExtractor();

		// deprecated, property index asc and desc is now in SinglePropertyIndex
		// class, not any more divided into two separate classes
		// switch (sort) {
		// case ASCENDING:
		// return SinglePropertyIndex.keyExtractor();
		// case DESCENDING:
		// return ByPropertyDescIndex.keyExtractor();
		// default:
		// throw new UnsupportedOperationException("Unsupported sort type.");
		// }
	}

	@Override
	public Table getTable() {
		switch (sort) {
		case ASCENDING:
			return Table.BY_PROPERTY_ASC;
		case DESCENDING:
			return Table.BY_PROPERTY_DESC;
		default:
			throw new UnsupportedOperationException("Unsupported sort type.");
		}
	}
}
