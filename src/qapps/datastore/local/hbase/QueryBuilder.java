package qapps.datastore.local.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.CompareFilter;

import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortPredicate;

public class QueryBuilder {

	private final HBaseDatastore schema;
	private final Getter getter;

	public QueryBuilder(HBaseDatastore schema, Getter getter) {
		if (schema == null || getter == null)
			throw new IllegalArgumentException();
		this.schema = schema;
		this.getter = getter;
	}

	// public PreparedQuery buildByKind(String kind) throws IOException {
	//
	// Scan scan = ByKindIndex.prepareScan(new Scan(), kind);
	//
	// System.out.println("building query for type: " + kind + " "
	// + Bytes.toString(scan.getStartRow()) + " => "
	// + Bytes.toString(scan.getStopRow()));
	//
	// return build(scan, schema.byKind(), ByKindIndex.keyExtractor());
	// }
	//
	// public PreparedQuery buildByPropertyAsc(String kind, String property,
	// Object value, FilterOperator operator) throws IOException {
	//
	// Scan scan = ByPropertyAscIndex.prepareScan(new Scan(), kind, property,
	// value, extract(operator));
	//
	// return build(scan, schema.byPropertyAsc(),
	// ByPropertyAscIndex.keyExtractor());
	// }
	//
	// public PreparedQuery buildByPropertyDesc(String kind, String property,
	// Object value, FilterOperator operator) throws IOException {
	//
	// Scan scan = ByPropertyDescIndex.prepareScan(new Scan(), kind, property,
	// value, extract(operator));
	//
	// return build(scan, schema.byPropertyAsc(),
	// ByPropertyAscIndex.keyExtractor());
	// }

	public PreparedQuery build(Query query) throws IOException {
		System.out.println("  FilterPredicates:");
		for (FilterPredicate filter : query.getFilterPredicates()) {
			System.out.println("    " + filter.getPropertyName() + " "
					+ filter.getOperator() + " " + filter.getValue() + " => "
					+ filter.getValue().getClass());
		}
		System.out.println("  SortPredicates:");
		for (SortPredicate sort : query.getSortPredicates()) {
			System.out.println("    " + sort.getPropertyName() + "="
					+ sort.getDirection());
		}

		// new way for extracting the right type of query

		// FilterPredicate primaryFilter = null;
		// for (FilterPredicate filter : query.getFilterPredicates()) {
		// if (filter.getOperator() != Query.FilterOperator.EQUAL) {
		// if (primaryFilter == null) {
		// inequalityRestricted = true;
		// primaryFilter = filter;
		// } else {
		// // composite index query
		// throw new UnsupportedOperationException(
		// "Composite queries not yet supported.");
		// }
		// }
		// }
		if (query.getFilterPredicates().size() < 1) {
			// simple by kind query
			return build(new ByKindQuery(query));
		} else if (query.getFilterPredicates().size() > 1) {
			// complex by multiple propery query
			throw new UnsupportedOperationException("Not supported yet.");
		} else {
			// by single property query
			return build(new ByPropertyQuery(query));
		}
		// throw new UnsupportedOperationException(
		// "Implementation is not yet finished");
	}

	private PreparedQuery build(KeyQuery query) throws IOException {
		return new HBaseQuery(query, schema, getter);
	}

	public static CompareFilter.CompareOp extract(FilterOperator operator) {

		switch (operator) {
		case EQUAL:
			return CompareFilter.CompareOp.EQUAL;
		case GREATER_THAN:
			return CompareFilter.CompareOp.GREATER;
		case GREATER_THAN_OR_EQUAL:
			return CompareFilter.CompareOp.GREATER_OR_EQUAL;
		case LESS_THAN:
			return CompareFilter.CompareOp.LESS;
		case LESS_THAN_OR_EQUAL:
			return CompareFilter.CompareOp.LESS_OR_EQUAL;
		case NOT_EQUAL:
			return CompareFilter.CompareOp.NOT_EQUAL;
		case IN:
			throw new UnsupportedOperationException(
					"Query for existence in a collection is not supported yet.");
		default:
			throw new UnsupportedOperationException(
					"Unknown query filter operator.");
		}
	}
}
