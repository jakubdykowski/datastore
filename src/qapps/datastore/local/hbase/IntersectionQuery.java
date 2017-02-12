package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.collections.CollectionUtils;

import com.google.appengine.api.datastore.Key;

public class IntersectionQuery implements KeyQuery {

	private final Collection<HBasePreparedQuery> queries;

	public IntersectionQuery(Collection<HBasePreparedQuery> queries) {
		if (queries == null || queries.size() < 1)
			throw new IllegalArgumentException(
					"Cannot be null oo shorter than one element.");
		this.queries = queries;
	}

	// since we give only keys there is nothing to worry about
	@SuppressWarnings("unchecked")
	@Override
	public Iterable<Key> fetchKeys(HBaseDatastore ds) throws IOException {

		Iterator<HBasePreparedQuery> iter = queries.iterator();
		Collection<Key> intersection = new LinkedList<>(fetch(iter.next(), ds));

		while (iter.hasNext()) {
			intersection = CollectionUtils.intersection(intersection,
					fetch(iter.next(), ds));
		}

		return intersection;
	}

	private Collection<Key> fetch(HBasePreparedQuery query, HBaseDatastore ds)
			throws IOException {

		Collection<Key> keys = new LinkedList<>();
		CollectionUtils.addAll(keys, new SingleQuery(query).fetchKeys(ds)
				.iterator());
		return keys;
	}

}
