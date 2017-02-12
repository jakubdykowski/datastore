package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;

import qapps.datastore.local.hbase.io.KeyExtractor;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.repackaged.com.google.common.collect.Lists;

public class HBaseQuery implements PreparedQuery {

	private final Getter getter;
	private final HBaseDatastore datastore;
	private final KeyQuery query;

	public HBaseQuery(KeyQuery query, HBaseDatastore schema, Getter getter) {
		this.query = query;
		this.getter = getter;
		this.datastore = schema;
	}

	@Override
	public Iterable<Entity> asIterable() {
		return asIterable(FetchOptions.Builder.withDefaults());
	}

	@Override
	public Iterable<Entity> asIterable(FetchOptions fetchOptions) {
		return new Iterable<Entity>() {
			@Override
			public Iterator<Entity> iterator() {
				return asIterator();
			}
		};
	}

	@Override
	public Iterator<Entity> asIterator() {
		return asIterator(FetchOptions.Builder.withDefaults());
	}

	@Override
	public Iterator<Entity> asIterator(FetchOptions fetchOpts) {

		System.out.println("PreparedQuery: fetchOpts: " + fetchOpts);
		System.err.println("calling table.getScanner()");

		try {
			// causes a bit more work deserailizing and creating independent
			// iterable collections
			// return
			// getter.get(query.fetchKeys(datastore)).values().iterator();
			return getter.getAsIterator(query.fetchKeys(datastore));

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"Error while performin actual query.");
		}
	}

	@Override
	public List<Entity> asList(FetchOptions fetchOptions) {
		return Lists.newLinkedList(asIterator());
	}

	@Override
	public QueryResultIterable<Entity> asQueryResultIterable() {
		return asQueryResultIterable(FetchOptions.Builder.withDefaults());
	}

	@Override
	public QueryResultIterable<Entity> asQueryResultIterable(
			final FetchOptions fetchOptions) {
		return new QueryResultIterable<Entity>() {

			@Override
			public QueryResultIterator<Entity> iterator() {
				return asQueryResultIterator(fetchOptions);
			}

		};
	}

	@Override
	public QueryResultIterator<Entity> asQueryResultIterator() {
		return asQueryResultIterator(FetchOptions.Builder.withDefaults());
	}

	@Override
	public QueryResultIterator<Entity> asQueryResultIterator(
			FetchOptions fetchOptions) {
		return new QueryResultIterator<Entity>() {
			private Iterator<Entity> i;
			{
				i = asIterator();
			}

			@Override
			public boolean hasNext() {
				return i.hasNext();
			}

			@Override
			public Entity next() {
				return i.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Not supported ever.");
			}

			@Override
			public Cursor getCursor() {
				// TODO Auto-generated method stub
				throw new UnsupportedOperationException("Not supported yet.");
			}

		};
	}

	@Override
	public QueryResultList<Entity> asQueryResultList(FetchOptions fetchOptions) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.appengine.api.datastore.PreparedQuery#asSingleEntity()
	 */
	@Override
	public Entity asSingleEntity() throws TooManyResultsException {
		// TODO throw IllegalStateException if txn is not acitve
		Iterator<Entity> i = asIterator();
		if (i.hasNext()) {
			Entity result = i.next();
			if (i.hasNext()) {
				throw new TooManyResultsException();
			} else
				return result;
		} else
			return null;
	}

	@Override
	public int countEntities() {
		return countEntities(FetchOptions.Builder.withDefaults());
	}

	@SuppressWarnings("finally")
	@Override
	public int countEntities(FetchOptions fetchOptions) {
		Iterator<Entity> i = asIterator();
		int count = 0;
		try {
			i.next();
			count++;
		} catch (NoSuchElementException e) {
		} finally {
			return count;
		}
	}
}
