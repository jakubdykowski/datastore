package qapps.datastore.local.hbase;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;

import qapps.datastore.local.hbase.io.KeyExtractor;

import com.google.appengine.api.datastore.Key;

public class KeyResultIterator implements Iterator<Key> {

	private Iterator<Result> rows;
	private KeyExtractor strategy;

	public KeyResultIterator(Iterator<Result> rows, KeyExtractor strategy) {
		this.rows = rows;
		this.strategy = strategy;
	}

	@Override
	public boolean hasNext() {
		return rows.hasNext();
	}

	@Override
	public Key next() {
		return strategy.extractKey(rows.next());
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported ever.");
	}

}
