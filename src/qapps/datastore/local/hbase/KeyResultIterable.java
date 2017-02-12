package qapps.datastore.local.hbase;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import qapps.datastore.local.hbase.io.KeyExtractor;

import com.google.appengine.api.datastore.Key;

public class KeyResultIterable implements Iterable<Key> {

	private Iterable<Result> rows;
	private KeyExtractor extractor;

	public KeyResultIterable(ResultScanner result, KeyExtractor strategy) {
		this.rows = result;
		this.extractor = strategy;
	}

	@Override
	public Iterator<Key> iterator() {
		return new KeyResultIterator(rows.iterator(), extractor);
	}
}
