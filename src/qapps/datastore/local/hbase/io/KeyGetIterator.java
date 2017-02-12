package qapps.datastore.local.hbase.io;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Get;

import qapps.datastore.local.hbase.BytesHelper;
import qapps.datastore.local.hbase.EntitiesTableUtils;

import com.google.appengine.api.datastore.Key;

public class KeyGetIterator implements Iterator<Get> {

	private final Iterator<Key> keys;
	public KeyGetIterator(Iterator<Key> keys) {
		this.keys = keys;
	}
	
	@Override
	public boolean hasNext() {
		return keys.hasNext();
	}
	@Override
	public Get next() {
		return Entities.;
	}
	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
}
