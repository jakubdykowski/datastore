package qapps.datastore.local.hbase;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;

import com.google.appengine.api.datastore.Entity;

public class EntityResultIterable implements Iterable<Entity> {

	private final Result[] entities;

	public EntityResultIterable(Result[] entities) {
		this.entities = entities;
	}

	@Override
	public Iterator<Entity> iterator() {
		return new EntityResultIterator(entities);
	}

}
