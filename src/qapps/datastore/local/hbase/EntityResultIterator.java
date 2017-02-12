package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Result;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Entity;

public class EntityResultIterator implements Iterator<Entity> {

	private final Result[] entities;
	private int next;

	public EntityResultIterator(Result[] entities) {
		if (entities == null)
			throw new IllegalArgumentException("NULL argument.");
		this.entities = entities;
		next = 0;
	}

	@Override
	public boolean hasNext() {

		// tab hasn't got more elements
		if (next >= entities.length)
			return false;
		// next element is null
		if (entities[next] == null || entities[next].isEmpty()) {
			++next;
			// check next element
			return hasNext();
		}
		return true;
	}

	@Override
	public Entity next() {

		if (!hasNext())
			throw new NoSuchElementException();

		try {

			return Entities.extractEntity(entities[next++]);

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"Cannot iterate over entities because one of them is corrupted.");
		}
	}

	@Override
	public void remove() {
		if (next == 0)
			throw new IllegalStateException(
					"Iteration does not has started yet.");
		if (entities[next - 1] == null)
			throw new IllegalStateException(
					"This methos has been called already, call next() first.");
		entities[next - 1] = null;
	}

}
