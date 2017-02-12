package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

/**
 * Computes extracting data from hbase Result each time whatever method is
 * called. Each call return completly new object but with the same properties.
 * Designed to iterate about one time.
 * 
 * @author qba
 * 
 */
public final class EntityResultMap implements Map<Key, Entity> {

	private final Result[] entities;
	private Iterable<Entity> iterable;

	public EntityResultMap(Result[] entities) {
		this.entities = entities;
	}

	@Override
	public void clear() {
		for (int i = 0; i < entities.length; i++) {
			entities[i] = null;
		}
	}

	@Override
	public boolean containsKey(Object key) {

		if (key == null || !(key instanceof Key))
			return false;

		byte[] keyBytes;
		try {
			keyBytes = BytesHelper.serialize((Key) key);
		} catch (IOException e) {
			return false;
		}

		for (int i = 0; i < entities.length; i++) {
			if (keyBytes.equals(entities[i].getRow())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {

		if (!(value instanceof Entity)) {
			return false;
		}

		for (Entity entity : new EntityResultIterable(entities)) {
			if (value.equals(entity)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Set<java.util.Map.Entry<Key, Entity>> entrySet() {
		return null;
	}

	@Override
	public Entity get(Object key) {

		if (key == null || !(key instanceof Key))
			return null;

		byte[] keyBytes;
		try {
			keyBytes = BytesHelper.serialize((Key) key);
		} catch (IOException e) {
			return null;
		}

		for (int i = 0; i < entities.length; i++) {
			if (keyBytes.equals(entities[i].getRow())) {
				try {
					return Entities.extractEntity(entities[i]);
				} catch (IOException e) {
					return null;
				}
			}
		}
		return null;
	}

	@Override
	public boolean isEmpty() {
		return entities.length == 0;
	}

	@Override
	public Set<Key> keySet() {
		return new AbstractSet<Key>() {

			@Override
			public Iterator<Key> iterator() {
				return new KeyIterator();
			}

			@Override
			public int size() {
				return EntityResultMap.this.size();
			}
			
		};
	}

	@Override
	public Entity put(Key arg0, Entity arg1) {
		throw new UnsupportedOperationException("Not supported yet");
	}

	@Override
	public void putAll(Map<? extends Key, ? extends Entity> map) {
		throw new UnsupportedOperationException("Not supported yet");
	}

	@Override
	public Entity remove(Object arg0) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public int size() {
		return entities.length;
	}

	@Override
	public Collection<Entity> values() {
		return new AbstractCollection<Entity>() {

			@Override
			public Iterator<Entity> iterator() {
				return iterable.iterator();
			}

			@Override
			public int size() {
				return entities.length;
			}

		};
	}

	public final class EntityResultEntrySet extends
			AbstractSet<Map.Entry<Key, Entity>> {

		@Override
		public Iterator<java.util.Map.Entry<Key, Entity>> iterator() {

			return new EntityResultEntryIterator(iterable);
		}

		@Override
		public int size() {
			return entities.length;
		}

	}

	public final class EntityResultEntryIterator implements
			Iterator<java.util.Map.Entry<Key, Entity>> {

		private final Iterator<Entity> iter;

		public EntityResultEntryIterator(Iterable<Entity> iterable) {
			this.iter = iterable.iterator();
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public java.util.Map.Entry<Key, Entity> next() {
			if (!iter.hasNext()) {
				throw new NoSuchElementException();
			}
			final Entity entity = iter.next();
			return new Entry<Key, Entity>() {

				@Override
				public Key getKey() {
					return entity.getKey();
				}

				@Override
				public Entity getValue() {
					return entity;
				}

				@Override
				public Entity setValue(Entity arg0) {
					throw new UnsupportedOperationException(
							"Not supported yet.");
				}

			};
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

	}

	public class KeyIterator implements Iterator<Key> {

		private int next;

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
		public Key next() {
			return BytesHelper.toKey(entities[next++].getRow());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

	}

}
