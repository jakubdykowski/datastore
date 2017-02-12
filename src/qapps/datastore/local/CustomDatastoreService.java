//package qapps.datastore.local;
//
//import java.io.IOException;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import qapps.datastore.Datastore;
//
//import com.google.appengine.api.datastore.DataTypeUtils;
//import com.google.appengine.api.datastore.DatastoreAttributes;
//import com.google.appengine.api.datastore.DatastoreFailureException;
//import com.google.appengine.api.datastore.DatastoreService;
//import com.google.appengine.api.datastore.Entity;
//import com.google.appengine.api.datastore.EntityNotFoundException;
//import com.google.appengine.api.datastore.Index;
//import com.google.appengine.api.datastore.Index.IndexState;
//import com.google.appengine.api.datastore.Key;
//import com.google.appengine.api.datastore.KeyRange;
//import com.google.appengine.api.datastore.PreparedQuery;
//import com.google.appengine.api.datastore.Query;
//import com.google.appengine.api.datastore.Transaction;
//import com.google.appengine.api.datastore.TransactionOptions;
//
//@Deprecated
//public class CustomDatastoreService implements DatastoreService {
//
//	private final DatastoreService service;
//
//	public CustomDatastoreService(DatastoreService service) {
//		System.out.println("creating instance of " + this.getClass());
//		this.service = service;
//	}
//
//	@Override
//	public Collection<Transaction> getActiveTransactions() {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException("Not supported yet.");
//	}
//
//	@Override
//	public Transaction getCurrentTransaction() {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException("Not supported yet.");
//	}
//
//	@Override
//	public Transaction getCurrentTransaction(Transaction arg0) {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException("Not supported yet.");
//	}
//
//	@Override
//	public PreparedQuery prepare(Query query) {
//		return prepare(null, query);
//	}
//
//	@Override
//	public PreparedQuery prepare(Transaction txn, Query query) {
//		try {
//			return datastore.prepare(query);
//		} catch (IOException e) {
//			throw new DatastoreFailureException("Error while preparing query",
//					e);
//		}
//	}
//
//	@Override
//	public KeyRangeState allocateIdRange(KeyRange range) {
//
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException("Not supported yet.");
//	}
//
//	@Override
//	public KeyRange allocateIds(String kind, long num) {
//
//		if (kind == null)
//			throw new IllegalArgumentException("Kind is NULL.");
//
//		if (num < 1 || num > 1000000000)
//			throw new IllegalArgumentException(
//					"Out of range: 1 -> 1 000 000 000");
//		try {
//
//			return datastore.allocateIds(kind, num);
//
//		} catch (IOException e) {
//			throw new DatastoreFailureException("IO error occured.", e);
//		}
//	}
//
//	@Override
//	public KeyRange allocateIds(Key parent, String kind, long num) {
//
//		if (hasNoId(parent))
//			throw new IllegalArgumentException("Invalid parent key.");
//
//		if (kind == null)
//			throw new IllegalArgumentException("Kind is NULL.");
//
//		if (num < 1 || num > 1000000000)
//			throw new IllegalArgumentException(
//					"Out of range: 1 -> 1 000 000 000");
//		try {
//
//			return datastore.allocateIds(parent, kind, num);
//
//		} catch (IOException e) {
//			throw new DatastoreFailureException("IO error occured.", e);
//		}
//
//	}
//
//	@Override
//	public Transaction beginTransaction() {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public Transaction beginTransaction(TransactionOptions options) {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public void delete(Key... keys) {
//		// TODO define default no txn behavior
//		delete(null, keys);
//	}
//
//	@Override
//	public void delete(Iterable<Key> keys) {
//		// TODO define default no txn behavior
//		delete(null, keys);
//	}
//
//	@Override
//	public void delete(Transaction txn, Key... keys) {
//
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException("Not supported yet.");
//	}
//
//	@Override
//	public void delete(Transaction txn, Iterable<Key> keys) {
//
//		// TODO implement deleting
//		throw new UnsupportedOperationException("Not supported yet.");
//	}
//
//	@Override
//	public Entity get(Key key) throws EntityNotFoundException,
//			DatastoreFailureException, IllegalArgumentException {
//
//		// TODO define default behavior if no txn is given
//		return get(null, key);
//	}
//
//	@Override
//	public Map<Key, Entity> get(Iterable<Key> keys)
//			throws IllegalArgumentException, DatastoreFailureException {
//
//		// TODO define default behavior if no txn is given
//		return get(null, keys);
//	}
//
//	@Override
//	public Entity get(Transaction txn, Key key) throws EntityNotFoundException,
//			IllegalStateException, IllegalArgumentException {
//
//		if (txn == null)
//			throw new IllegalArgumentException("Given transaction is NULL.");
//		if (!txn.isActive())
//			throw new IllegalArgumentException(
//					"Given transaction is not active.");
//
//		try {
//			// TODO implement transaction get
//
//			Entity fetched = datastore.getEntity(key);
//			if (fetched == null)
//				throw new EntityNotFoundException(key);
//			return fetched;
//
//		} catch (IOException e) {
//			throw new DatastoreFailureException(
//					"IO error occured while fetching.", e);
//		}
//	}
//
//	@Override
//	public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys)
//			throws DatastoreFailureException, IllegalStateException {
//
//		if (txn == null)
//			throw new IllegalArgumentException("Given transaction is NULL.");
//		if (!txn.isActive())
//			throw new IllegalArgumentException(
//					"Given transaction is not active.");
//
//		// valid keys
//		for (Key key : keys) {
//
//			if (key == null)
//				throw new IllegalArgumentException("One of given keys is NULL.");
//
//			if (hasNoId(key))
//				throw new IllegalArgumentException(
//						"One of the keys is invalid.");
//		}
//
//		System.out.println("fetching[" + keys + "]:");
//
//		try {
//
//			// TODO implement transaction get
//			return datastore.getEntity(keys);
//
//		} catch (IOException e) {
//			throw new DatastoreFailureException(
//					"IO error occured while fetching.", e);
//		}
//	}
//
//	@Override
//	public DatastoreAttributes getDatastoreAttributes() {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public Map<Index, IndexState> getIndexes() {
//		// TODO Auto-generated method stub
//		throw new UnsupportedOperationException(this.getClass().getSimpleName()
//				+ ".getIndexes() is not supported.");
//	}
//
//	@Override
//	public Key put(Entity entity) {
//
//		// TODO specify default no txn behavior
//		return put(null, entity);
//	}
//
//	@Override
//	public List<Key> put(Iterable<Entity> entities) {
//
//		// TODO specify default no txn behavior
//		return put(null, entities);
//	}
//
//	@Override
//	public Key put(Transaction txn, Entity entity) {
//
//		// TODO implement transactions
//		List<Key> result = put(txn, Collections.singleton(entity));
//
//		if (result.size() == 0 || result.get(0) == null)
//			throw new DatastoreFailureException("Failed to store entity.");
//
//		return result.get(0);
//	}
//
//	@Override
//	public List<Key> put(Transaction txn, Iterable<Entity> entities) {
//
//		List<String> nullProperties = new LinkedList<>();
//
//		try {
//
//			for (Entity entity : entities) {
//
//				// generate id if necessary
//				if (hasNoId(entity.getKey())) {
//
//					Key generated;
//
//					if (entity.getKey().getParent() == null)
//						generated = datastore.allocateIds(entity.getKind(), 1L)
//								.getStart();
//					else
//						generated = datastore.allocateIds(entity.getParent(),
//								entity.getKind(), 1L).getStart();
//
//					Entity newEntity = new Entity(generated);
//					newEntity.setPropertiesFrom(entity);
//					entity = newEntity;
//
//				}
//
//				for (Entry<String, Object> property : entity.getProperties()
//						.entrySet()) {
//
//					// check if any unsupported property types occurs
//					if (property.getValue() != null) {
//						if (!DataTypeUtils.isSupportedType(property.getValue()
//								.getClass())) {
//							throw new DatastoreFailureException(
//									"Illegal property type: "
//											+ property.getKey() + " => "
//											+ property.getClass().getName());
//						}
//					} else {
//						nullProperties.add(property.getKey());
//					}
//				}
//
//				// TODO check whether properties with null should be stored
//				if (nullProperties.size() > 0)
//					System.err
//							.println("NULL properties occured in entity while putting, removing them."
//									+ entity.getKey() + nullProperties);
//				for (String key : nullProperties)
//					entity.removeProperty(key);
//			}
//
//			// TODO implement transactions
//			return datastore.putEntity(entities);
//
//		} catch (IOException e) {
//			throw new DatastoreFailureException(
//					"IO error while storing entities", e);
//		}
//	}
//
//	private static boolean hasNoId(Key key) {
//		if (key.getKind() == null && key.getId() < 1)
//			return true;
//		else
//			return false;
//	}
//
//}
