package qapps.datastore.local.hbase;

import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreAttributes;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Index;
import com.google.appengine.api.datastore.Index.IndexState;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import static qapps.datastore.local.hbase.FutureHelper.quietGet;

public final class HbaseDatastoreService extends BaseHbaseDatastoreService
		implements DatastoreService {

	private final AsyncHbaseDatastoreService async;

	public HbaseDatastoreService(AsyncHbaseDatastoreService async) {
		super(async.ds, async.defaultTxnProvider);
		this.async = async;
	}

	@Override
	public KeyRangeState allocateIdRange(KeyRange range) {
		throw new UnsupportedOperationException();
	}

	@Override
	public KeyRange allocateIds(String kind, long num) {
		return quietGet(async.allocateIds(kind, num));
	}

	@Override
	public KeyRange allocateIds(Key parent, String kind, long num) {
		return quietGet(async.allocateIds(parent, kind, num));
	}

	@Override
	public Transaction beginTransaction() {
		return quietGet(async.beginTransaction());
	}

	@Override
	public Transaction beginTransaction(TransactionOptions options) {
		return quietGet(async.beginTransaction(options));
	}

	@Override
	public void delete(Key... keys) {
		quietGet(async.delete(keys));
	}

	@Override
	public void delete(Iterable<Key> keys) {
		quietGet(async.delete(keys));
	}

	@Override
	public void delete(Transaction txn, Key... keys) {
		quietGet(async.delete(txn, keys));
	}

	@Override
	public void delete(Transaction txn, Iterable<Key> keys) {
		quietGet(async.delete(txn, keys));
	}

	@Override
	public Entity get(Key key) throws EntityNotFoundException {
		return quietGet(async.get(key));
	}

	@Override
	public Map<Key, Entity> get(Iterable<Key> keys) {
		return quietGet(async.get(keys));
	}

	@Override
	public Entity get(Transaction txn, Key key) throws EntityNotFoundException {
		return quietGet(async.get(txn, key));
	}

	@Override
	public Map<Key, Entity> get(Transaction txn, Iterable<Key> keys) {
		return quietGet(async.get(txn, keys));
	}

	@Override
	public DatastoreAttributes getDatastoreAttributes() {
		return quietGet(async.getDatastoreAttributes());
	}

	@Override
	public Map<Index, IndexState> getIndexes() {
		return quietGet(async.getIndexes());
	}

	@Override
	public Key put(Entity entity) {
		return quietGet(async.put(entity));
	}

	@Override
	public List<Key> put(Iterable<Entity> entities) {
		return quietGet(async.put(entities));
	}

	@Override
	public Key put(Transaction txn, Entity entity) {
		return quietGet(async.put(txn, entity));
	}

	@Override
	public List<Key> put(Transaction txn, Iterable<Entity> entities) {
		return quietGet(async.put(txn, entities));
	}

}
