package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.RowLock;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

class Journal {

	private Schema schema;

	/*
	 * only temporary artificial map instead of real datastore journal
	 */
	private Map<String, Map<Key, Entity>> journals = new HashMap<>();

	public Journal(Schema schema) {
		this.schema = schema;
	}

	void write(Iterable<Entity> entities, HBaseTransactionOld txn)
			throws IOException {

		Map<Key, Entity> journal = getJournal(txn);

		for (Entity entity : entities) {

			journal.put(entity.getKey(), entity);
		}
	}

	void delete(Iterable<Key> keys, HBaseTransactionOld txn) {

		Map<Key, Entity> journal = getJournal(txn);

		for (Key key : keys) {

			journal.put(key, null);
		}

	}

	void clear(HBaseTransactionOld txn) throws IOException {
		journals.remove(txn.getId());
	}

	/**
	 * 
	 * @param txn
	 * @return null if txn has no journal
	 */
	Map<Key, Entity> get(HBaseTransactionOld txn) {
		return journals.get(txn.getId());
	}

	private Map<Key, Entity> getJournal(HBaseTransactionOld txn, HTableInterface conn, RowLock lock) {

		Map<Key, Entity> journal = journals.get(txn.getId());

		if (journal == null) {
			journal = new HashMap<Key, Entity>();
			journals.put(txn.getId(), journal);
		}
		return journal;
	}
}
