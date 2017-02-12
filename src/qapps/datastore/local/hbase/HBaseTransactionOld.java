package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;

public class HBaseTransactionOld implements Transaction {
	private static AtomicLong count = new AtomicLong(1);

	private long startTime;
	private final HBaseDatastore ds;
	private long id = count.getAndDecrement();
	private AtomicBoolean active = new AtomicBoolean(true);
	private Key entityGroup;
	private boolean started = false;

	HBaseTransactionOld(HBaseDatastore schema) {
		this.ds = schema;
	}

	@Override
	public void commit() {
		if (!active.compareAndSet(true, false)) {
			throw new IllegalStateException("Transaction is not active anymore.");
		}
		try {
			if (!EntitiesTable.commit(ds, this))
				throw new ConcurrentModificationException("Transaction commit failed.");
		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error while commiting transaction.");
		}
	}

	@Override
	public Future<Void> commitAsync() {
		return executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				commit();
				return null;
			}

		});
	}

	@Override
	public String getApp() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String getId() {
		return String.valueOf(id);
	}

	@Override
	public boolean isActive() {
		return active.get();
	}

	@Override
	public void rollback() {
		if (!active.compareAndSet(true, false)) {
			throw new IllegalStateException("Transaction is not active.");
		}
		// TODO
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Future<Void> rollbackAsync() {
		return executor.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				rollback();
				return null;
			}

		});
	}

	void verify() {
		if (!isActive()) {
			throw new IllegalStateException("Transaction is NOT active anymore.");
		}
	}
	

	Key getEntityGroup() {
		// if (entityGroup == null)
		// throw new IllegalStateException(
		// "Entity group has not been set yet.");
		// if (entityGroup == null)
		// throw new DatastoreFailureException(
		// "Transaction has no entity group set.");
		return entityGroup;
	}

	private void setEntityGroup(Key entityGroup) {

		if (entityGroup == null)
			throw new IllegalArgumentException("NULL argument.");

		if (entityGroup.getParent() != null)
			throw new DatastoreFailureException(
					"Argument should be the root entity of an entity group.");

		if (this.entityGroup != null)
			throw new IllegalStateException(
					"Transaction already have had assigned entity group.");

		this.entityGroup = entityGroup;
	}

	long getStartTime() {
		return this.startTime;
	}

//	private void setStartTime(long timestamp) {
//
//		if (timestamp < 1)
//			throw new IllegalArgumentException("Timestamp must be > 0.");
//
//		if (this.startTime != 0)
//			throw new IllegalStateException(
//					"Transaction already have had assigned start timestamp.");
//
//		this.startTime = timestamp;
//	}
	
	boolean isInitialized() {
		return this.started;
	}
	
	void initIfIsNot(Key entityGroup) throws IOException {
		if(!started) {
			started = true;
			setEntityGroup(entityGroup);
//			setStartTime(EntitiesTable.getLastCommited(BytesHelper.serialize(entityGroup), ds));
		}
	}
}
