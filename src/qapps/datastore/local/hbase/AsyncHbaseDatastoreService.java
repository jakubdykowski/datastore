package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.DataTypeUtils;
import com.google.appengine.api.datastore.DatastoreAttributes;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Index;
import com.google.appengine.api.datastore.Index.IndexState;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.repackaged.com.google.common.collect.Iterables;

/**
 * Creating instance cause a bit of work and resources to be allocated. Should
 * be instanced only once or as less as it is possible.
 * 
 * @author qba
 * 
 */
public final class AsyncHbaseDatastoreService extends BaseHbaseDatastoreService
		implements AsyncDatastoreService {

	private final IdAllocator allocator;
	private final Getter getter;
	private final Saver saver;
	private final Deleter deleter;
	private final QueryBuilder builder;

	/*
	 * Forbidden. Use build() and then create or shortcut variants create() to
	 * make instances.
	 */
	private AsyncHbaseDatastoreService(HBaseDatastore schema)
			throws IOException {
		super(schema, new TransactionStackImpl());

		if (schema == null)
			throw new IllegalArgumentException();

		getter = new Getter(schema);
		saver = new Saver(schema);
		deleter = new Deleter(schema);
		builder = new QueryBuilder(schema, getter);
		allocator = new IdAllocator(schema);

		this.ds = schema;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#put(com.google.appengine
	 * .api.datastore.Entity)
	 */
	public Future<Key> put(Entity entity) {

		// TODO specify no txn defaults
		return put(null, entity);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#put(com.google.appengine
	 * .api.datastore.Transaction, com.google.appengine.api.datastore.Entity)
	 */
	@Override
	public Future<Key> put(Transaction txn, Entity entity) {

		List<Key> result = put(txn, Collections.singleton(entity));

		if (result.size() == 0 || result.get(0) == null)
			throw new DatastoreFailureException(
					"Failed to store entity. No result returned.");

		return result.get(0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#put(java.lang.Iterable
	 * )
	 */
	public Future<List<Key>> put(Iterable<Entity> entities) {

		// TODO specify no transaction defaults
		return put(null, entities);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#put(com.google.appengine
	 * .api.datastore.Transaction, java.lang.Iterable)
	 */
	@Override
	public Future<List<Key>> put(Transaction txn, Iterable<Entity> entities) {

		verifyTxn(txn);

		List<String> nullProperties = new LinkedList<>();

		try {

			// verify entities
			for (Entity entity : entities) {

				// generate id if necessary
				if (!entity.getKey().isComplete()) {

					Key generated;

					if (entity.getKey().getParent() == null)
						generated = this.allocateIds(entity.getKind(), 1L)
								.getStart();
					else
						generated = this.allocateIds(entity.getParent(),
								entity.getKind(), 1L).getStart();

					Entity newEntity = new Entity(generated);
					newEntity.setPropertiesFrom(entity);
					entity = newEntity;

				}

				for (Entry<String, Object> property : entity.getProperties()
						.entrySet()) {

					// check if any unsupported property types occurs
					if (property.getValue() != null) {
						if (!DataTypeUtils.isSupportedType(property.getValue()
								.getClass())) {
							throw new DatastoreFailureException(
									"Illegal property type: "
											+ property.getKey() + " => "
											+ property.getClass().getName());
						}
					} else {
						nullProperties.add(property.getKey());
					}
				}

				// TODO check whether properties with null should be stored
				if (nullProperties.size() > 0)
					System.err
							.println("NULL properties occured in entity while putting, removing them."
									+ entity.getKey() + nullProperties);
				for (String key : nullProperties)
					entity.removeProperty(key);
			}

			if (txn != null) {
				return this.saver.put((HBaseTransactionOld) txn, entities);
			} else {
				return this.saver.put(entities);
			}

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error while storing one or more entities.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#get(com.google.appengine
	 * .api.datastore.Key)
	 */
	@Override
	public Future<Entity> get(Key key) throws EntityNotFoundException {

		// TODO specify no txn defualts
		return get(null, key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#get(com.google.appengine
	 * .api.datastore.Key)
	 */
	@Override
	public Future<Entity> get(Transaction txn, Key key)
			throws EntityNotFoundException {

		// TODO implement transaction get
		Entity fetched;
		try {

			fetched = getter.get(verifyTxn(txn), key);

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error while fetching entity '" + key + "'.");
		}

		if (fetched == null) {
			throw new EntityNotFoundException(key);
		}

		return fetched;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#get(java.lang.Iterable
	 * )
	 */
	@Override
	public Future<Map<Key, Entity>> get(Iterable<Key> keys) {

		// TODO specify no txn defaults
		return get(null, keys);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.google.appengine.api.datastore.DatastoreService#get(java.lang.Iterable
	 * )
	 */
	@Override
	public Future<Map<Key, Entity>> get(Transaction txn, Iterable<Key> keys) {

		// check for completion of all given keys
		for (Key key : keys) {

			if (!key.isComplete())
				throw new IllegalArgumentException("One of given keys '" + key
						+ "' is incomplete.");
		}

		try {

			return getter.get(verifyTxn(txn), keys);

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error occured while fetching entities :"
							+ Iterables.toString(keys) + ".");
		}
	}

	@Override
	public Future<Void> delete(Key... keys) {
		return delete(null, keys);
	}

	@Override
	public Future<Void> delete(Iterable<Key> keys) {
		return delete(null, keys);
	}

	@Override
	public Future<Void> delete(Transaction txn, Key... keys) {
		return delete(txn, Arrays.asList(keys));
	}

	@Override
	public Future<Void> delete(Transaction txn, Iterable<Key> keys) {

		if (txn == null)
			throw new IllegalArgumentException("Given transaction is NULL.");
		if (!txn.isActive())
			throw new IllegalArgumentException(
					"Given transaction is not active.");

		for (Key key : keys) {

			// verify that all keys has id assigned
			if (key == null)
				throw new IllegalArgumentException("One of given keys is NULL.");
			if (!key.isComplete())
				throw new IllegalArgumentException(
						"One of given keys has no id.");
		}

		try {

			// TODO implement transactions
			this.deleter.delete(keys);

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error occured when deleting entities");
		}
		return null;
	}

	@Override
	public Future<KeyRange> allocateIds(String kind, long amount) {

		if (kind == null)
			throw new IllegalArgumentException("Kind is NULL.");

		if (amount < 1 || amount > 1000000000)
			throw new IllegalArgumentException(
					"Out of range: 1 -> 1 000 000 000");

		long last;
		try {

			last = allocator.generateRoot(amount);

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error while allocating next id.");
		}

		return new KeyRange(null, kind, last - amount + 1, last);
	}

	@Override
	public Future<KeyRange> allocateIds(Key parent, String kind, long amount) {

		if (!parent.isComplete())
			throw new IllegalArgumentException("Incomplete parent key.");

		if (kind == null)
			throw new IllegalArgumentException("Kind is NULL.");

		if (amount < 1 || amount > 1000000000)
			throw new IllegalArgumentException(
					"Out of range: 1 -> 1 000 000 000");

		long last;
		try {

			last = allocator.generateGroup(parent, amount);

		} catch (IOException e) {
			throw new DatastoreFailureException(
					"IO error while allocating new ids.");
		}

		return new KeyRange(parent, kind, last - amount + 1, last);
	}

	@Override
	public Future<Transaction> beginTransaction() {

		return beginTransaction(TransactionOptions.Builder.withDefaults());
	}

	@Override
	public Future<Transaction> beginTransaction(TransactionOptions options) {

		if (options == null) {
			return beginTransaction();
		}
		if (options.isXG()) {
			throw new UnsupportedOperationException(
					"Cross-Group transaction are not supported yet.");
		}

		// TODO create some txn id management
		return new HBaseTransaction(ds);
	}

	@Override
	public Future<DatastoreAttributes> getDatastoreAttributes() {

		// TODO implement
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Future<Map<Index, IndexState>> getIndexes() {

		// TODO implement
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * Checks is instance of HBaseTransaction and is still active and throw
	 * exception if not.
	 * 
	 * @param txn
	 * @return HBaseTransaction
	 * @throws IllegalStateException
	 *             if is not active.
	 * @throws RuntimeException
	 *             if is not instance of HBaseTransaction.
	 */
	private static HBaseTransaction verifyTxn(Transaction txn) {

		if (txn == null)
			return null;
		if (!(txn instanceof HBaseTransactionOld))
			throw new RuntimeException(
					"Implementation error, can receive only HBaseTransaction's.");
		if (txn != null && !txn.isActive())
			throw new IllegalStateException(
					"Provided transaction is not active.");
		return (HBaseTransaction) txn;
	}

	public static void mainn(String[] args) throws IOException {
		// Datastore ds = HBaseDatastore.create();

		Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.rootdir", "file:///home/hadoop/datastore");
		// conf.addResource(new Path("/opt/hadoop/conf/hbase-site.xml"));
		// System.out.println(conf.get("hbase.rootdir"));
		HBaseAdmin.checkHBaseAvailable(conf);
		HBaseAdmin admin = new HBaseAdmin(conf);

		// HTableDescriptor desc =
		// admin.getTableDescriptor("entities".getBytes());
		// desc.addFamily(new HColumnDescriptor("props"));
		//
		// admin.disableTable("entities");
		// admin.modifyTable("entities".getBytes(), desc);
		// admin.enableTable("entities");
		// // admin.createTable(new HTableDescriptor("entities"));
		HTableDescriptor[] tables = admin.listTables();

		for (HTableDescriptor table : tables) {
			System.out.println(table.getNameAsString());
			System.out.println(table.getMaxFileSize());
			for (HColumnDescriptor c : table.getColumnFamilies()) {
				System.out.println("    " + c.getNameAsString());
			}
		}
	}

	/**
	 * Shortcut for build().crate() .
	 * 
	 * @return
	 * @throws IOException
	 */
	public static AsyncHbaseDatastoreService create() throws IOException {
		return build().create();
	}

	/**
	 * Shortcut for build(configuration).create();
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static AsyncHbaseDatastoreService create(Configuration conf)
			throws IOException {
		return new Builder(conf).create();
	}

	/**
	 * Return instance builder with default configuration.
	 * 
	 * @return
	 */
	public static AsyncHbaseDatastoreService.Builder build() {
		return new Builder();
	}

	/**
	 * Calls for instance builder with given predefined configuration.
	 * 
	 * @param conf
	 * @return
	 */
	public static AsyncHbaseDatastoreService.Builder build(Configuration conf) {
		return new Builder(conf);
	}

	/**
	 * Allows building instance with chaining.
	 * 
	 * @author qba
	 * 
	 */
	public static class Builder {

		private final Configuration conf;
		private boolean built = false;
		/*
		 * Max hbase connections number.
		 */
		private int poolSize = 10;

		private Builder() {
			this(HBaseConfiguration.create());
		}

		private Builder(Configuration conf) {
			this.conf = conf;
		}

		public Builder setPort(int port) {
			conf.set("hbase.zookeeper.property.clientPort",
					String.valueOf(port));
			return this;
		}

		public Builder setQuorum(String[] urls) {
			StringBuilder builder = new StringBuilder();
			for (String s : urls) {
				builder.append(s);
				builder.append(',');
			}
			conf.set("hbase.zookeeper.quorum",
					builder.substring(0, builder.length()));
			return this;
		}

		public Builder setQuorum(String url) {
			conf.set("hbase.zookeeper.quorum", url);
			return this;
		}

		public Builder setConnectionsPoolSize(int size) {
			if (size < 1)
				throw new IllegalArgumentException("Size must be > 0.");
			poolSize = size;
			return this;
		}

		/**
		 * Once you call create() and it succeeds any following call cause
		 * IllegalStateException.
		 * 
		 * @return
		 * @throws IOException
		 */
		public synchronized AsyncHbaseDatastoreService create()
				throws IOException {

			if (built)
				throw new IllegalStateException(
						"Already has been built. Create new builder.");

			AsyncHbaseDatastoreService instance = new AsyncHbaseDatastoreService(
					new HBaseDatastore(conf, poolSize));
			built = true;
			return instance;
		}
	}

}
