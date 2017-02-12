package qapps.datastore.local.hbase;

abstract class BaseModule {

	final AsyncHbaseDatastoreService async;

	public BaseModule(AsyncHbaseDatastoreService async) {
		this.async = async;
	}

}
