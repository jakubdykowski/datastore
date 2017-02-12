package qapps.objectify;

import qapps.datastore.local.CustomAsyncDatastoreService;

import com.google.appengine.api.datastore.AsyncDatastoreService;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.googlecode.objectify.ObjectifyFactory;

public class CustomObjectifyFactory extends ObjectifyFactory {

	private final DatastoreService service;

	public CustomObjectifyFactory(DatastoreService service) {
		this.service = service;
	}

	@Override
	protected AsyncDatastoreService getRawAsyncDatastoreService(
			DatastoreServiceConfig cfg) {
		return new CustomAsyncDatastoreService(service);
	}

	@Override
	protected DatastoreService getRawDatastoreService(DatastoreServiceConfig cfg) {
		return service;
	}

	// @Override
	// public Objectify begin() {
	// return begin(null);
	// }
	//
	// @Override
	// public Objectify begin(ObjectifyOpts opts) {
	// // TODO Auto-generated method stub
	// return createObjectify(new LocalAsyncDatastoreService(datastore), opts);
	// }

	// @Override
	// public <T> void register(Class<T> clazz) {
	// // TODO Auto-generated method stub
	// System.out.println("rejestrowanie: " + clazz);
	// // super.register(clazz);
	// registrar.register(clazz);
	// }
	//
	// @Override
	// public <T> EntityMetadata<? extends T> getMetadata(Class<T> clazz) {
	// EntityMetadata<T> metadata = this.registrar.getMetadata(clazz);
	// if (metadata == null)
	// throw new IllegalArgumentException("No class '" + clazz.getName()
	// + "' was registered.");
	// else
	// return metadata;
	// }
	//
	// /**
	// * Gets metadata for the specified kind, or throws an exception if the
	// kind
	// * is unknown
	// */
	// @Override
	// public <T> EntityMetadata<T> getMetadata(String kind) {
	// EntityMetadata<T> metadata = this.registrar.getMetadata(kind);
	// if (metadata == null)
	// throw new IllegalArgumentException("No class with kind '" + kind
	// + "' was registered");
	// else
	// return metadata;
	// }

}
