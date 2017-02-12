package qapps.objectify;

import java.io.IOException;

import qapps.datastore.local.hbase.AsyncHbaseDatastoreService;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.googlecode.objectify.Objectify;
import com.googlecode.objectify.ObjectifyFactory;

public class CustomObjectifyService {

	private static CustomObjectifyFactory instance;
	private static boolean changed = false;

	public static ObjectifyFactory factory() {

		try {

			return (instance != null) ? instance
					: (instance = new CustomObjectifyFactory(
							AsyncHbaseDatastoreService.create()));

		} catch (IOException e) {
			throw new DatastoreFailureException("Initialization exception", e);
		}
	}

	public static Objectify begin() {
		return factory().begin();
	}

	public static void register(Class<?> clazz) {
		factory().register(clazz);
	}

	public synchronized static void setDatastore(DatastoreService service) {

		if (service == null)
			throw new IllegalArgumentException("Cannot change to NULL.");

		if (changed)
			throw new IllegalStateException(
					"Already has been changed, can be changed only once.");

		instance = new CustomObjectifyFactory(service);
	}

}
