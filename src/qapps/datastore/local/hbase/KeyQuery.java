package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.List;

import com.google.appengine.api.datastore.Key;

/**
 * Prepared query fetching keys form datastore.
 * @author qba
 *
 */
interface KeyQuery {

	/*
	 * Performs acutal query to datastore.
	 */
	Iterable<Key> fetchKeys(HBaseDatastore datastore) throws IOException;
}
