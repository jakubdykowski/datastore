package qapps.datastore.local.hbase.io;

import org.apache.hadoop.hbase.client.Result;

import com.google.appengine.api.datastore.Key;

public interface KeyExtractor {

	Key extractKey(Result row);
}
