package qapps.datastore.local.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

public interface Response<T> {

	T extract(Result[] response) throws IOException;
}
