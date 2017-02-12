package qapps.datastore.local.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

public interface Request {

	Result[] request(HTableInterface conn) throws IOException;
}
