package qapps.datastore.local.hbase;

import org.apache.hadoop.hbase.client.Scan;

import qapps.datastore.local.hbase.io.KeyExtractor;

public interface HBasePreparedQuery {

	Scan getScan();

	KeyExtractor getKeyExtractor();

	Table getTable();

}
