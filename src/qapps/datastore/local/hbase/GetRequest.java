package qapps.datastore.local.hbase;

import static qapps.datastore.local.hbase.Entities.ENTITY_B;
import static qapps.datastore.local.hbase.Entities.FAMILY;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import qapps.datastore.local.hbase.io.Request;

public class GetRequest implements Request, Cloneable {

	private final List<Get> keys = new LinkedList<>();
	private HBaseTransaction txn;

	public GetRequest() {
	}

	@Override
	public Result[] request(HTableInterface conn) throws IOException {
		if(txn != null) for(Get key : keys) key.setTimeStamp(txn.get)
		return conn.get(keys);
	}

	public void addKey(byte[] key) {
		keys.add(new Get(key).addColumn(FAMILY, ENTITY_B));
	}

	public void setTransaction(HBaseTransaction txn) {
		this.txn = txn;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		GetRequest cloned = new GetRequest();
		cloned.keys.addAll(keys);
		cloned.txn = txn;
		return cloned;
	}
}
