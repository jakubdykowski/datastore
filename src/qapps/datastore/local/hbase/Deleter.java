package qapps.datastore.local.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.local.hbase.io.KindIndex;

import com.google.appengine.api.datastore.Key;

public class Deleter {

	private HBaseDatastore schema;

	public Deleter(HBaseDatastore schema) {
		
		if (schema == null)
			throw new IllegalArgumentException();
		this.schema = schema;
	}

	public void delete(Key key) throws IOException {
		delete(Collections.singleton(key));
	}

	public void delete(Iterable<Key> keys) throws IOException {

		List<Delete> entity = new LinkedList<>();
		List<Delete> kind = new LinkedList<>();

		for (Key key : keys) {

			byte[] keyBytes = BytesHelper.serialize(key);
			byte[] kindBytes = Bytes.toBytes(key.getKind());

			// delete exact entity
			entity.add(new Delete(BytesHelper.serialize(key)));

			// delete index by kind
			kind.add(KindIndex.prepareDelete(keyBytes, kindBytes));

			// TODO delete all indexes now or have it done later (hbase job)
		}

		new BatchDelete(entity, schema, Table.ENTITIES).access();
		new BatchDelete(kind, schema, Table.BY_KIND).access();

		// TODO perform hbase operations for all indexes
	}

	private class BatchDelete extends HBaseAccess {

		private List<Delete> deletes;

		protected BatchDelete(List<Delete> deletes, HBaseDatastore schema, Table table) {
			super(schema, table);
			this.deletes = deletes;
		}

		@Override
		protected Object doWork(HTableInterface table) throws IOException {
			table.delete(deletes);
			return null;
		}

	}
}
