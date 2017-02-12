package qapps.datastore.local.hbase.io;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.local.hbase.BytesHelper;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Key;

public class KindIndex {

	private KindIndex() {
	}

	private static byte[] SLASH = Bytes.toBytes("/");
	private static byte[] FAMILY = Bytes.toBytes("a");
	public static final String[] columns = new String[] { Bytes
			.toString(FAMILY) };

	private static KeyExtractor extractor = new KeyExtractor() {

		@Override
		public Key extractKey(Result result) {
			try {
				int start = BytesHelper.indexOf(result.getRow(), 0,
						result.getRow().length, BytesHelper.SLASH) + 1;
				byte[] byteKey = new byte[result.getRow().length - start];
				System.arraycopy(result.getRow(), start, byteKey, 0,
						byteKey.length);
				Key key = BytesHelper.toKey(byteKey);
				System.out.println("extracting key from Result: " + key);
				return key;

			} catch (Exception e) {
				throw new DatastoreFailureException(
						"Error while reading key from kind index row-key.", e);
			}
		}

	};

	public static KeyExtractor keyExtractor() {
		return extractor;
	}

	public static Put prepareIndex(byte[] key, byte[] kind) {
		return new Put(BytesHelper.concat(kind, SLASH, key)).add(
				Bytes.toBytes("a"), null, BytesHelper.EMPTY);
	}

	public static Scan prepareScan(String kind) {

		Scan scan = new Scan();

		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filters.addFilter(new FirstKeyOnlyFilter());
		filters.addFilter(new KeyOnlyFilter());
		scan.setFilter(filters);
		if (kind != null) {
			// TODO ensure implemented correctly
			scan.setStartRow(Bytes.toBytes(kind + "/"));
			scan.setStopRow(Bytes.toBytes(kind + "0"));
		} else
			throw new IllegalArgumentException(
					"Cannot search by kind where given kind is null, it's a nonsense.");

		return scan;

	}

	public static Delete prepareDelete(byte[] key, byte[] kind) {
		return new Delete(BytesHelper.concat(kind, SLASH, key));
	}
}
