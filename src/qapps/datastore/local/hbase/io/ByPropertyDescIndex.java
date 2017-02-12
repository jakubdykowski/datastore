package qapps.datastore.local.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.berkeley.DataType;
import qapps.datastore.local.hbase.BytesHelper;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Key;

@Deprecated
public class ByPropertyDescIndex {

	private ByPropertyDescIndex() {
	}

	private static byte[] slash = Bytes.toBytes("/");
	private static byte[] kindQualifier = Bytes.toBytes("t");
	private static byte[] valueQualifier = Bytes.toBytes("v");
	private static byte[] keyQualifier = Bytes.toBytes("k");
	private static byte[] family = Bytes.toBytes("d");
	public static final String[] columns = new String[] { Bytes
			.toString(family) };

	private static KeyExtractor extractor = new KeyExtractor() {

		@Override
		public Key extractKey(Result row) {
			try {
				// int start = BytesHelper.lastIndexOf(row.getRow(), 0, -1,
				// BytesHelper.slash) + 1;
				// byte[] keyBytes = new byte[row.getRow().length - start + 1];
				// System.arraycopy(row.getRow(), start, keyBytes, 0,
				// keyBytes.length);

				return BytesHelper.toKey(row.getValue(family, keyQualifier));
			} catch (Exception e) {
				throw new DatastoreFailureException(
						"Error while reading key from kind index row-key.", e);
			}
		}

	};

	public static KeyExtractor keyExtractor() {
		return extractor;
	}

	/*
	 * Copies the value array to new and reverse it.
	 */
	public static Put prepareIndex(byte[] key, byte[] kind, byte[] name,
			byte[] value) {
		// TODO ensure copying value array
		Put put = new Put(BytesHelper.concat(name, slash, value, slash, key));
		byte[] reversedValue = new byte[value.length];
		System.arraycopy(value, 0, reversedValue, 0, value.length);
		reverseArray(reversedValue);
		put.add(family, valueQualifier, reversedValue);
		put.add(family, keyQualifier, key);
		if (kind != null)
			put.add(family, kindQualifier, kind);
		return put;
	}

	public static Scan prepareScan(Scan scan, String kind, String property,
			Object value, CompareFilter.CompareOp operator) throws IOException {

		byte[] valueBytes = DataType.valueOf(value.getClass().getSimpleName())
				.serialize(value);

		// filter property name
		scan.setStartRow(Bytes.toBytes(property + "/"));
		scan.setStopRow(Bytes.toBytes(property + "0"));

		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		// filter kind
		if (kind != null) {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					family, kindQualifier, CompareFilter.CompareOp.EQUAL,
					Bytes.toBytes(kind));
			filter.setFilterIfMissing(true);
			filters.addFilter(filter);
		}
		// filter value
		filters.addFilter(new SingleColumnValueFilter(family, valueQualifier,
				operator, valueBytes));

		scan.setFilter(filters);
		System.out.println("preparing reversed scan =>" + "property:"
				+ property + " value:" + value + " kind:" + kind + " operator:"
				+ operator);
		return scan;
	}

	private static void reverseArray(byte[] array) {
		for (int i = 0; i < array.length / 2; i++) {
			byte temp = array[i];
			array[i] = array[array.length - i - 1];
			array[array.length - 1 - i] = temp;
		}
	}
}
