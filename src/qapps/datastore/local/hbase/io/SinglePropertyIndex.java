package qapps.datastore.local.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.berkeley.DataType;
import qapps.datastore.local.hbase.BytesHelper;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Key;

public class SinglePropertyIndex {

	private SinglePropertyIndex() {
	}

	private static byte[] SLASH = Bytes.toBytes("/");
	private static byte[] kindQualifier = Bytes.toBytes("t");
	private static byte[] valueQualifier = Bytes.toBytes("v");
	private static byte[] keyQualifier = Bytes.toBytes("k");
	private static byte[] FAMILY = Bytes.toBytes("d");
	private static byte[] EMPTY = {};
	public static final String[] COLUMNS = new String[] { Bytes
			.toString(FAMILY) };

	private static KeyExtractor extractor = new KeyExtractor() {

		@Override
		public Key extractKey(Result row) {
			try {
				// int start = BytesHelper.lastIndexOf(row.getRow(), 0, -1,
				// BytesHelper.slash) + 1;
				// byte[] keyBytes = new byte[row.getRow().length - start + 1];
				// System.arraycopy(row.getRow(), start, keyBytes, 0,
				// keyBytes.length);

				byte[] keyBytes = new byte[extractKeySize(row.getRow())];
				int toSkip = keyBytes.length / 127;

				System.arraycopy(row.getRow(), row.getRow().length
						- keyBytes.length - toSkip, keyBytes, 0,
						keyBytes.length);

				return BytesHelper.toKey(keyBytes);
			} catch (Exception e) {
				throw new DatastoreFailureException(
						"Error while reading key from kind index row-key.", e);
			}
		}

	};

	public static KeyExtractor getKeyExtractor() {
		return extractor;
	}

	@Deprecated
	public static Put prepareIndexOld(byte[] key, byte[] kind, byte[] name,
			byte[] value) {
		Put put = new Put(BytesHelper.concat(name, SLASH, value, SLASH, key));
		put.add(FAMILY, valueQualifier, value);
		put.add(FAMILY, keyQualifier, key);
		if (kind != null)
			put.add(FAMILY, kindQualifier, kind);
		return put;
	}

	public static Put prepareIndex(byte[] key, byte[] kind, byte[] name,
			byte[] value, boolean reversedOrder) {
		if (reversedOrder)
			value = reverseArray(value);
		Put put = new Put(BytesHelper.concat(kind, SLASH, name, SLASH, value,
				SLASH, key, buildKeyPointer(key.length)));
		put.add(FAMILY, EMPTY, null);
		return put;
	}

	public static Scan prepareScan(byte[] kind, byte[] property, byte[] value,
			CompareFilter.CompareOp operator, boolean reversedOrder) {

		assert operator != CompareFilter.CompareOp.NOT_EQUAL;

		Scan scan = new Scan();

		if (reversedOrder) {
			value = reverseArray(value);
		}

		switch (operator) {

		case EQUAL:
			scan.setStartRow(BytesHelper.concat(kind, SLASH, property, SLASH,
					value, SLASH));
			scan.setStopRow(BytesHelper.concat(kind, SLASH, property, SLASH,
					value, Bytes.toBytes("0")));
			break;
		case LESS:
			scan.setStartRow(BytesHelper.concat(kind, SLASH, property, SLASH));
			scan.setStopRow(BytesHelper.concat(kind, SLASH, property, SLASH,
					value, SLASH));
			break;
		case LESS_OR_EQUAL:
			scan.setStartRow(BytesHelper.concat(kind, SLASH, property, SLASH));
			scan.setStopRow(BytesHelper.concat(kind, SLASH, property, SLASH,
					value, Bytes.toBytes("0")));
			break;
		case GREATER:
			scan.setStartRow(BytesHelper.concat(kind, SLASH, property, SLASH,
					value, Bytes.toBytes("0")));
			scan.setStopRow(BytesHelper.concat(kind, SLASH, property,
					Bytes.toBytes("0")));
			break;
		case GREATER_OR_EQUAL:
			scan.setStartRow(BytesHelper.concat(kind, SLASH, property, SLASH,
					value, SLASH));
			scan.setStopRow(BytesHelper.concat(kind, SLASH, property,
					Bytes.toBytes("0")));
			break;
		}

		if (reversedOrder) {
			byte[] temp = scan.getStartRow();
			scan.setStartRow(scan.getStopRow());
			scan.setStopRow(temp);
		}

		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filters.addFilter(new FirstKeyOnlyFilter());
		filters.addFilter(new KeyOnlyFilter());
		scan.setFilter(filters);

		System.out
				.println("preparing scan =>" + "property:" + property
						+ " value:" + value + " kind:" + kind + " operator:"
						+ operator);
		return scan;
	}

	@Deprecated
	public static Scan prepareScanOld(Scan scan, String kind, String property,
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
					FAMILY, kindQualifier, CompareFilter.CompareOp.EQUAL,
					Bytes.toBytes(kind));
			filter.setFilterIfMissing(true);
			filters.addFilter(filter);
		}
		// filter value
		filters.addFilter(new SingleColumnValueFilter(FAMILY, valueQualifier,
				operator, valueBytes));

		scan.setFilter(filters);
		System.out
				.println("preparing scan =>" + "property:" + property
						+ " value:" + value + " kind:" + kind + " operator:"
						+ operator);
		return scan;
	}

	private static byte[] reverseArray(byte[] array) {
		
		// TODO ensure copying the array
		byte[] reversedValue = new byte[array.length];
		System.arraycopy(array, 0, reversedValue, 0, array.length);

		for (int i = 0; i < array.length / 2; i++) {
			byte temp = array[i];
			array[i] = array[array.length - i - 1];
			array[array.length - 1 - i] = temp;
		}

		return reversedValue;
	}

	private static byte[] buildKeyPointer(int size) {
		if (size < 127)
			return new byte[] { (byte) size };
		else {
			int parts = size / 127;
			byte[] result = new byte[parts];
			for (int i = parts - 1; i > 0; i--)
				result[i] = 127;
			result[0] = (byte) (size % 127);
			return result;
		}
	}

	private static int extractKeySize(byte[] row) {
		int size = 0;
		for (int i = row.length - 1; i >= 0; i--) {
			if (row[i] < 127)
				return size + row[i];
			else
				size += row[i];
		}
		assert true;
		throw new RuntimeException(
				"FatalError, implementation does not work correctly. Contact with developer.");
	}
}
