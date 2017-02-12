package qapps.datastore.local.hbase;

import java.io.IOException;

import qapps.datastore.local.hbase.BytesHelper;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Key;

public enum DataType {
	Blob(Blob.class) {
		@Override
		protected Object extract(byte[] b) {
			return BytesHelper.toBlob(b);
		}

		@Override
		public byte[] serialize(Object o) {
			return BytesHelper.serialize((Blob) o);
		}
	},
	ShortBlob(ShortBlob.class) {
		@Override
		protected Object extract(byte[] b) {
			return BytesHelper.toShortBlob(b);
		}

		@Override
		public byte[] serialize(Object o) {
			return BytesHelper.serialize((ShortBlob) o);
		}
	},
	String(String.class) {
		@Override
		protected Object extract(byte[] b) {
			return BytesHelper.toString(b);
		}

		@Override
		public byte[] serialize(Object o) {
			return BytesHelper.serialize((String) o);
		}
	},
	Long(long.class) {
		@Override
		protected Object extract(byte[] b) throws IOException {
			return BytesHelper.toLong(b);
		}

		@Override
		public byte[] serialize(Object o) throws IOException {
			return BytesHelper.serialize(((Long) o).longValue());
		}
	},
	Byte(byte.class) {
		@Override
		protected Object extract(byte[] b) {
			return BytesHelper.toByte(b);
		}

		@Override
		public byte[] serialize(Object o) throws IOException {
			return BytesHelper.serialize(((Byte) o).byteValue());
		}
	},
	Double(double.class) {
		@Override
		protected Object extract(byte[] b) throws IOException {
			return BytesHelper.toDouble(b);
		}

		@Override
		public byte[] serialize(Object o) throws IOException {
			return BytesHelper.serialize(((java.lang.Double) o).doubleValue());
		}
	},
	Boolean(Boolean.class) {

		@Override
		public byte[] serialize(Object o) throws IOException {
			return BytesHelper.serialize((Byte) o);
		}

		@Override
		protected Object extract(byte[] b) throws IOException {
			return BytesHelper.toLong(b);
		}

	},
	Key(Key.class) {

		@Override
		public byte[] serialize(Object o) throws IOException {
			return BytesHelper.serialize((Key) o);
		}

		@Override
		protected Object extract(byte[] b) throws IOException {
			return BytesHelper.toKey(b);
		}

	};
	private Class<?> clazz;

	private DataType(Class<?> clazz) {
		this.clazz = clazz;
	}

	public Class<?> getDataType() {
		return clazz;
	}

	public static String[] stringValues() {
		DataType[] values = values();
		String[] strings = new String[values.length];

		for (int i = 0; i < strings.length; i++) {
			strings[i] = values[i].name();
		}
		return strings;
	}

	public Object deserialize(byte[] b) throws IOException {
		return extract(b);
	}

	public abstract byte[] serialize(Object o) throws IOException;

	protected abstract Object extract(byte[] b) throws IOException;
}
