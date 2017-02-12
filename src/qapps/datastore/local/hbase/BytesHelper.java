package qapps.datastore.local.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.local.GaeHelper;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.ShortBlob;

public class BytesHelper {

	private static byte[] spliter = Bytes.toBytes("/");
	public static final byte SLASH = Bytes.toBytes("/")[0];
	public static final byte[] EMPTY = new byte[] {};
	private static byte[] intType = new byte[] { 0 };
	private static byte[] stringType = new byte[] { 1 };

	public static String toString(byte[] b) {
		return Bytes.toString(b);
	}

	public static Blob toBlob(byte[] b) {
		return new Blob(b);
	}

	public static ShortBlob toShortBlob(byte[] b) {
		return new ShortBlob(b);
	}

	public static long toLong(byte[] b) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(b);
		DataInputStream out = new DataInputStream(in);
		long result = out.readLong();
		out.close();
		return result;
	}

	public static byte toByte(byte[] b) {
		return b[0];
	}

	public static double toDouble(byte[] b) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(b);
		DataInputStream out = new DataInputStream(in);
		double result = out.readDouble();
		out.close();
		return result;
	}

	public static byte[] serialize(byte b) {
		return new byte[] { b };
	}

	public static byte[] serialize(Blob b) {
		return b.getBytes();
	}

	public static byte[] serialize(ShortBlob b) {
		return b.getBytes();
	}

	public static byte[] serialize(long l) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(b);
		out.writeLong(l);
		out.flush();
		out.close();
		return b.toByteArray();
	}

	public static byte[] serialize(String s) {
		return Bytes.toBytes(s);
	}

	public static byte[] serialize(double d) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(b);
		out.writeDouble(d);
		out.flush();
		out.close();
		return b.toByteArray();
	}

	public static byte[] serialize(Entity e) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(b);
		out.writeObject(e);
		out.flush();
		out.close();
		return b.toByteArray();
	}

	public static Entity toEntity(byte[] bytes) throws IOException {

		ObjectInputStream in = null;
		try {

			in = new ObjectInputStream(new ByteArrayInputStream(bytes));
			return (Entity) in.readObject();

		} catch (ClassNotFoundException e) {
			throw new IOException("Stored entity is corrupted, unkonwn type.",
					e);
		} finally {
			if (in != null)
				in.close();
		}

	}

	public static byte[] concat(byte[] a, byte[] b) {
		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	public static byte[] concat(byte[]... arrs) {
		if (arrs.length < 3)
			throw new IllegalArgumentException("Expected at least 3 arrays.");
		int size = 0;
		for (byte[] array : arrs) {
			if (array == null)
				throw new IllegalArgumentException("One of arras is null.");
			size += array.length;
		}
		byte[] result = new byte[size];
		size = 0;
		for (byte[] array : arrs) {
			System.arraycopy(array, 0, result, size, array.length);
			size += array.length;
		}
		return result;
	}

	public static byte[] raw(Key key) throws IOException {
		byte[] type;
		byte[] id;
		if (key.getName() == null) {
			type = intType;
			id = serialize(key.getId());
		} else {
			type = stringType;
			id = Bytes.toBytes(key.getName());
		}
		return concat(Bytes.toBytes(key.getKind() + "/"), type, id);
	}

	// public static Key toKey(byte[] b) {
	// GaeHelper.registerEnvironment();
	// Key parent = null, result = null;
	// String kind = null, name = null;
	// int start = 0, type, separator;
	// do {
	// type = indexOf(b, start, b.length - 1, slash) + 1;
	// System.out.println("type:" + type);
	// kind = Bytes.toString(b, start, type - start - 1);
	// if (b[type] == 0) {
	// result = KeyFactory.createKey(parent, kind,
	// Bytes.toLong(b, type + 1, 8));
	// start += kind.length() + 11;
	// } else {
	// type = indexOf(b, type + 1, b.length - 1, slash);
	// separator = indexOf(b, type + 1, b.length, slash);
	// name = Bytes.toString(b, type + 1, separator - type + 1);
	// name = name.substring(0, name.length() - 1);
	// result = KeyFactory.createKey(parent, kind, name);
	// start += kind.length() + name.length() + 3;
	// }
	// parent = result;
	// } while (start < b.length);
	// return result;
	//
	// }
	public static Key toKey(byte[] b) {
		GaeHelper.registerEnvironment();
		Key parent = null, result = null;
		String kind = null, name = null;
		int start = 0, type, separator;
		do {
			type = indexOf(b, start, b.length - 1, SLASH) + 1;
			// System.out.println("type:" + type + " count: " + count);
			kind = Bytes.toString(b, start, type - start - 1);
			if (b[type] == 0) {
				result = KeyFactory.createKey(parent, kind,
						Bytes.toLong(b, type + 1, 8));
				start += kind.length() + 11;
			} else {
				// type = indexOf(b, type + 1, b.length - 1, slash);
				separator = indexOf(b, type + 1, b.length, SLASH);
				if (separator == -1)
					separator = b.length - 1;
				name = Bytes.toString(b, type + 1, separator - type);
				result = KeyFactory.createKey(parent, kind, name);
				// System.out.println("name:" + name);
				start += kind.length() + name.length() + 3;
				// System.out.println("type:" + type + " name:" + name);
			}
			parent = result;
		} while (start < b.length);
		return result;

	}

	public static byte[] serialize(Key key) throws IOException {
		if (key.getParent() == null)
			return raw(key);
		int count = 1;
		Key parent = key.getParent();
		while (parent != null) {
			count++;
			parent = parent.getParent();
		}

		byte[][] keys = new byte[count * 2 - 1][];
		for (int i = count * 2 - 2; i > -1; i -= 2) {
			keys[i] = raw(key);
			if (i > 0)
				keys[i - 1] = spliter;
			key = key.getParent();
		}
		return concat(keys);
	}

	public static int indexOf(byte[] bytes, int offset, int endIndex, byte value) {
		for (; offset < endIndex; offset++) {
			if (bytes[offset] == value)
				return offset;
		}
		return -1;
	}

	public static int lastIndexOf(byte[] bytes, int offset, int endIndex,
			byte value) {
		offset = bytes.length - offset - 1;
		for (; offset >= endIndex; offset--) {
			if (bytes[offset] == value)
				return offset;
		}
		return -1;
	}
}
