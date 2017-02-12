package qapps.datastore.local;

import java.io.IOException;
import java.util.Arrays;

import qapps.datastore.local.hbase.BytesHelper;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

public class Test3 {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
		GaeHelper.registerEnvironment();
		Key root = KeyFactory.createKey("c", 99L);
		Key key1 = KeyFactory.createKey(root, "a", 1L);
		Key key2 = KeyFactory.createKey(key1, "a", 2L);
		// System.out.println(key1.equals(key2));
		// System.out.println(Arrays.toString(BytesHelper.serialize(key1)));
		// System.out.println(Arrays.toString(BytesHelper.serialize(key2)));
		//
		// System.out.println(key1);
		// System.out.println(key2);
		// byte[] path = BytesHelper.toPath(key2);
		// System.out.println(Arrays.toString(path));
		// long l = 99;
		// System.out.println(BytesHelper.deserialize(BytesHelper.serialize(l)));
		// System.out.println(DataTypeUtils.getSupportedTypes());
		long start, stop;BytesHelper.serialize(key2);
		start = System.currentTimeMillis();
		
		byte[] b = BytesHelper.serialize(key2);
		stop = System.currentTimeMillis();
		System.err.println("Serializing time: " + (stop - start));
		start = System.currentTimeMillis();
		BytesHelper.toKey(b);
		stop = System.currentTimeMillis();
		System.err.println("Deserializing time: " + (stop - start));
		System.out.println(Arrays.toString(b));
		System.out.println((byte) 'c');
		System.out.println((byte) '/');
		System.out.println(BytesHelper.toKey(b));
	}

	// public static void a(Object o) {
	// System.out.println("Object");
	// }

	public static void a(String o) {
		System.out.println("String");
	}

	public static void a(Long o) {
		System.out.println("Long");
	}

	public static void a(long o) {
		System.out.println("long");
	}

}
