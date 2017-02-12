package qapps.datastore.local;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.local.hbase.BytesHelper;

public class Main {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		// LocalServiceTestHelper sh = new LocalServiceTestHelper(
		// new LocalTaskQueueTestConfig(),
		// new LocalDatastoreServiceTestConfig());
		// sh.setUp();
		// // ObjectInputStream in = new ObjectInputStream(new
		// // ObjectOutputStream(K));
		// com.google.appengine.api.datastore.Key one =
		// KeyFactory.createKey("en",
		// 8);
		// com.google.appengine.api.datastore.Key two =
		// KeyFactory.createKey("en",
		// 8);
		// System.out.println(one.equals(check(two)));
		// ObjectifyService.register(Person.class);
		// Objectify ofy = ObjectifyService.begin();
		// Person person = new Person();
		// Key<Person> id = ofy.put(person);
		// System.out.println(id);

		// byte[] a = {};
		// byte[] b = {};
		// System.out.println(Arrays.equals(a, b));

		
	}

	public static void a(Object o) {
		System.out.println("object");
	}

	public static void a(long l) {

	}

	public static void testA(long l, int amount) throws IOException {
		long start, stop;
		start = System.nanoTime();
		for (int i = 0; i < amount; i++) {
			BytesHelper.serialize(l + i);
		}
		stop = System.nanoTime();
		System.out.println("TESTa:" + l + " DELAY: " + (stop - start)
				+ " AVERAGE:" + (stop - start) / amount);
	}

	public static void testB(long l, int amount) throws IOException {
		long start, stop;
		start = System.nanoTime();
		for (int i = 0; i < amount; i++) {
			Bytes.toBytes(l + i);
		}
		stop = System.nanoTime();
		System.out.println("TESTb:" + l + " DELAY: " + (stop - start)
				+ " AVERAGE:" + (stop - start) / amount);
	}

	public static void testC(long l, int amount) throws IOException {
//		long start, stop;
//		start = System.nanoTime();
//		for (int i = 0; i < amount; i++) {
//			p.value = l + i;
//			BytesHelper.serialize(l);
//		}
//		stop = System.nanoTime();
//		System.out.println("TESTc:" + l + " DELAY: " + (stop - start)
//				+ " AVERAGE:" + (stop - start) / amount);
	}

	public static void testD(long l, int amount) throws IOException {
		long start, stop;
		start = System.nanoTime();
		for (int i = 0; i < amount; i++) {
			BytesHelper.serialize(l);
		}
		stop = System.nanoTime();
		System.out.println("TESTd:" + l + " DELAY: " + (stop - start)
				+ " AVERAGE:" + (stop - start) / amount);
	}

	public static void print(byte[] l) {
		System.out.println(l.length + Arrays.toString(l));
	}

	@SuppressWarnings("unchecked")
	public static <T> T check(T o) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(b);
		out.writeObject(o);
		out.flush();
		out.close();
		byte[] obj = b.toByteArray();
		System.out.println("size: " + obj.length);
		System.out.println(" " + Arrays.toString(obj));
		ByteArrayInputStream in = new ByteArrayInputStream(obj);
		return (T) new ObjectInputStream(in).readObject();
	}

	public static long checkD(long l) throws IOException,
			ClassNotFoundException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(b);
		out.writeLong(l);
		out.flush();
		out.close();
		byte[] obj = b.toByteArray();
		System.out.println("size: " + obj.length);
		System.out.println(" " + Arrays.toString(obj));
		ByteArrayInputStream in = new ByteArrayInputStream(obj);
		return new DataInputStream(in).readLong();
	}

}
