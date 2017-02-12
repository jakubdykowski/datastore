package qapps.datastore.local;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.local.hbase.BytesHelper;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

public class Seraizliation {
	public static void main(String[] args) throws IOException {
		GaeHelper.registerEnvironment();
		Key key = KeyFactory.createKey("Car", 260L);
		byte[] byteKey = BytesHelper.serialize(key);
		System.out.println(Arrays.toString(byteKey));
		key = KeyFactory.createKey("Car", "GSZ123");
		byteKey = BytesHelper.serialize(key);
		System.out.println(Arrays.toString(byteKey));
		System.out.println(BytesHelper.toKey(byteKey));
		System.out.println((byte) '/');
		System.out.println((byte) '0');
		byte[] array = Bytes
				.toBytes("/");
		System.out.println(Arrays.toString(array));
		System.out.println(array.length);
	}
}
