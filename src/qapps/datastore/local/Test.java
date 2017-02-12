package qapps.datastore.local;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import qapps.datastore.local.entity.Car;
import qapps.datastore.local.entity.Person;
import qapps.objectify.CustomObjectifyService;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.Objectify;

public class Test {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		assert 1 != 2;
		
		System.out.println(Arrays.toString(new File(".").list()));
		GaeHelper.registerEnvironment();
		// NamespaceManager.getGoogleAppsNamespace();
		// NamespaceManager.set("local-datastore");
		// NamespaceManager.validateNamespace("local-datastore");
		
		CustomObjectifyService.register(Person.class);
		CustomObjectifyService.register(Car.class);

		Objectify ofy = CustomObjectifyService.begin();

		Person person = new Person();
		person.id = 3L;
		person.age = 19;
		person.pesel = 98546231451L;
		person.name = "Jan";
		person.surname = "Kowalski";

		// ofy.put(person);
		// Person p2 = new Person();
		// Key k = ofy.put(p2);
		// System.out.println(k);

		long start, stop;
		Key<Person> key = new Key<Person>(Person.class, 3L);

		start = System.currentTimeMillis();
		key = ofy.put(person);
		stop = System.currentTimeMillis();
		System.out.println("put DELAY: " + (stop - start));

		start = System.currentTimeMillis();
		Person fetched = ofy.get(key);
		stop = System.currentTimeMillis();
		System.out.println("get DELAY: " + (stop - start));
		System.out.println(fetched);

		start = System.currentTimeMillis();
		List<Person> list = ofy.query(Person.class).list();
		// ofy.query(Person.class).listKeys();
		stop = System.currentTimeMillis();
		System.out.println("query DELAY: " + (stop - start));
		System.out.println("entity result list: " + list);
		System.out.println("key result list:"
				+ ofy.query(Person.class).listKeys());
		// HBaseDatastore ds = HBaseDatastore.create();
		// for (Result r : ds.schema.byKind().getScanner(
		// new Scan(Bytes.toBytes("Person")))) {
		// System.out.println("query result: "
		// + KeyStrategy.BY_KIND.extractKey(r, "Person"));
		// }

		Car car = new Car();
		car.marka = "BMW";
		car.model = "M5";
		car.rejestracja = "GSZ242";
		ofy.put(car);
		
		car = new Car();
		car.marka = "Volkswagen";
		car.model = "Golf";
		car.rejestracja = "GMB487";
		ofy.put(car);

		car = new Car();
		car.marka = "Fiat";
		car.model = "126p";
		car.rejestracja = "GSZ653";
		ofy.put(car);

		car = new Car();
		car.marka = "BMW";
		car.model = "E92";
		car.rejestracja = "AAA999";
		ofy.put(car);

		System.out.println(ofy.query(Car.class).list());
		System.out.println(ofy.query(Person.class).list());

		// System.out.println(Arrays.toString(Bytes.toBytes("Person8f4as53d1as6")));
		// System.out.println(Arrays.toString(Bytes.toBytes("Person8")));
		// System.out.println(Arrays.toString(Bytes.toBytes("Person12")));
		// System.out.println(Arrays.toString(Bytes.toBytes("Person")));
		// System.out.println(Arrays.toString(Bytes.toBytes("Person" + new
		// byte[] {})));

		System.out.println(ofy.query(Car.class).filter("rejestracja >", "A")
				.order("-model").list());
		System.exit(0);
	}

}
