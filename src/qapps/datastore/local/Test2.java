package qapps.datastore.local;

import java.util.Random;

import qapps.datastore.local.entity.Person;
import qapps.datastore.local.test.Tester;
import qapps.objectify.CustomObjectifyService;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.Objectify;

public class Test2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final Random r = new Random(47);
		final Objectify ofy = CustomObjectifyService.begin();
		GaeHelper.registerEnvironment();
		CustomObjectifyService.register(Person.class);
		Person person;
		Key<Person> key;

		Tester put = new Tester("put1000");

		put.test(new Runnable() {

			@Override
			public void run() {
				Person person;
				for (int i = 1; i < 1000; i++) {
					person = new Person();
//					person.id = (long) i;
					person.name = "Kuba";
					person.pesel = r.nextLong();
					person.surname = "Dykowski";
					ofy.put(person);
				}
			}

		});

		Tester get = new Tester("get1000");

		get.test(new Runnable() {

			@Override
			public void run() {
				Person person;
				for (int i = 1; i < 1000; i++) {
					ofy.get(new Key<Person>(Person.class, i));
				}
			}

		});

	}
}
