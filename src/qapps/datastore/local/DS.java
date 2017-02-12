package qapps.datastore.local;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.apphosting.api.ApiProxy;

import qapps.datastore.local.entity.Person;
import qapps.datastore.local.hbase.AsyncHbaseDatastoreService;

public class DS {

	/**
	 * @param args
	 * @throws EntityNotFoundException
	 * @throws IOException
	 */
	public static void main(String[] args) throws EntityNotFoundException,
			IOException {
		// TODO Auto-generated method stub

		DatastoreService datastore = AsyncHbaseDatastoreService.create();

		ApiProxy.setEnvironmentForCurrentThread(new ApiProxy.Environment() {

			@Override
			public boolean isLoggedIn() {
				// TODO Auto-generated method stub
				return true;
			}

			@Override
			public boolean isAdmin() {
				// TODO Auto-generated method stub
				return true;
			}

			@Override
			public String getVersionId() {
				// TODO Auto-generated method stub
				return "1";
			}

			@Override
			public String getRequestNamespace() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getEmail() {
				// TODO Auto-generated method stub
				return "jakub.dykowski@gmail.com";
			}

			@Override
			public String getAuthDomain() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Map<String, Object> getAttributes() {
				// TODO Auto-generated method stub
				return new HashMap<String, Object>();
			}

			@Override
			public String getAppId() {
				// TODO Auto-generated method stub
				return "local-datastore";
			}
		});

		Person person = new Person();
		person.age = 19;
		person.pesel = 98546231451L;
		person.name = "Jan";
		person.surname = "Nowak";

		Key key = datastore.put(new Entity("dfas", 8));
		System.out.println(datastore.get(key));
	}

}
