package qapps.datastore.local;

import java.util.HashMap;
import java.util.Map;

import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;

public class GaeHelper {
	private static Environment environment = new LocalEnvironment();

	public static void registerEnvironment() {
		if (ApiProxy.getCurrentEnvironment() == null)
			ApiProxy.setEnvironmentForCurrentThread(environment);
	}

	public static class LocalEnvironment implements Environment {
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
			System.out.println("getAttrbutes()");
			return new HashMap<String, Object>();
		}

		@Override
		public String getAppId() {
			// TODO Auto-generated method stub
			return "localDatastore";
		}
	}
}
