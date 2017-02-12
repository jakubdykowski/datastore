package qapps.datastore.local.test;

import com.googlecode.objectify.Objectify;

public class Test {

	private Type type;

	public Test(Type type) {
		this.type = type;
	}

	public void run(Objectify ofy, int amount, boolean different) {
		type.run(ofy, amount, different);
	}

	public enum Type {
		GET {
			@Override
			public void run(Objectify ofy, int amount, boolean different) {
				for (int i = 0; i < amount; i++) {
//					ofy.
				}
			}
		},
		PUT {
			@Override
			public void run(Objectify ofy, int amount, boolean different) {
				// TODO Auto-generated method stub

			}
		};
		public abstract void run(Objectify ofy, int amount, boolean different);
	}
}
