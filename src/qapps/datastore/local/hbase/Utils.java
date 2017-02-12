package qapps.datastore.local.hbase;

import com.google.appengine.api.datastore.Key;

public class Utils {

	public static boolean isDescendantOf(Key key, Key ancestor) {
		if (key.equals(ancestor))
			return true;
		Key parent = key.getParent();
		while (parent != null && !parent.equals(ancestor)) {
			parent = parent.getParent();
		}
		if (parent != null)
			return true;
		else
			return false;
	}

	public static Key getEntityGroup(Key key) {
		if (key == null)
			throw new IllegalArgumentException("NULL key has no parent SILLY.");
		Key parent = key.getParent();
		while((parent=parent.getParent()) != null);
		return parent;
	}
}
