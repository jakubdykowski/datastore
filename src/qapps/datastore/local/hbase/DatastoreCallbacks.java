package qapps.datastore.local.hbase;

import com.google.appengine.api.datastore.DeleteContext;
import com.google.appengine.api.datastore.PutContext;

/**
 * Internal interface describing the callback operations we support.
 * 
 */
interface DatastoreCallbacks {

	/**
	 * Runs all PrePut callbacks for the given context.
	 * 
	 * @param context
	 *            The callback context
	 */
	void executePrePutCallbacks(PutContext context);

	/**
	 * Runs all PostPut callbacks for the given context.
	 * 
	 * @param context
	 *            The callback context
	 */
	void executePostPutCallbacks(PutContext context);

	/**
	 * Runs all PreDelete callbacks for the given context.
	 * 
	 * @param context
	 *            The callback context
	 */
	void executePreDeleteCallbacks(DeleteContext context);

	/**
	 * Runs all PostDelete callbacks for the given context.
	 * 
	 * @param context
	 *            The callback context
	 */
	void executePostDeleteCallbacks(DeleteContext context);

}
