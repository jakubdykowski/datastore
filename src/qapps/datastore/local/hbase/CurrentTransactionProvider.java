package qapps.datastore.local.hbase;

import com.google.appengine.api.datastore.Transaction;

/**
 * Internal interface describing an object that provide the current transaction.
 * 
 */
interface CurrentTransactionProvider {
	Transaction getCurrentTransaction(Transaction defaultValue);
}
