package qapps.datastore.local.hbase;

import java.util.Collection;

import com.google.appengine.api.datastore.BaseDatastoreService;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

public class BaseHbaseDatastoreService implements BaseDatastoreService {

	final TransactionStack defaultTxnProvider;
	final HBaseDatastore ds;

	public BaseHbaseDatastoreService(HBaseDatastore ds,TransactionStack stack) {
		this.defaultTxnProvider = stack;
		this.ds = ds;
	}
	
	@Override
	public final Collection<Transaction> getActiveTransactions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final Transaction getCurrentTransaction() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final Transaction getCurrentTransaction(Transaction arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final PreparedQuery prepare(Query arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final PreparedQuery prepare(Transaction arg0, Query arg1) {
		// TODO Auto-generated method stub
		return null;
	}

}
