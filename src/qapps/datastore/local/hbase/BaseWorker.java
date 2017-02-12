package qapps.datastore.local.hbase;

public abstract class BaseWorker {

	private final ThreadStack threadStack;

	public BaseWorker(ThreadStack stack) {
		this.threadStack = stack;
	}
}
