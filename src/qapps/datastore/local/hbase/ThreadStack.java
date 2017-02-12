package qapps.datastore.local.hbase;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ThreadStack implements Closeable {

	private final ExecutorService executor;

	public ThreadStack() {
		this.executor = Executors.newCachedThreadPool();
	}

	public <T> Future<T> call(Callable<T> callable) {
		return executor.submit(callable);
	}

	public void close() {
		executor.shutdownNow();
	}
}
