package qapps.datastore.local.test;

public class Tester {

	private Object name;

	public Tester(String name) {
		this.name = name;
	}

	public void test(Runnable runnable) {
		long stop, start = System.currentTimeMillis();
		runnable.run();
		stop = System.currentTimeMillis();
		System.out.println("Test:" + name + " time: " + (stop - start));
	}

}
