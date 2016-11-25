package com.test.java.concurrent;

public class TestThreadLocal {
	private ThreadLocal<Integer> seqNum = new ThreadLocal<Integer>() {
		public Integer initialValue() {
			return 0;
		}
	};

	public int getNextNum() {
		seqNum.set(seqNum.get() + 1);
		return seqNum.get();
	}

	public static void main(String[] args) {
		TestThreadLocal sn11 = new TestThreadLocal();
		TestThreadLocal sn12 = new TestThreadLocal();
		TestThread t1 = new TestThread(sn11, sn12);
		TestThread t2 = new TestThread(sn11, sn12);
		t1.start();
		t2.start();
		System.out.println("success");
	}

	private static class TestThread extends Thread {
		private TestThreadLocal sn1, sn2;

		public TestThread(TestThreadLocal sn1, TestThreadLocal sn2) {
			this.sn1 = sn1;
			this.sn2 = sn2;
		}

		public void run() {
			for (int i = 0; i < 2; i++) {
				System.out.println(Thread.currentThread().getName() + " "
						+ sn1.getNextNum() + " " + sn1.hashCode());
				System.out.println(Thread.currentThread().getName() + " "
						+ sn2.getNextNum() + " " + sn2.hashCode());
			}
		}
	}
}