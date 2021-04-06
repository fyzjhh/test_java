package com.test.memcache;

import com.test.memcache.TestJavaMemcache.MemcacheWorkerThread;

/** �����̣߳�����ͳ�Ƹ��������̵߳�ִ����� */
public class MemcacheManagerThread extends Thread {

	private int[] lastCount;

	private int[] currentCount;

	public MemcacheWorkerThread[] threads;

	private int interval;

	public boolean finished = false;

	public MemcacheManagerThread(int length, int interval) {
		this.lastCount = new int[length];
		this.currentCount = new int[length];
		this.threads = new MemcacheWorkerThread[length];
		this.interval = interval;
	}

	@Override
	public void run() {
		while (true) {
			try {
				if (finished) {
					return;
				}
				for (int i = 0; i < threads.length; i++) {
					lastCount[i] = threads[i].Memcacheop.count;
				}

				sleep(interval * 1000);

				for (int i = 0; i < threads.length; i++) {
					currentCount[i] = threads[i].Memcacheop.count;
				}
				System.out.println("----------------------------");
				/** ��ӡ��interval����ڵĸ��������߳�ִ�в����Ĵ��� */
				for (int i = 0; i < currentCount.length; i++) {
					System.out.print(currentCount[i] - lastCount[i]);
					System.out.print(" ");
				}
				int total = 0;
				for (int i = 0; i < currentCount.length; i++) {

					total += currentCount[i] - lastCount[i];
				}
				System.out.println(" ");
				System.out.println("operation num in " + interval + "s: "
						+ total);
				System.out.println("");
			} catch (InterruptedException e) {
				return;
			}
		}
	}
}
