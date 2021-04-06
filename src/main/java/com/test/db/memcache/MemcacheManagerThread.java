package com.test.memcache;

import com.test.memcache.TestJavaMemcache.MemcacheWorkerThread;

/** 管理线程，定期统计各个工作线程的执行情况 */
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
				/** 打印出interval间隔内的各个工作线程执行操作的次数 */
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
