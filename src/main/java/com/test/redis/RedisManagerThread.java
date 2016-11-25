package com.test.redis;

import com.test.redis.TestRedis.RedisWorkerThread;

/** �����̣߳�����ͳ�Ƹ��������̵߳�ִ����� */
public class RedisManagerThread extends Thread {

	private int[] lastCount;

	private int[] currentCount;

	public RedisWorkerThread[] threads;

	private int interval;

	public boolean finished = false;

	public RedisManagerThread(int length, int interval) {
		this.lastCount = new int[length];
		this.currentCount = new int[length];
		this.threads = new RedisWorkerThread[length];
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
					lastCount[i] = threads[i].redisop.count;
				}

				sleep(interval * 1000);

				for (int i = 0; i < threads.length; i++) {
					currentCount[i] = threads[i].redisop.count;
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
