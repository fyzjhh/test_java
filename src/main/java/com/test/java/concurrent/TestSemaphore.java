package com.test.java.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class TestSemaphore {

	public static void main(String[] args) throws Exception {

		testcdl();
	}

	private static void testcdl() {
		ExecutorService exec = Executors.newCachedThreadPool();
		final Semaphore semp = new Semaphore(5);
		for (int index = 0; index < 20; index++) {
			Runnable run = new XWorker(semp, index);
			exec.execute(run);
		}
		// �˳��̳߳�
		exec.shutdown();
	}

}

class XWorker implements Runnable {
	Semaphore semp;
	int no;

	public XWorker(Semaphore semp, int no) {
		super();
		this.semp = semp;
		this.no = no;
	}

	public void run() {
		try {
			// ��ȡ���
			semp.acquire();
			System.out.println("Accessing: " + no);
			Thread.sleep((long) (Math.random() * 10000));
			// ��������ͷ�
			semp.release();
		} catch (InterruptedException e) {
		}

	}

}
