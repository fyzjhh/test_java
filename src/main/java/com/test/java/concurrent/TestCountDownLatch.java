package com.test.java.concurrent;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestCountDownLatch {

	public static void main(String[] args) throws Exception {

		testcdl();
	}

	private static void testcdl() {
		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(8);

		for (int i = 0; i < 8; i++) {
			executor.execute(new Worker(i, latch));
		}
		latch.countDown();
		executor.shutdown();
	}

	private static void testcdl2() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(4);
		ExecutorService executor = Executors.newFixedThreadPool(4);
		long s = System.currentTimeMillis();
		for (int i = 0; i < 4; i++)
			// executor.execute(new XWorker(i, latch));
			latch.await();
		System.out.format("the 4 tyres were assembled. costs %d ms.%n",
				System.currentTimeMillis() - s);
		executor.shutdown();
	}
}

class Player implements Runnable {
	private String name;
	private CountDownLatch latch;

	public Player(String name, CountDownLatch latch) {
		super();
		this.name = name;
		this.latch = latch;
	}

	public void run() {
		try {
			latch.await();
			run1();
			System.out.format("Player %s reaches the endponit.%n", name);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void run1() throws InterruptedException {
		Thread.sleep(new Random().nextInt(1000));
	}
}

class Worker implements Runnable {
	private int id;
	private CountDownLatch latch;

	public Worker(int id, CountDownLatch latch) {
		this.id = id;
		this.latch = latch;
	}

	public void run() {
		try {
			System.out.format("tyre %d is assembling.%n", id);
			marshalTyre();
			System.out.format("tyre %d is assembled.%n", id);
			latch.countDown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private void marshalTyre() throws InterruptedException {
		Thread.sleep(new Random().nextInt(60000));
	}
}
