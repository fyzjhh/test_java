package com.test.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

//JRE判断程序是否执行结束的标准是所有的前台执线程行完毕了，而不管后台线程的状态.
public class TestSingleton {

	private static TestSingleton instance = new TestSingleton();

	private Timer asynChecker = new Timer(true);

	private Map<Integer, String> disabledPools = new HashMap<Integer, String>();

	private TestSingleton() {
		asynChecker.schedule(new TimerTask() {
			@Override
			public void run() {
				checkDisabledPools();
			}
		}, 10, 100);
	}

	public static TestSingleton getInstance() {
		return instance;
	}

	private void checkDisabledPools() {
		if (disabledPools.isEmpty())
			return;
		System.out.println("check.....");
		/* 以下省略 */
	}

	public static void main(String[] args) throws Exception {
		TestSingleton t = TestSingleton.getInstance();
		
		for (int i = 0; i < 1000; i++) {
			System.out.println("cnt....." + i);
			Thread.currentThread().sleep(100);
		}
	}
}
