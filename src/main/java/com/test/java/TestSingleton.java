package com.test.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

//JRE�жϳ����Ƿ�ִ�н����ı�׼�����е�ǰִ̨�߳�������ˣ������ܺ�̨�̵߳�״̬.
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
		/* ����ʡ�� */
	}

	public static void main(String[] args) throws Exception {
		TestSingleton t = TestSingleton.getInstance();
		
		for (int i = 0; i < 1000; i++) {
			System.out.println("cnt....." + i);
			Thread.currentThread().sleep(100);
		}
	}
}
