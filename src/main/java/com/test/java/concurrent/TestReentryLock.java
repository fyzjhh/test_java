package com.test.java.concurrent;

import java.util.Calendar;
import java.util.concurrent.locks.ReentrantLock;

//JRE�жϳ����Ƿ�ִ�н���ı�׼�����е�ǰִ̨�߳�������ˣ���ܺ�̨�̵߳�״̬.
public class TestReentryLock {
	private ReentrantLock lock = null;
	// �����߳�ͬ�����ʵĹ������
	public int data = 100;

	public TestReentryLock() {
		// ����һ�����ɾ���Ŀ�������
		lock = new ReentrantLock();
	}

	public static void main(String[] args) {

		TestReentryLock tester = new TestReentryLock();

		// ���Կ����룬����testReentry() ִ�л�ȡ�����ʾ��Ϣ�Ĺ���
		tester.testReentry();
		// ��ִ�е�����������ʾ�������
		tester.testReentry();
		// �ٴ�����
		tester.testReentry();

		// �ͷ�������Ե���Ҫ�����������������������߳��޷���ȡ����
		tester.getLock().unlock();
		tester.getLock().unlock();
		tester.getLock().unlock();

		// ����3���̲߳��������µĹ������data�ķ���
		tester.test();
	}

	public ReentrantLock getLock() {
		return lock;
	}

	public void test() {
		new Thread(new WorkerThread(this)).start();
		new Thread(new WorkerThread(this)).start();
		new Thread(new WorkerThread(this)).start();
	}

	public void testReentry() {
		lock.lock();

		Calendar now = Calendar.getInstance();

		System.out.println(now.getTime() + " " + Thread.currentThread()
				+ " get lock.");
	}

	// �̵߳��õķ���
	public void testRun() throws Exception {
		// ����
		lock.lock();

		Calendar now = Calendar.getInstance();
		try {
			// ��ȡ�����ʾ ��ǰʱ�� ��ǰ�����߳� ������ݵ�ֵ����ʹ������� + 1��
			System.out.println(now.getTime() + " " + Thread.currentThread()
					+ " accesses the data " + data++);

			// ģ�������?�����������һ��
			Thread.sleep(500);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// ����
			lock.unlock();
		}
	}
}

// �����̣߳�����TestServer.testRun
class WorkerThread implements Runnable {

	private TestReentryLock tester = null;

	public WorkerThread(TestReentryLock testLock) {
		this.tester = testLock;
	}

	public void run() {
		// ѭ�����ã����Լ����Թ������+1��Ȼ����ʾ����
		while (true) {
			try {
				// ����tester.testRun()
				tester.testRun();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
