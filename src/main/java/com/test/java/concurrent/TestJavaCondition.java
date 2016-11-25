package com.test.java.concurrent;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TestJavaCondition {
	public static void main(String[] args) {
		final Business business = new Business();
		new Thread(new Runnable() {
			@Override
			public void run() {
				threadExecute(business, "sub");
			}
		}).start();
		threadExecute(business, "main");
	}

	public static void threadExecute(Business business, String threadType) {
		for (int i = 0; i < 100; i++) {
			try {
				if ("main".equals(threadType)) {
					business.main(i);
				} else {
					business.sub(i);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

class Business {
	private boolean bool = true;
	private Lock lock = new ReentrantLock();
	private Condition condition = lock.newCondition();

	public/* synchronized */void main(int loop) throws InterruptedException {
		lock.lock();
		try {
			while (bool) {
				condition.await();// this.wait();
			}
			for (int i = 0; i < 100; i++) {
				System.out.println("main thread seq of " + i + ", loop of "
						+ loop);
			}
			bool = true;
			condition.signal();// this.notify();
		} finally {
			lock.unlock();
		}
	}

	public/* synchronized */void sub(int loop) throws InterruptedException {
		lock.lock();
		try {
			while (!bool) {
				condition.await();// this.wait();
			}
			for (int i = 0; i < 10; i++) {
				System.out.println("sub thread seq of " + i + ", loop of "
						+ loop);
			}
			bool = false;
			condition.signal();// this.notify();
		} finally {
			lock.unlock();
		}
	}
}

class BoundedBuffer {
	final Lock lock = new ReentrantLock();// ������
	final Condition notFull = lock.newCondition();// д�߳�����
	final Condition notEmpty = lock.newCondition();// ���߳�����

	final Object[] items = new Object[100];// �������
	int putptr/* д���� */, takeptr/* ������ */, count/* �����д��ڵ����ݸ��� */;

	public void put(Object x) throws InterruptedException {
		lock.lock();
		try {
			while (count == items.length)
				// �����������
				notFull.await();// ����д�߳�
			items[putptr] = x;// ��ֵ
			if (++putptr == items.length)
				putptr = 0;// ���д����д�����е����һ��λ���ˣ���ô��Ϊ0
			++count;// ����++
			notEmpty.signal();// ���Ѷ��߳�
		} finally {
			lock.unlock();
		}
	}

	public Object take() throws InterruptedException {
		lock.lock();
		try {
			while (count == 0)
				// �������Ϊ��
				notEmpty.await();// �������߳�
			Object x = items[takeptr];// ȡֵ
			if (++takeptr == items.length)
				takeptr = 0;// ����������������е����һ��λ���ˣ���ô��Ϊ0
			--count;// ����--
			notFull.signal();// ����д�߳�
			return x;
		} finally {
			lock.unlock();
		}
	}
}