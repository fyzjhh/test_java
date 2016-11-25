package com.test.java.concurrent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class StackPut extends Thread {
	private String threadname; // �̵߳����
	private BlockingDeque pool; // �Զ����

	public StackPut(String threadname, BlockingDeque bqueue) {
		this.threadname = threadname;
		this.pool = bqueue;
	}

	public void run() {
		int i = 0;
		while (true) {
			try {
				System.out.println(threadname + " put:" + i);
				pool.putFirst(i);
				i++;
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}

class QPut extends Thread {
	private String threadname; // �̵߳����
	private BlockingQueue pool; // �Զ����

	public QPut(String threadname, BlockingQueue bqueue) {
		this.threadname = threadname;
		this.pool = bqueue;
	}

	public void run() {
		int i = 0;
		while (true) {
			try {
				System.out.println(threadname + " put:" + i);
				pool.put(i);
				i++;
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}

class StackTake extends Thread {
	private String threadname; // �̵߳����
	private BlockingDeque pool; // �Զ����

	public StackTake(String threadname, BlockingDeque bqueue) {
		this.threadname = threadname;
		this.pool = bqueue;
	}

	public void run() {
		while (true) {
			try {
				int i = (Integer) pool.takeFirst();
				System.out.println(threadname + " get:" + i);
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}

class QTake extends Thread {
	private String threadname; // �̵߳����
	private BlockingQueue pool; // �Զ����

	public QTake(String threadname, BlockingQueue bqueue) {
		this.threadname = threadname;
		this.pool = bqueue;
	}

	public void run() {
		while (true) {
			try {
				int i = (Integer) pool.take();
				System.out.println(threadname + " get:" + i);
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}

public class TestJavaConcurrence implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3014716914084285225L;
	private static final int MIN_INDEX = 19968;
	private static final int MAX_INDEX = 40869;
	private static final String CR = "\r\n";
	private static final String TAB = "\t";
	OOMObject tables[];

	static class OOMObject {

	}

	public static void main(String[] args) throws Exception {

		// 创建一个可重用固定线程数的线程池
		ExecutorService pool = Executors.newFixedThreadPool(6);
		for (int i = 0; i < 10; i++) {
			Thread t = new MyThread("t" + i, 10000 + 1000 * i);
			pool.execute(t);
		}
		// 关闭线程池
		pool.shutdown();

		System.out.println();
		System.out.println("====success====");
	}

	private static void testScanner() {
		System.out.println("������");
		Scanner reader = new Scanner(System.in);
		int m = 0;
		while (reader.hasNextLine()) {
			String x = reader.nextLine();
			m = m + 1;
			System.out.println(x);
			if (x.equals("exit")) {
				break;
			}

		}
		System.out.printf("%d����", m);
	}

	private static void testblockstack() {
		BlockingDeque bqueue = new LinkedBlockingDeque(20);
		// �����̳߳�
		ExecutorService threadPool = Executors.newFixedThreadPool(5);
		StackPut p1 = new StackPut("����put A", bqueue);
		StackPut p2 = new StackPut("����put B", bqueue);
		StackTake t1 = new StackTake("����take A", bqueue);
		StackTake t2 = new StackTake("����take B", bqueue);
		// ���̳߳���ִ������
		threadPool.execute(p1);
		threadPool.execute(p2);
		threadPool.execute(t1);
		threadPool.execute(t2);
		// �رճ�
		threadPool.shutdown();
	}

	private static void testblockqueue() {
		BlockingQueue bqueue = new ArrayBlockingQueue(20);
		// �����̳߳�
		ExecutorService threadPool = Executors.newFixedThreadPool(4);
		QPut p1 = new QPut("����put A", bqueue);
		QPut p2 = new QPut("����put B", bqueue);
		QTake t1 = new QTake("����take A", bqueue);
		// ���̳߳���ִ������
		threadPool.execute(p1);
		threadPool.execute(p2);
		threadPool.execute(t1);
		// �رճ�
		threadPool.shutdown();
	}

	private static void testsemaphore() {
		MyPool myPool = new MyPool(20);
		// �����̳߳�
		ExecutorService threadPool = Executors.newFixedThreadPool(2);

		// �رճ�
		threadPool.shutdown();
	}

	private static void testLock() {
		// �����������ʵ��˻�
		MyCount myCount = new MyCount("95599200901215522", 10000);
		// ����һ�������
		ReadWriteLock lock = new ReentrantReadWriteLock(false);
		// ����һ���̳߳�
		ExecutorService pool = Executors.newFixedThreadPool(2);
		// ����һЩ���������û���һ�����ÿ�����Ĵ棬ȡ��ȡ�������ְ�
		User u1 = new User("����", myCount, -4000, lock, false);
		User u2 = new User("�������", myCount, 6000, lock, true);
		User u3 = new User("�������", myCount, -8000, lock, false);
		User u4 = new User("����", myCount, 800, lock, false);
		User u5 = new User("�������", myCount, 0, lock, true);
		// ���̳߳���ִ�и����û��Ĳ���
		pool.execute(u1);
		pool.execute(u2);
		pool.execute(u3);
		pool.execute(u4);
		pool.execute(u5);
		// �ر��̳߳�
		pool.shutdown();
	}

	private static void convertChar() throws UnsupportedEncodingException {
		System.out.println(Charset.defaultCharset().name());
		String[] srccharset = new String[] { "latin1", "CP936", "GB18030",
				"utf8" };
		String[] descharset = new String[] { "latin1", "CP936", "GB18030",
				"utf8" };
		String srcstr = "����";
		for (int i = 0; i < srccharset.length; i++) {
			byte[] bytes = srcstr.getBytes(srccharset[i]);
			for (int j = 0; j < bytes.length; j++) {
				System.out.print(bytes[j] + " ");
			}
			System.out.println();

			for (int k = 0; k < srccharset.length; k++) {
				String desstr = new String(bytes, descharset[k]);
				System.out.println(descharset[k] + "\t" + desstr);
			}
		}

	}

	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private int stackLength = 1;
	static int d = 11;
	static int a0 = d;
	static int b = 3;
	static int c = 5;
	static int m = 9;

	public void test1() {
		LinkedHashMap<String, String> chm = new LinkedHashMap<String, String>();
		// ConcurrentHashMap<String, String> chm = new ConcurrentHashMap<String,
		// String>(
		// 330, 0.75f, 50);

		chm.put("1", "21");
		chm.put("2", "22");
		chm.put("3", "23");
		// tables = new OOMObject[16];
		System.out.println("success");
	}

	public void stackLeak() {

		stackLength++;

		stackLeak();

	}

	private static long getrandom(long small, long large) throws Exception {

		return small + (long) ((large - small) * Math.random());

	}

	private static void testrnd() throws Exception {

		Random random = new Random();
		// 946656000 1420041600

		System.out.println(new Date(100, 0, 1).getTime());
		System.out.println(new Date(115, 0, 1).getTime());
		System.out.println(new Date(115, 0, 1).getTime()
				- new Date(100, 0, 1).getTime());

		System.out.println(dtft.format(new Date(getrandom(946656000000L,
				1420041600000L))));
		System.out.println(dtft.format(new Date(getrandom(946656000000L,
				1420041600000L))));
		System.out.println(dtft.format(new Date(getrandom(946656000000L,
				1420041600000L))));
		System.out.println(dtft.format(new Date(getrandom(946656000000L,
				1420041600000L))));
		//
		// System.out.println(random.nextInt());
		// System.out.println(random.nextInt());
		// System.out.println(random.nextInt());
		// System.out.println(random.nextLong());
		// System.out.println(random.nextLong());
		// System.out.println(random.nextLong());

	}

	private static void teststackoom() throws Exception {

		TestJavaConcurrence oom = new TestJavaConcurrence();

		try {

			oom.stackLeak();

		} catch (Exception e) {

			System.out.println("stack length:" + oom.stackLength);

			throw e;

		}

	}

	private static void testheapoom() {
		List<TestJavaConcurrence> list = new ArrayList<TestJavaConcurrence>();

		while (true) {

			list.add(new TestJavaConcurrence());

		}
	}

}

class User implements Runnable {
	private String name; // �û���
	private MyCount myCount; // ��Ҫ�������˻�
	private int iocash; // �����Ľ���Ȼ����֮����
	private ReadWriteLock myLock; // ִ�в�������������
	private boolean ischeck; // �Ƿ��ѯ

	User(String name, MyCount myCount, int iocash, ReadWriteLock myLock,
			boolean ischeck) {
		this.name = name;
		this.myCount = myCount;
		this.iocash = iocash;
		this.myLock = myLock;
		this.ischeck = ischeck;
	}

	public void run() {
		if (ischeck) {
			// ��ȡ����
			myLock.readLock().lock();
			System.out.println("����" + name + "���ڲ�ѯ" + myCount
					+ "�˻�����ǰ���Ϊ" + myCount.getCash());
			// �ͷŶ���
			myLock.readLock().unlock();
		} else {
			// ��ȡд��
			myLock.writeLock().lock();
			// ִ���ֽ�ҵ��
			System.out.println("д��" + name + "���ڲ���" + myCount + "�˻������Ϊ"
					+ iocash + "����ǰ���Ϊ" + myCount.getCash());
			myCount.setCash(myCount.getCash() + iocash);
			System.out.println("д��" + name + "����" + myCount + "�˻��ɹ������Ϊ"
					+ iocash + "����ǰ���Ϊ" + myCount.getCash());
			// �ͷ�д��
			myLock.writeLock().unlock();
		}
	}
}

/**
 * ���ÿ��˻���������͸֧
 */
class MyCount {
	private String oid; // �˺�
	private int cash; // �˻����

	MyCount(String oid, int cash) {
		this.oid = oid;
		this.cash = cash;
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public int getCash() {
		return cash;
	}

	public void setCash(int cash) {
		this.cash = cash;
	}

	@Override
	public String toString() {
		return "MyCount{" + "oid='" + oid + '\'' + ", cash=" + cash + '}';
	}
}

/**
 * һ����
 */
class MyPool {
	private Semaphore sp; // ����ص��ź���

	/**
	 * �صĴ�С�������С�ᴫ�ݸ��ź���
	 * 
	 * @param size
	 *            �صĴ�С
	 */
	MyPool(int size) {
		this.sp = new Semaphore(size);
	}

	public Semaphore getSp() {
		return sp;
	}

	public void setSp(Semaphore sp) {
		this.sp = sp;
	}
}

class MyThread extends Thread {
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String tablename; // �̵߳����
	private int x; // �����ź����Ĵ�С

	MyThread(String tablename, int x) {
		this.tablename = tablename;
		this.x = x;
	}

	public void run() {

		Connection conn;
		Statement pstmt;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager
					.getConnection("jdbc:mysql://192.168.12.142:3306/db1?user=root&password=ttxsdb@10");
			Long startTime = System.currentTimeMillis();
			String crttbl = "  create table IF NOT EXISTS " + tablename
					+ "(id int primary key, name varchar(20)) engine = innodb ";
			pstmt = conn.createStatement();
			pstmt.execute(crttbl);

			pstmt.close();

			PreparedStatement ps = conn.prepareStatement("replace into "
					+ tablename + " values(?,?);");
			;
			for (int i = 0; i < x; i++) {
				if (i % 500 == 499) {
					System.out.println(i + "====");
				}
				ps.setInt(1, i);
				ps.setString(2, "xxx" + i);
				ps.execute();
			}
			ps.close();

			Long endTime = System.currentTimeMillis();
			Date spenttime = new Date(endTime - startTime);
			System.out.println("spent time :" + format.format(spenttime));
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
