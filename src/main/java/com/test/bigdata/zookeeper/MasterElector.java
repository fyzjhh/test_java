package com.test.zookeeper;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class MasterElector {

	private static final int SESSION_TIMEOUT = 30000;
	private int myid = 0;
	private String basepath = "/jhh";

	public static void main(String[] args) throws Exception {
		MasterElector m = new MasterElector();
		m.connect();
		m.test();

		Thread.sleep(3600 * 1000);
	}

	private Watcher watcher = new Watcher() {
		public void process(WatchedEvent event) {

			String s = "";
			if (event.getType() == EventType.None) {

				// We are are being told that the state of the
				// connection has changed
				switch (event.getState()) {
				case SyncConnected:
					// 连接成功

					break;
				case Disconnected:
					// 连接断开，关闭服务

					break;
				case Expired:
				default:

					// 重新连接，服务可能得关闭和启动
					break;
				}

			}
			if (event.getType() == EventType.NodeCreated) {
				s = "" + event.getPath() + "\t" + event.getType();

				// 如果子节点数达到指定的值， 查询子节点，开始选举
			}
			if (event.getType() == EventType.NodeDeleted) {
				s = "" + event.getPath() + "\t" + event.getType();
				// 查询子节点，如果达到指定的值，重新选举
			}
			if (event.getType() == EventType.NodeDataChanged) {
				s = "" + event.getPath() + "\t" + event.getType();
				// 查看时候是master，如果是通知其他的master切换状态
			}
			if (event.getType() == EventType.NodeChildrenChanged) {
				s = "" + event.getPath() + "\t" + event.getType();
			}

			System.out.println(s);
		}
	};

	private ZooKeeper zooKeeper;

	/**
	 * 连接zookeeper <br>
	 * ------------------------------<br>
	 * 
	 * @throws IOException
	 */
	public void connect() throws Exception {
		zooKeeper = new ZooKeeper(
				"192.168.12.221:2281,192.168.12.221:2282,192.168.12.221:2283",
				SESSION_TIMEOUT, watcher);
		Stat s = zooKeeper.exists(basepath, false);
		if (s == null) {
			zooKeeper.create(basepath, "jhh".getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
	}

	/**
	 * 关闭连接 <br>
	 * ------------------------------<br>
	 */
	public void close() {
		try {
			zooKeeper.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void create() {
		String result = null;
		try {
			result = zooKeeper.create(basepath + "/" + myid,
					String.valueOf(myid).getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
			System.out.println("result:" + result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void test_exists() {
		try {
			Stat s = zooKeeper.exists("/jhh", true);
			System.out.println(s);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void test() {
		try {
			// zooKeeper.exists("/jhh", true);
			// String s = zooKeeper.create("/jhh", "jhh".getBytes(),
			// Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			Stat s = new Stat();

			byte[] bytes = zooKeeper.getData("/jhh", true, s);
			System.out.println(new String(bytes));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
