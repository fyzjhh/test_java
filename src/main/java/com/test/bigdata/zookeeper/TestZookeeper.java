package com.test.zookeeper;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

//import com.netease.backend.sdfs.mds.bean.Admin;
//import com.netease.backend.sdfs.util.SdfsConvert;

public class TestZookeeper {

	private static final int SESSION_TIMEOUT = 30000;
	static String SP = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

	public static void main(String[] args) throws Exception {
		// delelezook("172.17.0.227:2182", "/appebox");
		// delelezook("172.17.4.115:2182", "/eboxperf");

		delelezook("10.100.83.100:2181", "/wangpan");
		delelezook("10.100.83.100:2181", "/wp");
		delelezook("10.100.83.100:2181", "/wpbak");
		delelezook("10.100.83.100:2181", "/nemr");
		delelezook("10.100.83.100:2181", "/nemrbak");
		delelezook("10.100.83.100:2181", "/sdfsbakbak");
		delelezook("10.100.83.100:2181", "/sdfs");
		delelezook("10.100.83.100:2181", "/sdfsbak");
		
		// testcreatenode();
		// delelezook("192.168.164.62:2181", "/lel");

		//adduser(new String[] { "172.17.4.115:2182", "/appebox", "jhh", "jhh" });
	}

	public static void adduser(String[] args) throws KeeperException,
			InterruptedException, IOException, NoSuchAlgorithmException {

		ZooKeeper zk = new ZooKeeper(args[0], 50000, null);
		System.out.println(args[3]);
		System.out.println(generateDigest(args[3]));
		Admin admin = new Admin(args[2], generateDigest(args[3]), "s");
		Set<Admin> admins = new HashSet<Admin>();
		admins.add(admin);
		zk.create(args[1] + "/admin", SdfsConvert.getBytes(admins),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.setData(args[1] + "/admin", SdfsConvert.getBytes(admins), -1);

	}

	static final private String base64Encode(byte b[]) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < b.length;) {
			int pad = 0;
			int v = (b[i++] & 0xff) << 16;
			if (i < b.length) {
				v |= (b[i++] & 0xff) << 8;
			} else {
				pad++;
			}
			if (i < b.length) {
				v |= (b[i++] & 0xff);
			} else {
				pad++;
			}
			sb.append(encode(v >> 18));
			sb.append(encode(v >> 12));
			if (pad < 2) {
				sb.append(encode(v >> 6));
			} else {
				sb.append('=');
			}
			if (pad < 1) {
				sb.append(encode(v));
			} else {
				sb.append('=');
			}
		}
		return sb.toString();
	}

	public static void delelezook(String server, String path) throws Exception {
		ZooKeeper zk = new ZooKeeper(server, TestZookeeper.SESSION_TIMEOUT,
				null);

		delete(path, zk);
		// for (int i = 1; i < 100; i++) {
		// delete("/lel_" + i, zk);
		// }

		// delete("/lel_100", zk);
		// delete("/xx", zk);
		// delete("/yy", zk);
		// delete("/z", zk);
	}

	public static void delete(String path, ZooKeeper zk) throws Exception {
		List<String> childs = zk.getChildren(path, false);
		for (String child : childs) {
			delete(path + "/" + child, zk);
		}
		System.out.println("delete " + path);
		zk.delete(path, -1);
	}

	static final private char encode(int i) {
		i &= 0x3f;
		if (i < 26) {
			return (char) ('A' + i);
		}
		if (i < 52) {
			return (char) ('a' + i - 26);
		}
		if (i < 62) {
			return (char) ('0' + i - 52);
		}
		return i == 62 ? '+' : '/';
	}

	static public String generateDigest(String Password)
			throws NoSuchAlgorithmException {
		byte digest[] = MessageDigest.getInstance("SHA1").digest(
				Password.getBytes());
		return base64Encode(digest);
	}

	private static void testcrtroot() throws IOException, InterruptedException,
			KeeperException {

		zk.create("/appebox", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create("/appeboxbak", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

	}

	static ZooKeeper zk;

	static Watcher wh = new Watcher() {
		public void process(org.apache.zookeeper.WatchedEvent event) {
			System.out.println(event.toString());
		}
	};
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static void testcreatenode() throws IOException,
			InterruptedException, KeeperException {

		for (int m = 1; m < 10; m++) {
			String rp = "/top" + m;

			ZooKeeper zk11 = new ZooKeeper(
					"172.17.2.165:2181,172.17.2.122:2181,172.17.0.113:2181",
					3000, wh);

			zk11.create(rp, ("rootpathvalue").getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);

			for (int i = 1; i <= 10000; i++) {
				String subpath = rp + "/v_" + i;

				if (i % 500 == 499) {
					System.out.println(format.format(new Date())
							+ " keys number : " + i + " for rootpath :" + rp);
				}
				zk11.create(subpath, (SP + i).getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
			// zk11.sync(path, cb, ctx);
			zk11.close();
		}

		// for (int i = 1; i <= 10; i++) {
		// String levels = "";
		// for (int j = 1; j <= i; j++) {
		// levels = levels + "/dd_" + j;
		// }
		//
		// System.out.println(new String(zk11.getData(levels, false, null)));
		// }

	}

	private void test1() throws IOException, InterruptedException,
			KeeperException {
		System.out.println(" ����  ,Ȩ�ޣ� OPEN_ACL_UNSAFE ,�ڵ����ͣ� Persistent");

		// for (int i = 1; i <= 10; i++) {
		// String levels = "";
		// for (int j = 1; j <= i; j++) {
		// levels = levels + "/lvl_" + j ;
		// }
		//
		// //System.out.println(levels);
		// zk.create(levels, ("data" + i).getBytes(), Ids.OPEN_ACL_UNSAFE,
		// CreateMode.PERSISTENT);
		// }

		for (int i = 1; i <= 10; i++) {
			String levels = "";
			for (int j = 1; j <= i; j++) {
				levels = levels + "/lvl_" + j;
			}

			System.out.println(new String(zk.getData(levels, false, null)));
		}

	}

	private void testgetall(String rp) throws IOException,
			InterruptedException, KeeperException {

		List<String> path = zk.getChildren(rp, false);

		for (Iterator<String> iterator = path.iterator(); iterator.hasNext();) {
			String p = (String) iterator.next();
			String relp = null;
			if (rp.equals("/")) {
				relp = rp + p;
			} else {
				relp = rp + "/" + p;
			}
			System.out.println(new String(zk.getData(relp, false, null)));
			testgetall(relp);
		}
	}

	private void ZKOperations() throws IOException, InterruptedException,
			KeeperException

	{

		System.out
				.println("/n1. ���� ZooKeeper �ڵ� (znode �� zoo2, ��ݣ� myData2 ��Ȩ�ޣ� OPEN_ACL_UNSAFE ���ڵ����ͣ� Persistent");

		zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

		System.out.println("/n2. �鿴�Ƿ񴴽��ɹ��� ");

		System.out.println(new String(zk.getData("/zoo2", false, null)));

		System.out.println("/n3. �޸Ľڵ���� ");

		zk.setData("/zoo2", "shenlan211314".getBytes(), -1);

		System.out.println("/n4. �鿴�Ƿ��޸ĳɹ��� ");

		System.out.println(new String(zk.getData("/zoo2", false, null)));

		System.out.println("/n5. ɾ��ڵ� ");

		zk.delete("/zoo2", -1);

		System.out.println("/n6. �鿴�ڵ��Ƿ�ɾ�� ");

		System.out.println(" �ڵ�״̬�� [" + zk.exists("/zoo2", false) + "]");

	}
}
