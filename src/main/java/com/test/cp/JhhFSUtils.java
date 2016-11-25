package com.test.cp;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import com.netease.backend.dfs.DFSException;
import com.netease.backend.dfs.DFSManager;
import com.netease.backend.dfs.sn.StorageNodeDisk;
import com.netease.backend.dfs.util.DFSMeta;
import com.netease.backend.dfs.util.Util;
import com.netease.backend.mfs.MFSManager;
import com.test.java.charset.EncodingUtils;

public class JhhFSUtils extends DFSManager {
	static String[] whos = {};
	static String ufile = "D:/temp/jpm/tmpuser.txt";
	static String whopath = "D:/temp/jpm/";
	static Hashtable<String, String> toMap = new Hashtable<String, String>();
	static String basefd = "D:/temp/jpm/";
	private static String hosts = "db-33.photo.163.org:5558";
	static Connection conn = null;
	static String dburl = "172.17.3.85:8890?key=D:/NetEase/ddb/ddb3/conf/secret.key&logdir=log/log2";
	static String sqlfsfile = "select owner,docid,filename,from_unixtime(createtime/1000,'%Y-%m-%d %h:%i:%s') from FS_File where owner like '@popo.personal.xxxx%' and unix_timestamp()-(createtime/1000)<=40*24*3600 limit 1000";

	static JhhFSUtils fsutils = new JhhFSUtils();
	static HashMap<Long, String> idMap = new HashMap<Long, String>();

	static MFSManager mfs = new MFSManager();

	public static void main(String[] args) {
		try {
			test();
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static boolean test1() throws Exception {

		Class.forName("com.netease.backend.db.DBDriver");
		conn = DriverManager.getConnection("192.168.164.162:8888?key=D:/NetEase/ddb/ddb3/conf/secret.key&logdir=log/log9", "popomirror", "popomirror");
		return true;

	}

	public boolean launch() throws Exception {

		try {
			// super.masterType = 3;
			// super.launch(hosts);
			mfs.launch(hosts);
			Class.forName("com.netease.backend.db.DBDriver");
			conn = DriverManager.getConnection(dburl, "fs_mirror", "fs_mirror");
			return true;
		} catch (DFSException e) {
			e.printStackTrace();
			return false;
		}

	}

	public static void getToMaps() throws Exception {
		if (whos.length > 0) {
			String tmpStr = null;
			String uid = null;
			String uname = null;
			for (int i = 0; i < whos.length; i++) {
				tmpStr = whos[i].trim();
				String[] arrstr = tmpStr.split("[\t, ]");
				if (arrstr != null && arrstr.length >= 2) {
					uid = arrstr[0];
					uname = arrstr[1];
					toMap.put(uid, uname);
				}
			}
		} else {

			File file = new File(ufile);
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				String tmpStr = null;

				String uid = null;
				String uname = null;

				while ((tmpStr = reader.readLine()) != null) {
					String[] arrstr = tmpStr.split("\t");
					if (arrstr != null && arrstr.length >= 2) {
						uid = arrstr[0];
						uname = arrstr[1];
						toMap.put(uid, uname);
					}
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
					}
				}
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private static void test() throws Exception {
		boolean launched = fsutils.launch();
		if (launched) {
			getToMaps();
			Iterator iter = toMap.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String uid = (String) entry.getKey();
				String uname = (String) entry.getValue();
				System.out.println("======get from " + uid + "--" + uname);
				getDocids(uid);
				if (idMap.size() > 0) {

					whopath = basefd + uid + "--" + uname + "/";
					initDir(whopath);

					Iterator idit = idMap.entrySet().iterator();
					Long docid = null;
					String fn = null;
					// String retUrl = "xxxx";
					while (idit.hasNext()) {
						try {
							Map.Entry iden = (Map.Entry) idit.next();
							docid = (Long) iden.getKey();
							fn = (String) iden.getValue();

							// retUrl = fsutils.getNginxUrl(docid, fn);
							// write2File(fn, docid, retUrl);

							readfile(docid, fn);
							System.out.println("====get done... " + docid);
						} catch (Exception e) {
							System.out.println("====get fail!!! " + docid);
							continue;
						}
					}
					idMap.clear();
				}
			}
		}
		fsutils.closeRes();
	}

	private static void readfile(long docid, String fn) throws Exception {
		String fullfn = whopath + docid + "--" + fn;

		FileOutputStream fos = new FileOutputStream(new File(fullfn));

		byte[] buffer = new byte[65536];
		InputStream ofis = mfs.getFile(docid).getInputStream();

		while (ofis.read(buffer) != -1) {
			fos.write(buffer);
		}

		fos.flush();
		ofis.close();
		fos.close();

	}

	private static void initDir(String dir) {
		File desdir = new File(dir);
		if (desdir.exists() == false) {
			desdir.mkdir();
		}
	}

	private void closeRes() throws SQLException {
		if (conn != null) {
			conn.close();
		}
		// super.shutdown();
		mfs.shutdown();
		// fsutils.shutdown();
	}

	public String locateId(long docid) throws Exception {

		DFSMeta dfsmap = paramM.getMeta();
		String ip = null;
		StorageNodeDisk[] sns = dfsmap.querySNs(docid);
		for (int j = 0, n = sns.length; j < n; j++) {
			ip = Util.ip2String(sns[j].getIp());
		}
		return ip;

	}

	public static void getDocids(String username) throws Exception {

		Statement stfrom = conn.createStatement();
		String exestr = sqlfsfile.replace("xxxx", username);
		ResultSet rs = stfrom.executeQuery(exestr);

		while (rs.next()) {
			long docid = rs.getLong(2);
			String filename = rs.getString(3);
			idMap.put(docid, filename);
		}
		rs.close();
		stfrom.close();

	}

	public String getNginxUrl(long docid, String fileName) throws Exception {
		String host = locateId(docid);
		fileName = URLEncoder.encode(fileName, "UTF-8");
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + 1);
		String expiredTimestamp = "" + calendar.getTimeInMillis() / 1000;

		String ret = JhhFSUtils.getDownloadUrl(host, "directDL", docid + "",
				expiredTimestamp, fileName);
		return ret;
	}

	public static String getDownloadUrl(String host, String prefix,
			String docId, String timestamp, String fileName) {

		StringBuilder sb = new StringBuilder(100);
		sb.append("http://");
		sb.append(host);
		sb.append("/");
		if (prefix != null) {
			sb.append(prefix);
			sb.append("/");
		}
		String dt = EncodingUtils.DIGEST_KEY_STR_FileStation + timestamp
				+ docId;
		sb.append(EncodingUtils.getMD5ContentBase64(dt));
		sb.append("/");
		sb.append(timestamp);
		sb.append("/");
		sb.append(EncodingUtils.base64edDigest(docId));
		sb.append("/");
		sb.append(docId);
		sb.append("/");
		sb.append(fileName);

		return sb.toString();
	}

	public static void write2File(String fn, long docid, String furl)
			throws Exception {
		// new一个URL对象
		URL url = new URL(furl);
		// 打开链接
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		// 设置请求方式为"GET"
		conn.setRequestMethod("GET");
		// 超时响应时间为5秒
		conn.setConnectTimeout(5 * 1000);
		// 通过输入流获取图片数据
		InputStream inStream = conn.getInputStream();
		// 得到图片的二进制数据，以二进制封装得到数据，具有通用性
		byte[] data = readInputStream(inStream);
		// new一个文件对象用来保存图片，默认保存当前工程根目录

		String fullfn = whopath + docid + fn;
		File imageFile = new File(fullfn);
		// 创建输出流
		FileOutputStream outStream = new FileOutputStream(imageFile);
		// 写入数据
		outStream.write(data);
		// 关闭输出流
		outStream.close();
	}

	public static byte[] readInputStream(InputStream inStream) throws Exception {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		// 创建一个Buffer字符串
		byte[] buffer = new byte[1024];
		// 每次读取的字符串长度，如果为-1，代表全部读取完毕
		int len = 0;
		// 使用一个输入流从buffer里把数据读取出来
		while ((len = inStream.read(buffer)) != -1) {
			// 用输出流往buffer里写入数据，中间参数代表从哪个位置开始读，len代表读取的长度
			outStream.write(buffer, 0, len);
		}
		// 关闭输入流
		inStream.close();
		// 把outStream里的数据写入内存
		return outStream.toByteArray();
	}
}
