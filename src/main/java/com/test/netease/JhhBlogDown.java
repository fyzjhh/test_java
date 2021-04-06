package com.test.netease.other;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class JhhBlogDown {
	private static final String FG = "--";
	private static final String TAG = "java|j2se|linux|kernel|db|database|mysql|oracle|sql|transaction|thread|���|���ݿ�|�ں�|�߳�|����|�㷨|����";
	static final String TEMP1 = "titlexxxx\n\n" + "bodyxxxx\n";
	static final String TEMP = "<html>                                         "
			+ "    <head>                                     "
			+ "        <title>                                "
			+ "            titlexxxx                          "
			+ "        </title>                               "
			+ "    </head>                                    "
			+ "                                               "
			+ "    <body>                                     "
			+ "                                               "
			+ "<h2>�û�=usernamexxxx</h2>		<hr> <p></p>      "
			+ "<h2>����=classnamexxxx</h2>		<hr> <p></p>  "
			+ "<h2>����=titlexxxx</h2>		<hr> <p></p>      "
			+ "                                               "
			+ "bodyxxxx                                       "
			+ "                                               "
			+ "    </body>                                    "
			+ "                                               "
			+ "</html>                                        ";
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	static long macrosecsinday = 24 * 3600 * 1000;
	static String basefd = "D:/temp/jbc/";
	static String ufile = "D:/temp/jbc/tmpuser.txt";
	static String[] users = { "panhaojhh" };
	static String uname = null;
	static Connection conn = null;
	static String url = "172.19.2.193:8888?key=D:/NetEase/ddb/ddb3/conf/secret.key&logdir=log/logblog";
	static String sqlacc = "select ID from Account where Username=? limit 1";
	// static String sqlname =
	// "select ID,Username from Account where ID=? limit 1";
	static String sqlblogcnt = "select count(ID) from Blog where UserID=? limit 1";
	static String sqlblogcls = "select count(ID) from Blog where UserID=? and ClassID=? limit 1";
	static String sqlblogclass = "select UserID,ID,ClassName,BlogCount,PublishedBlogCount,PublicBlogCount from BlogClass where UserID=? limit 100";

	static String sqlblog = "select UserName,ClassName,Title,Content,UserID,UserNickname,ClassID,ID,PublishTime,IP,AccessCount,Rank from Blog where UserID=? and ClassID=?";
	static Hashtable<String, String> userMap = new Hashtable<String, String>();

	static String sqlcata = "select UserID,ID,ClassName from BlogClass where (UserID>=0 and  UserID<100000) and (ClassName regexp 'java|j2se|linux|kernel|database|mysql|oracle') limit 100";


	static String sqlBCId = "select count(ID) from Blog where ID>=? and ID<? limit 1";

	static String sqlBCByTitle = "select UserName,ClassName,Title,Content,UserID,UserNickname,ClassID,ID,PublishTime,IP,AccessCount,Rank from Blog where ID>=? and ID<? and Title regexp ?";

	public static void main(String[] args) throws Exception {
		// getBlogsByUser();
		// getBlogsByCls();
		getBlogsByTitle();
	}


	/*
	 * blogid	-1	1180624635
	 * blogclassid	1	253603010
	 * ���Źؼ��� ��Ҫ�����ȡ������ �� ��Сid���
	 * ���ȵ�ؼ��� ��Ҫ��С��ȡ������ �� ����id���
	 */
	static String[] topics = { "windows 7" };
	static int topicBlogCnt = 0;
	static String topicDir = null;
	static String topic = null;
	static int BCLIMIT = 10;
	static long intVal = 20000, gateVal = 5000;
	private static void getBlogsByTitle() throws Exception {
		openConn();

		initDir(basefd);

		for (int i = 0; i < topics.length; i++) {
			topic = topics[i];
			topicBlogCnt = 0;

			long minId = 100000000;
			long tmpId = minId;
			topicDir = basefd + repfile(topic) + "/";
			initDir(topicDir);
			System.out.println("====start get blog for topic:" + topic);

			long starttime = System.currentTimeMillis();
			while (topicBlogCnt < BCLIMIT) {

				long endtime = System.currentTimeMillis();
				long costime = endtime - starttime;
				String s = "====blog cnt:" + topicBlogCnt + ",cost time:" + costime
						/ 1000 + ",end id:" + tmpId;
				System.out.println(s);
				if (costime > 15 * 60 * 1000) {
					break;
				}

				tmpId = loopBlogId(tmpId);
			}

		}

		closeConn();
	}

	private static long loopBlogId(long startId) throws Exception {

		long tmpStartId = startId;
		long endId = tmpStartId + intVal;

		long tmpAllCnt = 0;

		while (tmpAllCnt < gateVal) {
			long bcnt = getBlogCntInRange(tmpStartId, endId);
			tmpAllCnt = tmpAllCnt + bcnt;
			if (tmpAllCnt < gateVal) {
				tmpStartId = endId;
				endId = endId + intVal;
			} else {
				break;
			}
		}

		PreparedStatement stBCTitle = conn.prepareStatement(sqlBCByTitle);
		stBCTitle.setLong(1, startId);
		stBCTitle.setLong(2, endId);
		stBCTitle.setString(3, topic);
		ResultSet rsBCTitle = stBCTitle.executeQuery();
		int tmpcnt = 0;
		while (rsBCTitle.next()) {
			tmpcnt++;
			String unm = rsBCTitle.getString(1);
			String clsnm = rsBCTitle.getString(2);
			String title = rsBCTitle.getString(3);
			String bcontent = rsBCTitle.getString(4);

			String fntitle = null;
			if (title != null) {
				fntitle = repfile(title);
			} else {
				fntitle = "�տտտ�";
			}
			if (bcontent == null) {
				bcontent = "�տտտ�";
			}
			if (clsnm != null) {
				clsnm = repfile(clsnm);
			}
			if (unm != null) {
				unm = repfile(unm);
			}
			String fn = fntitle + ".html";
			String fullfn = topicDir + fn;

			FileOutputStream fos = new FileOutputStream(fullfn, true);
			OutputStreamWriter output = new OutputStreamWriter(fos, "GBK");

			String towrite = TEMP.replace("titlexxxx", title).replace(
					"bodyxxxx", bcontent).replace("usernamexxxx", unm).replace(
							"classnamexxxx", clsnm);
			output.write(towrite + "\n");
			output.close();

		}
		rsBCTitle.close();
		stBCTitle.close();

		topicBlogCnt = topicBlogCnt + tmpcnt;

		return endId;
	}

	private static long getBlogCntInRange(long startId, long endId)
			throws Exception {

		long ret = 0;
		PreparedStatement stBCId = conn.prepareStatement(sqlBCId);
		stBCId.setLong(1, startId);
		stBCId.setLong(2, endId);
		ResultSet rsBCId = stBCId.executeQuery();
		while (rsBCId.next()) {
			ret = rsBCId.getLong(1);
		}
		rsBCId.close();
		stBCId.close();
		return ret;
	}

	private static void getBlogsByCls() throws Exception {
		openConn();

		initDir(basefd);

		PreparedStatement stcata = conn.prepareStatement(sqlcata);
		ResultSet rscata = stcata.executeQuery();

		while (rscata.next()) {
			long uid = rscata.getLong(1);
			long clsid = rscata.getLong(2);
			String clsname = rscata.getString(3);

			PreparedStatement stblogcnt = conn.prepareStatement(sqlblogcls);
			stblogcnt.setLong(1, uid);
			stblogcnt.setLong(2, clsid);

			ResultSet blogcnt_rs = stblogcnt.executeQuery();
			while (blogcnt_rs.next()) {
				int blogcnt = blogcnt_rs.getInt(1);
				if (blogcnt >= 4) {

					System.out.println("get blogs ;para:" + uid + "," + clsid
							+ "," + clsname);
					getBlogsByUidCls(uid, clsid);
				}
			}

		}

		rscata.close();
		stcata.close();

		closeConn();
	}

	private static void getBlogsByUidCls(long uid, long clsid) throws Exception {

		PreparedStatement stblog = conn.prepareStatement(sqlblog);
		stblog.setLong(1, uid);
		stblog.setLong(2, clsid);

		ResultSet stblog_rs = stblog.executeQuery();

		while (stblog_rs.next()) {
			String unm = stblog_rs.getString(1);
			String clsnm = stblog_rs.getString(2);
			String title = stblog_rs.getString(3);
			String bcontent = stblog_rs.getString(4);

			String fntitle = null;
			if (title != null) {
				fntitle = repfile(title);
			} else {
				fntitle = "�տտտ�";
			}
			if (bcontent == null) {
				bcontent = "�տտտ�";
			}
			if (clsnm != null) {
				clsnm = repfile(clsnm);
			}
			if (unm != null) {
				unm = repfile(unm);
			}
			String clsdir = basefd + unm + FG + clsnm + "/";
			initDir(clsdir);

			String fn = fntitle + ".blog";
			String fullfn = clsdir + fn;

			FileOutputStream fos = new FileOutputStream(fullfn, true);
			OutputStreamWriter output = new OutputStreamWriter(fos, "GBK");

			String towrite = TEMP1.replace("titlexxxx", title).replace(
					"bodyxxxx", bcontent);
			output.write(towrite + "\n");
			output.close();
		}
		stblog_rs.close();

	}

	@SuppressWarnings("rawtypes")
	private static void getBlogsByUser() throws Exception {
		openConn();

		getUserMaps();

		initDir(basefd);

		Iterator iter = userMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			uname = ((String) entry.getKey()).trim();

			System.out.println("==========get blogs from :" + uname);

			JhhBlogDown.getBlogsFromUser();

		}

		closeConn();
	}

	public static void getUserMaps() throws Exception {
		if (users.length > 0) {
			String tmpStr = null;
			String uname = null;
			for (int i = 0; i < users.length; i++) {
				tmpStr = users[i].trim();
				String[] arrstr = tmpStr.split("[\t,]");
				if (arrstr != null && arrstr.length >= 1) {
					uname = arrstr[0];
					userMap.put(uname, uname);
				}
			}
		} else {

			File file = new File(ufile);
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				String tmpStr = null;

				String uname = null;

				while ((tmpStr = reader.readLine()) != null) {
					String[] arrstr = tmpStr.split("\t");
					if (arrstr != null && arrstr.length >= 1) {
						uname = arrstr[0];
						userMap.put(uname, uname);
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

	private static void initDir(String dir) {
		File b = new File(dir);
		if (b.exists() == false) {
			b.mkdirs();
		}
	}

	public static void openConn() throws Exception {

		Class.forName("com.netease.backend.db.DBDriver");
		conn = DriverManager.getConnection(url, "jhh", "jhh");

	}

	public static void closeConn() throws Exception {
		conn.close();
	}

	public static void getBlogsFromUser() throws Exception {

		PreparedStatement stfrom = conn.prepareStatement(sqlacc);
		stfrom.setString(1, uname);
		System.out.println("exec:" + sqlacc + ";para:" + uname);
		ResultSet rs = stfrom.executeQuery();

		while (rs.next()) {
			long uid = rs.getLong(1);
			PreparedStatement stblogcnt = conn.prepareStatement(sqlblogcnt);
			stblogcnt.setLong(1, uid);
			System.out.println("exec:" + sqlblogcnt + ";para:" + uid);
			ResultSet blogcnt_rs = stblogcnt.executeQuery();
			while (blogcnt_rs.next()) {
				int blogcnt = blogcnt_rs.getInt(1);
				if (blogcnt >= 10) {
					System.out.println(uname + " blog cnt :" + blogcnt);
					initDir(basefd + uname);
					getBlogs(uid);
				}
			}

		}

		rs.close();
		stfrom.close();

	}

	private static void getBlogs(long uid) throws Exception {
		PreparedStatement stbc = conn.prepareStatement(sqlblogclass);
		stbc.setLong(1, uid);
		System.out.println("exec:" + sqlblogclass + ";para:" + uid);
		ResultSet stbc_rs = stbc.executeQuery();

		while (stbc_rs.next()) {
			long userid = stbc_rs.getLong(1);
			long classid = stbc_rs.getLong(2);
			String clsname = stbc_rs.getString(3);
			if (null != clsname && clsname.toLowerCase().matches(TAG)) {

				System.out.println("exec:" + sqlblog + ";para:" + userid + ","
						+ clsname);

				// PreparedStatement stblog = conn.prepareStatement(sqlblog);
				// stblog.setLong(1, userid);
				// stblog.setLong(2, classid);
				//
				// ResultSet stblog_rs = stblog.executeQuery();
				//
				// while (stblog_rs.next()) {
				// String name = stblog_rs.getString(1);
				// String cls = stblog_rs.getString(2);
				// String title = stblog_rs.getString(3);
				// String blogcontent = stblog_rs.getString(4);
				//
				// String fntitle = null;
				// if (title != null) {
				// fntitle = repfile(title);
				// }
				// if (cls != null) {
				// cls = repfile(cls);
				// }
				// String fn = fntitle + ".blog";
				// String clsdir = basefd + name + "/" + cls;
				// String fullfn = clsdir + "/" + fn;
				// initDir(clsdir);
				// FileOutputStream fos = new FileOutputStream(fullfn, true);
				// OutputStreamWriter output = new OutputStreamWriter(fos,
				// "GBK");
				//
				// String towrite = TEMP1.replace("titlexxxx", title).replace(
				// "bodyxxxx", blogcontent);
				// output.write(towrite + "\n");
				// output.close();
				// }
				// stblog_rs.close();
			} else {
				System.out.println("���Է��� para:" + userid + "," + clsname);
			}
		}
		stbc_rs.close();
	}

	private static String repfile(String f) {
		if (f != null) {
			return f.replace("\\", "fxg").replace("/", "xg")
					.replace(".", "dian").replace("?", "wh")
					.replace("\t", "tab").replace("<", "xyh")
					.replace(">", "dyh").replace("\"", "syh")
					.replace("|", "sx").replace("*", "xh").replace(":", "mh");
		} else {
			return null;
		}
	}
}
