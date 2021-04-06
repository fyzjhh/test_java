package com.test.netease.other;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netease.popo.common.marshal.StrProperty;
import com.netease.popo.common.pack.Unpack;
import com.netease.popo.common.protocol.LNotify;
import com.netease.popo.common.protocol.LinkFrame;
import com.netease.popo.common.service.ISessionService;

public class JhhPpMsg {
	private static final String FG = "--";
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	static long macrosecsinday = 24 * 3600 * 1000;
	static String basefd = "D:/temp/jpm/";
	static String ufile = "D:/temp/jpm/tmpuser.txt";
	static String uactfile = "D:/temp/jpm/uact.txt";
	static String from = "hzzhaotianyuan@corp.netease.com";
	static String toid = "hzyehe@corp.netease.com";
	static String toname = "hzyehe@corp.netease.com,yh";

	static String[] tos = {"hzyehe@corp.netease.com,yh"};
	static Hashtable<String, String> toMap = new Hashtable<String, String>();
	// static String fromdt = df.format(new Date());
	static String fromdt = "2013-04-01";
	static Connection conn = null;
	static String url = "192.168.164.162:8888?key=D:/NetEase/ddb/ddb3/conf/secret.key&logdir=log/log11";
	static String sqlfrom = "select distinct nfrom from leavenotify where nto=? and validflag=0 and addtime>='xxxx' limit 200";
	static String sqlmsg = "select id, nid, nfrom, nto, notify, addtime from leavenotify where nfrom=? and nto=? and validflag=0 and addtime>='xxxx'  limit 2000";
	static String sqluaction = "select uid,actionid,comment,optime from uactionstat where uid=? and actionid='login' order by optime desc limit 1";
	static String sqluinfo = "select uid,nick,birthday,city from uinfo where uid =? limit 1";
	static Hashtable<String, String> userMap = new Hashtable<String, String>(
			5000);
	static String sqluact = "select * from uactionstat where uid=? and optime>? and comment like 'Popo08%' order by optime desc limit 100";
	static String sqlactionstat = "select uid,optime from uactionstat where optime>'starttime' and optime<'endtime' and actionid='login' limit 2000";
	static String sqluinfosex = "select uid,nick,birthday,city from uinfo where uid in (xxxx) and sex=2";
	static String sqltmsg = "SELECT id, nid, nfrom, nto, notify, addtime FROM leavenotify WHERE (nid not in (50, 10014, 330)) AND (nto = ?) AND addtime>='xxxx' ORDER BY addtime DESC  limit 2000";

	public static void main(String[] args) throws Exception {
		// encodeContent("D:/temp/jpm/test.txt");
//		 getOnlineUsers();
		// getUserActions();
		getMsgOne2One();
		getMsgToFile();
//		getTMsgs();
	}

	@SuppressWarnings("rawtypes")
	private static void getUserActions() throws Exception {
		openConn();

		getToMaps();

		initDir(basefd);

		Iterator iter = toMap.entrySet().iterator();
		FileOutputStream fos = new FileOutputStream(uactfile, false);
		OutputStreamWriter output = new OutputStreamWriter(fos);
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String uid = (String) entry.getKey();
			String uname = (String) entry.getValue();

			toid = uid.trim();
			toname = uname.trim();
			fromdt = getFromOptime(toid);

			System.out.println("==========get uaction from :" + uid + ","
					+ uname + "\t" + fromdt);

			PreparedStatement stuact = conn.prepareStatement(sqluact);
			stuact.setString(1, toid);
			stuact.setString(2, fromdt);
			ResultSet rs = stuact.executeQuery();

			int uactcnt = 0;
			while (rs.next()) {
				String sact = rs.getString(2);
				String soptime = rs.getString(4);
				String scomm = rs.getString(3);
				uactcnt++;
				String oline = uactcnt + "\t" + toid + "\t" + toname + "\t"
						+ sact + "\t" + soptime + "\t" + scomm;
				System.out.println(oline);
				output.write(oline + "\n");
			}
			output.write("\n\n");
			rs.close();
			stuact.close();

		}
		output.close();
		closeConn();
	}

	private static void encodeContent(String fn) throws Exception {

		String reg_emts = "(.*)\\[emts\\](.*)\\[/emts\\](.*)";
		String reg_pic = "(.*)\\[pic\\](.*)\\[/pic\\](.*)";
		String reg_popo = "(.*)popo(.*)";
		String reg_qq = "(.*)[0-9]{6,12}(.*)";

		Pattern p_emts = Pattern.compile(reg_emts);
		Pattern p_pic = Pattern.compile(reg_pic);
		Pattern p_popo = Pattern.compile(reg_popo);
		Pattern p_qq = Pattern.compile(reg_qq);

		File file = new File(fn);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tmpStr = null;

			String msgid = null;
			String msgfrom = null;
			String msgto = null;
			String msgtime = null;
			String mt = null;
			while ((tmpStr = reader.readLine()) != null) {
				String[] arrstr = tmpStr.split("\t");
				if (arrstr != null && arrstr.length >= 5) {
					msgid = arrstr[0];
					msgfrom = arrstr[1];
					msgto = arrstr[2];
					msgtime = arrstr[3];
					mt = arrstr[4];

					if (true) {
						msgfrom = "x";
					}
					if (true) {
						msgto = "y";
					}

					if (mt != null) {
						Matcher m_emts = p_emts.matcher(mt);
						if (m_emts.find()) {
							mt = m_emts.group(1) + m_emts.group(2)
									+ m_emts.group(3);
						}

						Matcher m_pic = p_pic.matcher(mt);
						if (m_pic.find()) {
							mt = m_pic.group(1) + m_pic.group(2)
									+ m_pic.group(3);
						}

						Matcher m_popo = p_popo.matcher(mt);
						if (m_popo.find()) {
							mt = m_popo.group(1) + m_popo.group(2);
						}

						Matcher m_qq = p_qq.matcher(mt);
						if (m_qq.find()) {
							mt = m_qq.group(1) + m_qq.group(2);
						}

					}
					// String enStr = msgid + "\t" + msgfrom + "\t" + msgto +
					// "\t"
					// + msgtime + "\t" + mt;
					String enStr = msgfrom + "\t" + msgto + "\t" + mt;
					System.out.println(enStr);
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

	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	private static void getOnlineUsers() throws Exception {
		openConn();

		SimpleDateFormat qdfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
		// Date endd = new Date(112,7,28,20,0,0);
		Date endd = new Date();
		Date startd = new Date(endd.getTime() - 60 * 60 * 1000);
		String startstr = qdfs.format(startd);
		String endstr = qdfs.format(endd);

		String es = sqlactionstat.replace("starttime", startstr).replace(
				"endtime", endstr);
		PreparedStatement st = conn.prepareStatement(es);

		ResultSet rs = st.executeQuery();

		ArrayList uidLists = new ArrayList();
		Hashtable tmpUserMap = new Hashtable();
		while (rs.next()) {
			String uid = rs.getString(1);
			String optime = rs.getString(2);
			tmpUserMap.put(uid, optime);
			uidLists.add(uid);
		}
		if (uidLists.size() > 0) {

			String uids = "'";
			for (Iterator iterator = uidLists.iterator(); iterator.hasNext();) {
				String u = (String) iterator.next();
				uids += u + "','";
			}
			uids = uids.substring(0, uids.length() - 2);

			Statement st_uinfo = conn.createStatement();
			String exec_sqluinfo = sqluinfosex.replace("xxxx", uids);
			ResultSet rs_uinfo = st_uinfo.executeQuery(exec_sqluinfo);

			FileOutputStream fos = new FileOutputStream(ufile, false);
			OutputStreamWriter output = new OutputStreamWriter(fos);

			while (rs_uinfo.next()) {
				String uinfo_uid = rs_uinfo.getString(1);
				String uinfo_nick = rs_uinfo.getString(2);
				Date uinfo_birthday = rs_uinfo.getDate(3);
				String uinfo_city = rs_uinfo.getString(4);
				if (null != uinfo_nick && !"".equals(uinfo_nick)) {
					String oline = uinfo_uid + "\t" + uinfo_nick + "\t"
							+ uinfo_birthday + "\t" + uinfo_city + "\t"
							+ tmpUserMap.get(uinfo_uid);
					System.out.println(oline);

					output.write(oline + "\n");
				}

			}
			output.close();

		}

		rs.close();
		st.close();

		closeConn();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void getTMsgs() throws Exception {
		openConn();

		getToMaps();

		initDir(basefd);

		// getMsgOne2One();
		Iterator iter = toMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String uid = (String) entry.getKey();
			String uname = (String) entry.getValue();

			toid = uid.trim();
			toname = uname.trim();
			System.out.println("==========get msg from :" + uid + "," + uname);

			// fromdt = getFromOptime(toid);
			String strfrom = sqltmsg.replace("xxxx", fromdt);
			PreparedStatement ps_tmsg = conn.prepareStatement(strfrom);
			ps_tmsg.setString(1, toid);
			System.out.println("exec stat:" + strfrom + ";para:" + toid);

			ArrayList<Entity> enties = new ArrayList<Entity>();
			query(enties, ps_tmsg);

			if (enties.size() > 4) {
				System.out.println("count:" + enties.size());
				Collections.sort(enties);
				String fn = basefd + toid + fromdt + "tmsg.txt";

				FileOutputStream fos = new FileOutputStream(fn, true);
				OutputStreamWriter output = new OutputStreamWriter(fos, "UTF8");
				for (Iterator<Entity> iterator = enties.iterator(); iterator
						.hasNext();) {

					Entity entity = (Entity) iterator.next();
					// output.write(JhhBaseCrypt.jiacrypt("\n--------\n"));
					output.write(entity.toString() + "\n");

				}
				output.close();
			}
			ps_tmsg.close();
		}

		closeConn();
	}

	@SuppressWarnings("rawtypes")
	private static void getMsgToFile() throws Exception {
		openConn();

		getToMaps();

		initDir(basefd);

		// getMsgOne2One();
		Iterator iter = toMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String uid = (String) entry.getKey();
			String uname = (String) entry.getValue();

			toid = uid.trim();
			toname = uname.trim();
			System.out.println("==========get msg from :" + uid + "," + uname);

			// fromdt = getFromOptime(toid);

			JhhPpMsg.getMsgOneWithAll();

		}

		closeConn();
	}

	public static void initHash() throws Exception {
		FileReader uf = new FileReader("D:/jhhWorks/jhh/ziliao/ju.txt");
		BufferedReader br = new BufferedReader(uf);
		String line = null;
		String en = null;
		String ch = null;
		while ((line = br.readLine()) != null) {
			en = line.split("\t")[0];
			ch = new String(line.split("\t")[1].getBytes(), "UTF8");

			userMap.put(en, ch);
		}
	}

	public static void getToMaps() throws Exception {
		if (tos.length > 0) {
			String tmpStr = null;
			String uid = null;
			String uname = null;
			for (int i = 0; i < tos.length; i++) {
				tmpStr = tos[i].trim();
				String[] arrstr = tmpStr.split("[\t, ]");
				if (arrstr != null && arrstr.length >= 2) {
					uid = arrstr[0];
					uname = arrstr[1];
					if (toMap.get(uid) == null) {
						toMap.put(uid, uname);
					}
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

	private static void readMsgFromFile() throws Exception {
		for (int i = 0; i < tos.length; i++) {
			toid = tos[i].trim();

			String desd = basefd + toid + "__mi/";
			File desdir = new File(desd);
			if (desdir.exists() == false) {
				desdir.mkdir();
			}
			BufferedReader rf = null;
			String tls = null;
			String file = basefd + toid + "/";
			File fdf = new File(file);
			for (File f : fdf.listFiles()) {
				if (f.isFile()) {
					String fn = f.getName();
					System.out.println("read msg :" + fn);

					FileOutputStream fos = new FileOutputStream(desd + fn, true);
					OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF8");

					rf = new BufferedReader(new FileReader(f));
					while ((tls = rf.readLine()) != null) {
						String ming = JhhCrypt.jhhencode(tls);
						osw.write(ming + "\n");
					}
					rf.close();
					osw.close();
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
		conn = DriverManager.getConnection(url, "popomirror", "popomirror");

	}

	public static void closeConn() throws Exception {
		conn.close();
	}

	public static void getMsgOneWithAll() throws Exception {

		String strfrom = sqlfrom.replace("xxxx", fromdt);
		PreparedStatement stfrom = conn.prepareStatement(strfrom);
		stfrom.setString(1, toid);
		System.out.println("exec stat:" + strfrom + ";para:" + toid);
		ResultSet rs = stfrom.executeQuery();

		while (rs.next()) {
			String ufrom = rs.getString(1);
			if (!toid.equalsIgnoreCase(ufrom)) {
				String tn = toname.replace("|", FG).replace("/", FG);
				String d = basefd + toid + FG + tn + FG + fromdt + "/";
				getMsgFromDDB(ufrom, toid, d);
			}
		}
		rs.close();
		stfrom.close();

	}

	private static String getUserNickName(String uinfo_uid) throws Exception {
		PreparedStatement ps_uinfo = conn.prepareStatement(sqluinfo);
		ps_uinfo.setString(1, uinfo_uid);
		ResultSet rs_uinfo = ps_uinfo.executeQuery();
		String uinfo_nickname = "xxxx";
		while (rs_uinfo.next()) {
			// uid,actionid,comment,optime
			uinfo_nickname = rs_uinfo.getString(2);
		}

		return uinfo_nickname;
	}

	private static String getFromOptime(String uinfo_uid) throws Exception {
		PreparedStatement ps_uaction = conn.prepareStatement(sqluaction);
		ps_uaction.setString(1, uinfo_uid);
		ResultSet rs_uaction = ps_uaction.executeQuery();
		String uaction_optime = "2012-12-31";
		while (rs_uaction.next()) {
			// uid,actionid,comment,optime
			uaction_optime = rs_uaction.getString(4);
		}
		if (uaction_optime.length() >= 10) {

			uaction_optime = uaction_optime.substring(0, 10);
			Date d = df.parse(uaction_optime);
			Date bd = new Date(d.getTime() - 15 * macrosecsinday);
			uaction_optime = df.format(bd);
		}

		return uaction_optime;
	}

	public static void getMsgOne2One() throws Exception {
		openConn();
		getMsgFromDDB(from, toid, basefd);

	}

	@SuppressWarnings("unchecked")
	private static void getMsgFromDDB(String ufrom, String to, String fd)
			throws Exception {

		System.out.println("========" + ufrom + "\t" + to + "\t" + fd);

		ArrayList<Entity> enties = new ArrayList<Entity>();
		String strmsg = sqlmsg.replace("xxxx", fromdt);
		PreparedStatement st1 = conn.prepareStatement(strmsg);
		st1.setString(1, ufrom);
		st1.setString(2, to);

		PreparedStatement st2 = conn.prepareStatement(strmsg);
		st2.setString(1, to);
		st2.setString(2, ufrom);

		System.out.println("exec stat:" + strmsg + ";para:" + ufrom + "," + to);
		query(enties, st1);
		query(enties, st2);

		if (enties.size() > 4) {
			System.out.println("count:" + enties.size());
			initDir(fd);
			Collections.sort(enties);

			String fn = fd + ufrom + "2" + to + ".txt";

			FileOutputStream fos = new FileOutputStream(fn, true);
			OutputStreamWriter output = new OutputStreamWriter(fos, "UTF8");
			for (Iterator<Entity> iterator = enties.iterator(); iterator
					.hasNext();) {

				Entity entity = (Entity) iterator.next();
				// output.write(JhhBaseCrypt.jiacrypt("\n--------\n"));
				output.write(entity.toString() + "\n");

			}
			output.close();
		}
	}

	public static String getFullName(String uid) throws Exception {

		String nickname = userMap.get(uid);
		if (nickname == null) {
			nickname = toMap.get(uid);
			if (nickname == null) {
				nickname = getUserNickName(uid);
			}
		}
		return "[" + uid + "," + nickname + "]";

	}

	private static void query(ArrayList<Entity> entities, PreparedStatement st)
			throws SQLException {
		ResultSet rs = st.executeQuery();

		int id = 0;

		while (rs.next()) {
			id++;
			LNotify ln = new LNotify();
			String from = rs.getString(3);
			String to = rs.getString(4);
			ln.uuid = rs.getString(1);
			ln.nid = rs.getInt(2);
			ln.from = from;
			ln.to = to;
			Blob blob = rs.getBlob(5);
			ln.notify = blob.getBytes(1, (int) blob.length());
			ln.addtime = rs.getString(6);

			Unpack up = new Unpack(ln.notify);
			LinkFrame lf = new LinkFrame();
			lf.unmarshal(up);
			if (lf.commandId != 3 && lf.commandId != 7)
				continue;

			try {
				StrProperty prop = new StrProperty();
				prop.unmarshal(up);

				String time = prop.get(ISessionService.TagId.TAG_TIME);
				String text = prop.get(ISessionService.TagId.TAG_MSGBODY);

				String fromstr = getFullName(from);
				String tostr = getFullName(to);

				Entity e = new Entity(id, fromstr, tostr, time, text);
				entities.add(e);

			} catch (Exception ex) {
				continue;
			}
		}
		rs.close();
		st.close();
	}
}

@SuppressWarnings("rawtypes")
class Entity implements Comparable {
	int id;
	String from;
	String to;
	// String fromname;
	// String toname;
	String time;
	String text;

	public Entity(int id, String from, String to, String time, String text) {
		super();
		this.id = id;
		this.from = from;
		this.to = to;
		this.time = time;
		this.text = text;
	}

	public String toString() {

		return ("id=" + id + "\t from=" + from + "\t to=" + to + "\t time="
				+ time + "\t text=" + text);

		// return JhhCrypt.jhhencode("id=" + id + "\t from=" + from + "\t to="
		// + to + "\t time=" + time + "\t text=" + text);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return this.time.compareTo(((Entity) o).time);
	}
}