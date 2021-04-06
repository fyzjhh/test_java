package com.test.netease.other;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JhhPpTinfo {
	static long macrosecsinday = 24 * 3600 * 1000;
	static String[] alltids = {};
	static HashMap<String, String> tidMap = new HashMap<String, String>();
	static String condkeywords = "(tname regexp '.*') and (tname regexp '�ڻ�')";
	static String basefd = "D:/temp/jpm/";
	static String from = "@corp.netease.com";
	static String to = "@corp.netease.com";
	static String fromdt = "2012-07-20";
	static Connection conn = null;
	static String url = "192.168.164.162:8888?key=D:/NetEase/ddb/ddb3/conf/secret.key";
	static String sqltinfo = "select tid,tname,owner,createtime,free from tinfo where xxxx and validflag=1 and clienttype=0";
	static String sqltlist = "select tid,uid,nick,sex,phone,email,address from tlist where tid=? and validflag=1 and clienttype=0";
	// static String sqluinfo =
	// "select uid,nick,birthday,city from uinfo where uid in (xxxx) and sex=2";
	static String sqluinfo = "select uid,nick,birthday,city from uinfo where uid in (xxxx)";
	static String sqluaction = "select uid,actionid,comment,optime from uactionstat where uid=? and actionid='login' order by optime desc limit 1";
	static OutputStreamWriter uoutf;

	public static void main(String[] args) throws Exception {
		// getQQUser();
		openConn();

		getTidMap();

		JhhPpTinfo.getUinfos();

		closeConn();

	}

	@SuppressWarnings("rawtypes")
	public static void getUinfos() throws Exception {

		initDir(basefd);
		String udir = basefd + "qihuo_uinfos.txt";
		uoutf = new OutputStreamWriter(new FileOutputStream(udir));

		Iterator iter = tidMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String tid = (String) entry.getKey();
			String tname = (String) entry.getValue();

			String s = "[tid " + tid + ",tname " + tname + "]";
			System.out.println("==tinfo== " + s);
			uoutf.write("\n\n==tinfo== " + s + "\n");

			getTUserInfo(tid);
			uoutf.flush();
		}

		uoutf.close();
	}

	public static void getQQUser() throws Exception {
		FileReader uf = new FileReader(basefd + "alluinfos.txt");
		BufferedReader br = new BufferedReader(uf);

		String line = null;
		ArrayList<String> qqUsers = new ArrayList<String>();
		while ((line = br.readLine()) != null) {
			if (line.matches(".*@qq.com.*")) {
				qqUsers.add(line);
			}
		}
		Collections.sort(qqUsers);
		FileOutputStream qqfos = new FileOutputStream(basefd + "allqqusers.txt");
		OutputStreamWriter qqosw = new OutputStreamWriter(qqfos);
		for (Iterator<String> qqit = qqUsers.iterator(); qqit.hasNext();) {
			String str = (String) qqit.next();
			qqosw.write(str + "\n");
		}

		qqosw.close();
	}

	private static void getTidMap() throws Exception {
		if (alltids.length > 1) {
			String[] tids = alltids;
			for (int i = 0; i < tids.length; i++) {
				String[] en = tids[i].split("\t");
				tidMap.put(en[0], en[1]);
			}
		} else {

			String tmpdir = basefd + "qihuo.txt";
			FileReader uf = new FileReader(tmpdir);
			BufferedReader br = new BufferedReader(uf);
			String line = null;
			String tmptid = null;
			String tmptname = null;
			while ((line = br.readLine()) != null) {
				tmptid = line.split("\t")[0];
				tmptname = line.split("\t")[1];
				if (tmptname != null && tmptname.matches(".*�ڻ�.*"))
				tidMap.put(tmptid, tmptname);
			}

			if (tidMap.size() > 0) {
				return;
			}
			Statement st_tinfo = conn.createStatement();
			String exesqltinfo = sqltinfo.replace("xxxx", condkeywords);
			ResultSet rs_tinfo = st_tinfo.executeQuery(exesqltinfo);

			initDir(basefd);
			String tdir = basefd + "qihuo.txt";
			uoutf = new OutputStreamWriter(new FileOutputStream(tdir));
			HashMap<String, String> tmpTidMap = new HashMap<String, String>();
			while (rs_tinfo.next()) {
				String tid = rs_tinfo.getString(1);
				String tname = rs_tinfo.getString(2);
				tmpTidMap.put(tid, tname);
			}
			Object[] tidarr = tmpTidMap.keySet().toArray();
			Arrays.sort(tidarr);
			for (int i = 0; i < tidarr.length; i++) {
				String tid = String.valueOf(tidarr[i]);
				String tname = tmpTidMap.get(tid);
				uoutf.write(tid + "\t" + tname + "\n");
				tidMap.put(tid, tname);
				// System.out.println(tid + "\t" + tname);
			}
			rs_tinfo.close();
			st_tinfo.close();
			uoutf.close();
		}

	}

	private static void initDir(String dir) {
		File b = new File(dir);
		if (b.exists() == false) {
			b.mkdir();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void getTUserInfo(String tid) throws Exception {
		PreparedStatement ps_tlist = conn.prepareStatement(sqltlist);
		ps_tlist.setString(1, tid);
		ResultSet rs_tlist = ps_tlist.executeQuery();
		ArrayList uidLists = new ArrayList();
		while (rs_tlist.next()) {
			String tlist_uid = rs_tlist.getString(2);
			uidLists.add(tlist_uid);
		}
		if (uidLists.size() > 2) {

			String uids = "'";
			for (Iterator iterator = uidLists.iterator(); iterator.hasNext();) {
				String u = (String) iterator.next();
				uids += u + "','";
			}
			uids = uids.substring(0, uids.length() - 2);

			Statement st_uinfo = conn.createStatement();
			String exec_sqluinfo = sqluinfo.replace("xxxx", uids);
			ResultSet rs_uinfo = st_uinfo.executeQuery(exec_sqluinfo);
			while (rs_uinfo.next()) {
				String uinfo_uid = rs_uinfo.getString(1);
				String uinfo_nick = rs_uinfo.getString(2);
				Date uinfo_birthday = rs_uinfo.getDate(3);
				String uinfo_city = rs_uinfo.getString(4);
				// uid,nick,birthday,city

				String uaction_optime = getLastOpTime(uinfo_uid);
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				uaction_optime = uaction_optime.substring(0, 10);
				Date d = df.parse(uaction_optime);
				Date todayd = new Date();
				if (todayd.getTime() - d.getTime() <= 60 * macrosecsinday) {

					System.out.println(uinfo_uid + "\t" + uinfo_nick + "\t"
							+ uinfo_birthday + "\t" + uinfo_city + "\t"
							+ uaction_optime);
					uoutf.write(uinfo_uid + "\t" + uinfo_nick + "\t"
							+ uinfo_birthday + "\t" + uinfo_city + "\t"
							+ uaction_optime + "\n");

				}

			}

		}
	}

	private static String getLastOpTime(String uinfo_uid) throws Exception {
		PreparedStatement ps_uaction = conn.prepareStatement(sqluaction);
		ps_uaction.setString(1, uinfo_uid);
		ResultSet rs_uaction = ps_uaction.executeQuery();
		String uaction_optime = "2000-12-31";
		while (rs_uaction.next()) {
			// uid,actionid,comment,optime
			uaction_optime = rs_uaction.getString(4);
		}

		if (uaction_optime.length() >= 10) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			uaction_optime = uaction_optime.substring(0, 10);
			Date d = df.parse(uaction_optime);
			uaction_optime = df.format(d);
		}
		return uaction_optime;
	}

	public static void openConn() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		conn = DriverManager.getConnection(url, "popomirror", "popomirror");
	}

	public static void closeConn() throws Exception {
		conn.close();
	}

}
