package com.test.dianhun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.ipseek.IPLocation;
import com.ipseek.IPSeeker;

public class Tools_Test extends Tools {

	String teststr = null;
	String[] args = null;

	public void test_long2str() throws Exception {
		long[] aa = { 1429221425, 1094922289, 1395013107 };
		for (int i = 0; i < aa.length; i++) {
			String s = Tools.long2str(aa[i]);
			System.out.println(s + TAB + aa[i]);
		}
	}

	public int hash(List<Object> list) {
		int ret = -1;

		for (Object o : list) {
			if (o instanceof Integer) {
				ret = ret + ((Integer) o);
			}
			if (o instanceof String) {
				ret = ret + o.hashCode();
			}
		}
		return ret;

	}

	public void test_str2long() throws Exception {
		// String[] strs = new String[] { "UM01", "UM09", "UM10", "UM15",
		// "UM27",
		// "UM29", "UM34", "UM41", "UM47", "UM50", "UM59", "UM64", "UM67" };

		String[] strs = new String[] { "U065", "UD80", "UO04", "UY82", "U017",
				"UB72", "UG39", "UI54", "UK56", "UN87", "US64", "UU53", "UZ38", };

		for (int i = 0; i < strs.length; i++) {
			byte[] a = strs[i].getBytes();
			long s = Tools.str2long(a);
			System.out.println(strs[i] + COMMA + s);
		}

	}

	public void test_get_testrs() throws Exception {
		teststr = " -w D:/temp/teststats -a 2013-10-31 -s get_testrs ";
		args = teststr.split(" +");
	}

	public void test_get_ipseek() throws Exception {
		String r = spacedatetimeformat.format(new Date()) + TAB
				+ "test_get_ipseek start";
		System.out.println(r);

		// 指定纯真数据库的文件名，所在文件夹
		IPSeeker ip = new IPSeeker(
				"D:\\dianhun\\jianghehui\\dianhun\\src\\resource\\CoralWry_lite.dat");
		// 测试IP 58.20.43.13
		// String ipstr = "58.20.43.13";
		File fdf = new File("D:/temp/ip.log");
		String ipstr = EMPTY;
		int index = 0;
		BufferedReader rf = new BufferedReader(new FileReader(fdf));
		while ((ipstr = rf.readLine()) != null) {
			index++;
			IPLocation ret = ip.getIPLocation(ipstr);
			System.out.println(index + COMMA + ret.getCountry() + COMMA
					+ ret.getArea());
		}
		rf.close();
		r = spacedatetimeformat.format(new Date()) + TAB
				+ "test_get_ipseek stop";
		System.out.println(r);
	}

	public void test_do_sql() throws Exception {
		do_ddbsql();
	}

	public void test_do_spark() throws Exception {
		do_spark_jdbc();
	}

	public void test_addMonth() throws Exception {
		action_date = "2014-12-30";
		int i = 2;
		System.out.println(addMonth(action_date, i));
	}

	public void test_get_ipInfo() throws Exception {
		String r = spacedatetimeformat.format(new Date()) + TAB
				+ "test_get_ipInfo start";
		System.out.println(r);
		File fdf = new File("D:/temp/ip.log");
		String ipstr = EMPTY;
		int index = 0;
		BufferedReader rf = new BufferedReader(new FileReader(fdf));
		while ((ipstr = rf.readLine()) != null) {
			index++;
			String ret = IPSeeker.getIpInfo(ipstr);
			// String[] rets = ret.split(":")[1].split(COMMA);
			System.out.println(index + COMMA + ret);
			// System.out.println(rets[0] + COMMA + rets[2] + COMMA + rets[3]
			// + COMMA + rets[4]);
		}
		rf.close();
		r = spacedatetimeformat.format(new Date()) + TAB
				+ "test_get_ipInfo stop";
		System.out.println(r);
	}
}
