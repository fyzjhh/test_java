package com.test.java.concurrent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test1Thread implements Runnable {

	public String loginId;
	public String pwd;

	public Test1Thread(String loginId, String pwd) {
		this.pwd = pwd;
		this.loginId = loginId;
	}

	static DateFormat spacedatetimeformat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			String s1 = spacedatetimeformat.format(new Date());
			System.out.println(s1);
			Thread.sleep(5 * 1000);
			String s2 = spacedatetimeformat.format(new Date());
			System.out.println(s2);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.out.println("error--------");
		}

	}

}
