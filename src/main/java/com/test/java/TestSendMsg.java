package com.test.java;

import java.io.IOException;

import sun.net.www.http.HttpClient;

public class TestSendMsg {

	/**
	 * 发送单个短信
	 */
	public String sendMessage() {
		try {
			StringBuffer bufxml = new StringBuffer();
			bufxml.append("<?xml version=");
			bufxml.append("\"1.0\"");
			bufxml.append(" encoding=");
			bufxml.append("\"UTF-8\"");
			bufxml.append("?><xmlRequest><header><syscode>");
			bufxml.append("业务编码");
			bufxml.append("</syscode><depcode>");
			bufxml.append("00000000");
			bufxml.append("</depcode><depname>安华农业保险股份有限公司</depname><password>密码</password></header><body><sms><mobile>");
			bufxml.append("手机号");
			bufxml.append("</mobile><content>");
			bufxml.append("短信内容");
			bufxml.append("</content></sms></body></xmlRequest>");

			PostMethod post = new PostMethod(
					"http://10.0.7.40:8080/smsHttpServlet.servlet");// 请求地址
			// 这里添加xml字符串
			post.setRequestBody(bufxml.toString());

			// 指定请求内容的类型
			post.setRequestHeader("Content-type", "text/xml; charset=UTF-8");
			HttpClient httpclient = new HttpClient();// 创建 HttpClient 的实例
			int result;
			try {
				result = httpclient.executeMethod(post);
				System.out.println("Response status code: " + result);// 返回200为成功
				System.out.println("Response body: ");
				System.out.println(post.getResponseBodyAsString());// 返回的内容
				String responseXml = post.getResponseBodyAsString();
				if (!"".equals(responseXml) && responseXml != null) {
					if (responseXml.contains("000")) {
						system.out.println("发送成功");

					} else if (responseXml.contains("001")) {
						system.out.println("发送失败");
					}
				}
				post.releaseConnection();// 释放连接
			} catch (HttpException e) {
				system.out.println("发送失败");

				e.printStackTrace();
			} catch (IOException e) {
				system.out.println("发送失败");

				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
			this.message = this.getFailMessage("发送失败");
		}
		return "json";
	}
}
