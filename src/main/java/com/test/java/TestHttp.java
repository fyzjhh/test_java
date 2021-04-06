package com.test.http;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Set;

public class TestHttp implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3014716914084285225L;
	private static final int MIN_INDEX = 19968;
	private static final int MAX_INDEX = 40869;
	private static final String CR = "\r\n";
	private static final String TAB = "\t";

	public static void main(String[] args) throws Exception {
		
		String hs = "http://weibo.com/u/1667368437/home?wvr=5";
		String en_hs = URLEncoder.encode(hs, "utf8");
		String de_hs = URLDecoder.decode(en_hs, "utf8");
		System.out.println(hs);
		System.out.println(en_hs);
		System.out.println(de_hs);
		
//		int[] uids = {2060926175,1912959280};
//		for (int i = 0; i < uids.length; i++) {
//
//			get_weibo_friendships_friends(String.valueOf(uids[i]));
//		}

		System.out.println("====success====");
	}

	public static final String GET_URL = "http://www.jiucool.com/request.php?key=j0r56u2";

	public static final String POST_URL = "http://www.jiucool.com/request.php";

	public static void readContentFromGet() throws IOException {
		// 拼凑get请求的URL字串，使用URLEncoder.encode对特殊和不可见字符进行编码
		String getURL = "https://www.google.com.hk/";
		URL getUrl = new URL(getURL);
		// 根据拼凑的URL，打开连接，URL.openConnection函数会根据URL的类型，
		// 返回不同的URLConnection子类的对象，这里URL是一个http，因此实际返回的是HttpURLConnection
		HttpURLConnection connection = (HttpURLConnection) getUrl
				.openConnection();
		// 进行连接，但是实际上get request要在下一句的connection.getInputStream()函数中才会真正发到
		// 服务器
		connection.connect();
		// 取得输入流，并使用Reader读取
		// BufferedReader reader = new BufferedReader(new InputStreamReader(
		// connection.getInputStream(), "utf-8"));// 设置编码,否则中文乱码
		// System.out.println("=============================");
		// System.out.println("Contents of get request");
		// System.out.println("=============================");
		// String lines;
		// while ((lines = reader.readLine()) != null) {
		// // lines = new String(lines.getBytes(), "utf-8");
		// System.out.println(lines);
		// }
		// reader.close();
		System.out.println("=============================");
		System.out.println("Contents of response header");
		System.out.println("=============================");
		Map headers = connection.getHeaderFields();
		Set<String> keys = headers.keySet();
		for (String key : keys) {
			String val = connection.getHeaderField(key);
			System.out.println(key + "     " + val);
		}

		System.out.println(connection.getLastModified());

		InputStream is = connection.getInputStream();
		System.out.println("=============================");
		System.out.println("Contents of get request");
		System.out.println("=============================");
		int n = 1;
		byte buffer[] = new byte[n];
		// 读取输入流
		while ((is.read(buffer, 0, n) != -1) && (n > 0)) {
			System.out.print(new String(buffer));
		}
		// 断开连接
		connection.disconnect();
		System.out.println("=============================");
		System.out.println("Contents of get request ends");
		System.out.println("=============================");
	}

	public static void readContentFromPost() throws IOException {
		// Post请求的url，与get不同的是不需要带参数
		URL postUrl = new URL(POST_URL);
		// 打开连接
		HttpURLConnection connection = (HttpURLConnection) postUrl
				.openConnection();
		// Output to the connection. Default is
		// false, set to true because post
		// method must write something to the
		// connection
		// 设置是否向connection输出，因为这个是post请求，参数要放在
		// http正文内，因此需要设为true
		connection.setDoOutput(true);
		// Read from the connection. Default is true.
		connection.setDoInput(true);
		// Set the post method. Default is GET
		connection.setRequestMethod("POST");
		// Post cannot use caches
		// Post 请求不能使用缓存
		connection.setUseCaches(false);
		// This method takes effects to
		// every instances of this class.
		// URLConnection.setFollowRedirects是static函数，作用于所有的URLConnection对象。
		// connection.setFollowRedirects(true);

		// This methods only
		// takes effacts to this
		// instance.
		// URLConnection.setInstanceFollowRedirects是成员函数，仅作用于当前函数
		connection.setInstanceFollowRedirects(true);
		// Set the content type to urlencoded,
		// because we will write
		// some URL-encoded content to the
		// connection. Settings above must be set before connect!
		// 配置本次连接的Content-type，配置为application/x-www-form-urlencoded的
		// 意思是正文是urlencoded编码过的form参数，下面我们可以看到我们对正文内容使用URLEncoder.encode
		// 进行编码
		connection.setRequestProperty("Content-Type",
				"application/x-www-form-urlencoded");
		// 连接，从postUrl.openConnection()至此的配置必须要在connect之前完成，
		// 要注意的是connection.getOutputStream会隐含的进行connect。
		connection.connect();
		DataOutputStream out = new DataOutputStream(
				connection.getOutputStream());
		// The URL-encoded contend
		// 正文，正文内容其实跟get的URL中'?'后的参数字符串一致
		String content = "key=j0r53nmbbd78x7m1pqml06u2&type=1&toemail=jiucool@gmail.com"
				+ "&activatecode=" + URLEncoder.encode("久酷博客", "utf-8");
		// DataOutputStream.writeBytes将字符串中的16位的unicode字符以8位的字符形式写道流里面
		out.writeBytes(content);
		out.flush();
		out.close(); // flush and close
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				connection.getInputStream(), "utf-8"));// 设置编码,否则中文乱码
		String line = "";
		System.out.println("=============================");
		System.out.println("Contents of post request");
		System.out.println("=============================");
		while ((line = reader.readLine()) != null) {
			// line = new String(line.getBytes(), "utf-8");
			System.out.println(line);
		}
		System.out.println("=============================");
		System.out.println("Contents of post request ends");
		System.out.println("=============================");
		reader.close();
		connection.disconnect();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void get_weibo_friendships_friends(String uid) throws IOException {
		// 拼凑get请求的URL字串，使用URLEncoder.encode对特殊和不可见字符进行编码
		String getURL = "https://api.weibo.com/2/friendships/friends.json?access_token=2.00_hGqoB0ns_uJ699acae82c07QrXS&uid=${uid}";
		getURL = getURL.replaceAll("\\$\\{uid\\}", uid);
		URL getUrl = new URL(getURL);
		HttpURLConnection connection = (HttpURLConnection) getUrl
				.openConnection();
		connection.connect();

		System.out.println("=============================");
		System.out.println("Contents of response header");
		System.out.println("=============================");
		Map headers = connection.getHeaderFields();
		Set<String> keys = headers.keySet();
		for (String key : keys) {
			String val = connection.getHeaderField(key);
			System.out.println(key + "     " + val);
		}
		System.out.println(connection.getLastModified());

		// 取得输入流，并使用Reader读取
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				connection.getInputStream(), "utf-8"));// 设置编码,否则中文乱码
		System.out.println("=============================");
		System.out.println("Contents of get request content");
		System.out.println("=============================");
		String line;
		String retStr = "";
		while ((line = reader.readLine()) != null) {
			retStr += line + "\r\n";
		}
		reader.close();
		System.out.println(retStr);

		// 断开连接
		connection.disconnect();
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void get_qq_user_info(String uid) throws IOException {
		// 拼凑get请求的URL字串，使用URLEncoder.encode对特殊和不可见字符进行编码
		String getURL = "http://openapi.tencentyun.com/v3/user/get_info?openid=&openkey&appid=1103682408&sig&pfformat=json&userip=112.90.139.30";
		getURL = getURL.replaceAll("\\$\\{uid\\}", uid);
		URL getUrl = new URL(getURL);
		HttpURLConnection connection = (HttpURLConnection) getUrl
				.openConnection();
		connection.connect();

		System.out.println("=============================");
		System.out.println("Contents of response header");
		System.out.println("=============================");
		Map headers = connection.getHeaderFields();
		Set<String> keys = headers.keySet();
		for (String key : keys) {
			String val = connection.getHeaderField(key);
			System.out.println(key + "     " + val);
		}
		System.out.println(connection.getLastModified());

		// 取得输入流，并使用Reader读取
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				connection.getInputStream(), "utf-8"));// 设置编码,否则中文乱码
		System.out.println("=============================");
		System.out.println("Contents of get request content");
		System.out.println("=============================");
		String line;
		String retStr = "";
		while ((line = reader.readLine()) != null) {
			retStr += line + "\r\n";
		}
		reader.close();
		System.out.println(retStr);

		// 断开连接
		connection.disconnect();
	}
	public static void testPost() throws IOException {

		/**
		 * 首先要和URL下的URLConnection对话。 URLConnection可以很容易的从URL得到。比如： // Using
		 * java.net.URL and //java.net.URLConnection
		 */
		URL url = new URL("http://www.faircanton.com/message/check.asp");
		URLConnection connection = url.openConnection();
		/**
		 * 然后把连接设为输出模式。URLConnection通常作为输入来使用，比如下载一个Web页。
		 * 通过把URLConnection设为输出，你可以把数据向你个Web页传送。下面是如何做：
		 */
		connection.setDoOutput(true);
		/**
		 * 最后，为了得到OutputStream，简单起见，把它约束在Writer并且放入POST信息中，例如： ...
		 */
		OutputStreamWriter out = new OutputStreamWriter(
				connection.getOutputStream(), "8859_1");
		out.write("username=kevin&password=*********"); // post的关键所在！
		// remember to clean up
		out.flush();
		out.close();
		/**
		 * 这样就可以发送一个看起来象这样的POST： POST /jobsearch/jobsearch.cgi HTTP 1.0 ACCEPT:
		 * text/plain Content-type: application/x-www-form-urlencoded
		 * Content-length: 99 username=bob password=someword
		 */
		// 一旦发送成功，用以下方法就可以得到服务器的回应：
		String sCurrentLine;
		String sTotalString;
		sCurrentLine = "";
		sTotalString = "";
		InputStream l_urlStream;
		l_urlStream = connection.getInputStream();
		// 传说中的三层包装阿！
		BufferedReader l_reader = new BufferedReader(new InputStreamReader(
				l_urlStream));
		while ((sCurrentLine = l_reader.readLine()) != null) {
			sTotalString += sCurrentLine + "\r\n";

		}
		System.out.println(sTotalString);
	}

	private static void test() throws Exception {
		Socket s = new Socket("172.16.1.123", 80);
		InputStream ins = s.getInputStream();
		OutputStream os = s.getOutputStream();
		os.write("GET /a.jsp HTTP/1.1\r\n".getBytes());
		os.write("Host:172.16.1.123\r\n".getBytes());
		// 必须加host，否则服务器会返回bad request,无论是tomcat还是resin，还是nginx或者apache httpd
		os.write("\r\n\r\n".getBytes());// 这个也必不可少，http协议规定的
		os.flush();
		BufferedReader br = new BufferedReader(new InputStreamReader(ins));
		String line = null;
		line = br.readLine();
		while (line != null) {
			System.out.println(line);
			line = br.readLine();
		}
		ins.close();
	}

	/*
	 * 
	 * <% out.print("helloworld!"); out.flush(); out.print("123"); out.flush();
	 * %>
	 * 
	 * HTTP/1.1 200 OK Server: Apache-Coyote/1.1 Set-Cookie:
	 * JSESSIONID=8A7461DDA53B4C4DD0E89D73219CB5F8; Path=/ Content-Type:
	 * text/html;charset=UTF-8 Transfer-Encoding: chunked Date: Wed, 10 Nov 2010
	 * 07:10:05 GMT
	 * 
	 * b helloworld! 3 123 0
	 * 
	 * 其中Transfer-Encoding:chunked表示内容长度在后面介绍，b这个16进制数字就表示长度为11，然后是内容
	 * 
	 * (helloworld!)，然后以\r\n结束，如果有下一段，则一个16进制数字表示长度，直到最后一段没有了，才以0
	 * 
	 * 结束。
	 */
}
