package com.test.latex;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class TestJava implements Serializable {
	static String header = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">  <html lang=\"zh\"> <head>  <meta http-equiv=\"Content-Type\" content=\"text/html\" charset=\"utf-8\"> <title>javadoc</title> <link rel=\"stylesheet\" type=\"text/css\" href=\"./stylesheet.css\" title=\"Style\"> </head>  <body>    ";
	static String dir = "D:/temp/jhh/";

	public static void main(String[] args) throws Exception {

		// String[] test_frac = { "frac", "\\frac {V_m} {K_M+S}" };
		// String[] test_sum = { "sum", "\\sum_{k=1}^{n}\\frac{1}{k}" };
		// String[] test_fun = { "fun",
		// "f(x)=a_{n}x^{n}+a_{n-1}x^{n-1}+...+a_2x^2+a_1x+a_0\\quad\\quad\\quad\\quad(a_n\\ne0)"
		// };
//		String[] test_matrix = { "matrix",
//				"\\begin{bmatrix}1 & 2\\\\3 &4\\end{bmatrix}" };
		testLatex();
		System.out.println("====success====");
	}

	@SuppressWarnings("rawtypes")
	private static void testLatex() throws Exception {
		Properties pro = new Properties();

		InputStream inStr = new FileInputStream("src/com/test/latex/forluma.properties");
		pro.load(inStr);

		Iterator it = pro.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			// System.out.println(key + ":" + value);
			gen(key, value);
		}
	}

	protected static void gen(String name, String func) throws Exception {

		String getURL = "http://chart.apis.google.com/chart?cht=tx&chl="
				+ URLEncoder.encode(func, "utf-8");

		URL getUrl = new URL(getURL);
		HttpURLConnection connection = (HttpURLConnection) getUrl
				.openConnection();
		connection.connect();
		InputStream fis = connection.getInputStream();

		FileOutputStream fos = new FileOutputStream(dir + name + ".png");
		byte[] buf = new byte[1024];

		while (fis.read(buf) != -1) {
			fos.write(buf);
		}

		fis.close();
		fos.close();

		connection.disconnect();
	}
}
