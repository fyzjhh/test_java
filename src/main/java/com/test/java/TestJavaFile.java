package com.test.java;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;

public class TestJavaFile {

	public static void main(String[] args) throws Exception {

		read_file_char("D:/temp/userid_diaoxian.csv");

		System.out.println();
		System.out.println("====success====");
	}

	// 按照字节读写
	public static void read_write_byte(String filein, String fileout)
			throws Exception {

		FileInputStream fis = new FileInputStream(filein);
		FileOutputStream fos = new FileOutputStream(fileout);
		byte[] b = new byte[1024];
		BufferedInputStream bis = new BufferedInputStream(fis);
		BufferedOutputStream hos = new BufferedOutputStream(fos);

		while (bis.read(b) != -1) {
			hos.write(b);
		}
		hos.close();
		bis.close();
	}

	// 字节流转成字符流
	public static void byte_to_char(String filename) throws Exception {
		File file = new File(filename);
		if (file.exists() == false) {
			return;
		}
		FileInputStream fis = new FileInputStream(file);
		InputStreamReader isr = new InputStreamReader(fis, "UTF8");
		BufferedReader br = new BufferedReader(isr);

		String line_str = null;
		while ((line_str = br.readLine()) != null) {
			System.out.println(line_str);
		}
		br.close();
	}

	// 字节流读取文件
	public static void read_file_byte(String filename) throws Exception {
		File file = new File(filename);
		if (file.exists() == false) {
			return;
		}
		InputStream in = new FileInputStream(file);
		BufferedInputStream bin = new BufferedInputStream(in);
		int b;
		while ((b = bin.read()) != -1) {
			System.out.print((byte) b);
		}

		bin.close();
	}

	// 字节流读取文件
	public static void read_file_nbyte(String filename) throws Exception {
		File file = new File(filename);
		if (file.exists() == false) {
			return;
		}
		InputStream in = new FileInputStream(file);

		BufferedInputStream bin = new BufferedInputStream(in);
		int b;

		while ((b = bin.read()) != -1) {
			System.out.print((byte) b);
		}

		bin.close();
	}

	// 字符流读取文件
	public static void read_file_char(String filename) throws Exception {

		File file = new File(filename);
		if (file.exists() == false) {
			return;
		}
		FileReader fr = new FileReader(file);
		fr.skip(0);
		BufferedReader br = new BufferedReader(fr);

		String line_str = null;
		while ((line_str = br.readLine()) != null) {
			System.out.println(line_str);
		}
		br.close();
	}

	// 字节流写入文件
	public static void write_file_byte(String filename, boolean append)
			throws Exception {
		File file = new File(filename);
		if (file.exists() == false) {
			file.createNewFile();
		}

		OutputStream os = new FileOutputStream(file, append);

		os.write("xxx".getBytes());
		os.flush();

		os.close();
	}

	// 字符流写入文件
	public static void write_file_char(String filename, boolean append)
			throws Exception {
		File file = new File(filename);
		if (file.exists() == false) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file, append);
		BufferedWriter bw = new BufferedWriter(fw);

		bw.write("x:\n");
		bw.flush();

		bw.close();
	}

	/**
	 * 显示输入流中还剩的字节数
	 * 
	 * @param in
	 */
	public static void showAvailableBytes(InputStream in) {
		try {
			System.out.println("当前字节输入流中的字节数为:" + in.available());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public static void printDirectory(File f) {
		if (!f.isDirectory()) {
			System.out.println("FFFF    " + f.getAbsolutePath());
		} else {
			File[] fs = f.listFiles();
			System.out.println("DDDD    " + f.getAbsolutePath());

			for (int i = 0; i < fs.length; ++i) {
				File file = fs[i];
				printDirectory(file);
			}
		}
	}

	public static void test_dataoututstream() throws Exception {

		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		System.out.println("os.name is " + osName);

		String filename = "/tmp/1.bin";
		if (osName.matches(".*Linux.*")) {
			filename = "/user/data/tmp/linux_1.bin";
		}
		if (osName.matches(".*Windows.*")) {
			filename = "D:/temp/win_1.bin";
		}
		System.out.println("filename is " + filename);

		FileOutputStream fos = new FileOutputStream(filename);
		DataOutputStream out = new DataOutputStream(fos);

		// out.writeLong(1048576L);
		// out.writeInt(1048576);
		// out.writeChar(' ');
		// out.writeByte(16);

		out.writeLong(-1L);
		out.writeInt(-8);
		out.writeChar(' ');
		out.writeByte(-64);

		out.flush();
		out.close();

	}

	public static void test_datainputstream() throws Exception {

		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		System.out.println("os.name is " + osName);

		String filename = "/tmp/1.bin";
		if (osName.matches(".*Linux.*")) {
			filename = "/user/data/tmp/linux_1.bin";
		}
		if (osName.matches(".*Windows.*")) {
			filename = "D:/temp/win_1.bin";
		}
		System.out.println("filename is " + filename);

		// read it in again
		DataInputStream in = new DataInputStream(new FileInputStream(filename));

		try {
			long long1 = in.readLong();
			int int1 = in.readInt();
			char char1 = in.readChar();
			byte byte1 = in.readByte();

			System.out.println("long1 is " + long1);
			System.out.println("int1 is " + int1);
			System.out.println("char1 is " + char1);
			System.out.println("byte1 is " + byte1);
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void write_file_char() throws Exception {

		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		System.out.println("os.name is " + osName);

		String filename = "/tmp/1.bin";
		if (osName.matches(".*Linux.*")) {
			filename = "/user/data/tmp/linux_1.bin";
		}
		if (osName.matches(".*Windows.*")) {
			filename = "D:/temp/win_1.bin";
		}
		System.out.println("filename is " + filename);
		File file = new File(filename);
		if (file.exists() == false) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file, false);
		BufferedWriter bw = new BufferedWriter(fw);
		char c;
		/*
		 * 这里的数值是unicode的编码序号 写入文件是按照utf8的编码
		 */
		for (int i = 35840; i < 35843; i++) {
			bw.write(" " + String.valueOf(i) + " ");
			bw.write(i);
			c = (char) i;
			System.out.println(c + " ");
		}

		bw.flush();

		bw.close();
	}

	public static void write_char() throws Exception {

		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		System.out.println("os.name is " + osName);

		String filename = "/tmp/1.bin";
		if (osName.matches(".*Linux.*")) {
			filename = "/user/data/tmp/linux_1.bin";
		}
		if (osName.matches(".*Windows.*")) {
			filename = "D:/temp/gbk.bin";
		}
		System.out.println("filename is " + filename);
		File file = new File(filename);
		if (file.exists() == false) {
			file.createNewFile();
		}
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(
				filename), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);

		for (int i = 35840; i < 35843; i++) {
			bw.write(String.valueOf(i) + " ");
			bw.write(i);
		}

		bw.flush();

		bw.close();
	}
}
