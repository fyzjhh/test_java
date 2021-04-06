package com.test.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

public class HdsfFileUtil {

	static FileSystem fs;

	@SuppressWarnings("deprecation")
	public static void openHdfs() throws IOException {
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
//		conf.set("fs.default.name", "hdfs://192.168.12.221:9000");
//		fs = FileSystem.get(conf);
		 InetSocketAddress addr = new InetSocketAddress("192.168.12.221",
		 9000);
		 fs = new DistributedFileSystem(addr, conf);
	}

	public static void main(String[] args) throws Exception {

		openHdfs();

		readHdfs("/tmp/t1/file1.txt");
		writeHdfs("/tmp/t1/file2.txt");
	}

	public static void deleteHdfsFile(String path) throws IOException {

		Configuration conf = new Configuration();

		conf.addResource("D:/temp/core-site.xml");

		Path delefPath = new Path(path);
		FileSystem hdfs = delefPath.getFileSystem(conf);

		if (hdfs.exists(delefPath)) {
			hdfs.delete(delefPath, true);
		} else {
			System.out.println("文件不存在：删除失败");
		}
	}

	public static void uploadToHdfs(String local, String hdfs)
			throws IOException {

		Configuration config = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(hdfs), config);

		FileInputStream fis = new FileInputStream(new File(local));
		OutputStream os = fs.create(new Path(hdfs));

		IOUtils.copyBytes(fis, os, 4096, true);

		os.close();
		fis.close();

	}

	public static void readFromHdfs(String fileName, String dest)
			throws IOException {

		Configuration conf = new Configuration();
		conf.addResource("D:/temp/core-site.xml");

		FileSystem fs = FileSystem.get(URI.create(fileName), conf);

		FSDataInputStream hdfsInStream = fs.open(new Path(fileName));

		OutputStream out = new FileOutputStream(dest);

		byte[] ioBuffer = new byte[1024];

		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {

			out.write(ioBuffer, 0, readLen);
			System.out.println(new String(ioBuffer));
			readLen = hdfsInStream.read(ioBuffer);

		}

		out.close();
		hdfsInStream.close();
		fs.close();
	}

	public static void readHdfs(String fileName) throws IOException {

		Path p = new Path(fileName);
		FSDataInputStream fsdis = null;
		if (fs.exists(p)) {
			fsdis = fs.open(p);
		} else {
			return;
		}
		InputStreamReader isr = new InputStreamReader(fsdis);
		BufferedReader br = new BufferedReader(isr);

		String str = null;
		while ((str = br.readLine()) != null) {
			System.out.println(str);
		}

		br.close();
	}

	public static void writeHdfs(String fileName) throws IOException {

		Path p = new Path(fileName);
		FSDataOutputStream dos = null;
		if (fs.exists(p)) {
			dos = fs.append(p);
		} else {
			dos = fs.create(p);
		}
		OutputStreamWriter osw = new OutputStreamWriter(dos);
		BufferedWriter bw = new BufferedWriter(osw);

		bw.write("xxx10\n");
		bw.write("xxx20\n");
		bw.write("xxx30\n");

		bw.flush();
		bw.close();
	}

	public static void getDirectoryFromHdfs(String path) throws IOException {

		Configuration conf = new Configuration();
		conf.addResource("D:/temp/core-site.xml");

		FileSystem fs = FileSystem.get(URI.create(path), conf);

		FileStatus fileList[] = fs.listStatus(new Path(path));

		int size = fileList.length;

		for (int i = 0; i < size; i++) {
			if (fileList[i].isDir() == false) {

				System.out.println("filename:"
						+ fileList[i].getPath().getName() + "\tsize:"
						+ fileList[i].getLen());
			} else {
				String newpath = fileList[i].getPath().toString();
				getDirectoryFromHdfs(newpath);
			}
		}

		fs.close();

	}

}