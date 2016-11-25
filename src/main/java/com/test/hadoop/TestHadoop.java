package com.test.hadoop;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class TestHadoop {

	public static void main(String[] args) throws Exception {

	}

	public static void uploadLocalFile2HDFS(String s, String d)
			throws IOException {
		Configuration config = new Configuration();
		config.addResource("/tmp/core-site.xml");
		FileSystem hdfs = FileSystem.get(config);

		Path src = new Path(s);
		Path dst = new Path(d);

		hdfs.copyFromLocalFile(src, dst);

		hdfs.close();
	}

	private static void writetohdfsfile(String s, String d) throws IOException {

		// 两个参数分别是本地文件系统的的输入文件路径和HDFS中的输出文件位置
		// 如果这段代码最终运行在Hadoop所在的服务器上，那么本地文件系统是相对于那台服务器的本地文件系统
		// 如果这段代码运行在我们Windows PC上，那么本地文件系统是这台Window PC的文件系统
		String localSrc = s;
		String dst = d;

		// 因为本地文件系统是基于java.io包的，所以我们创建一个本地文件输入流
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

		// 读取hadoop文件系统的配置
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");

		// 仍然用FileSystem和HDFS打交道
		// 获得一个对应HDFS目标文件的文件系统
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		// 创建一个指向HDFS目标文件的输出流
		OutputStream out = fs.create(new Path(dst));
		// 用IOUtils工具将文件从本地文件系统复制到HDFS目标文件中
		IOUtils.copyBytes(in, out, 4096, true);

		System.out.println("复制完成");
	}

	private static void readfromhdfsfile(String s) throws IOException {
		// TODO Auto-generated method stub

		// 第一个参数传递进来的是hadoop文件系统中的某个文件的URI,以hdfs://ip 的theme开头
		String uri = s;
		// 读取hadoop文件系统的配置
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");

		// FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			// 实验一：输出全部文件内容
			System.out.println("实验一：输出全部文件内容");
			// 让FileSystem打开一个uri对应的FSDataInputStream文件输入流，读取这个文件
			in = fs.open(new Path(uri));
			// 用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
			IOUtils.copyBytes(in, System.out, 50, false);
			System.out.println();

		} finally {
			IOUtils.closeStream(in);
		}
	}

}
