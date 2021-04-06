package com.test.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class Gen_SequenceFile {

	/**
	 * 写入到sequence file
	 * 
	 * @param filePath
	 * @param conf
	 * @param datas
	 */
	public static void write2SequenceFile(String filePath, Configuration conf,
			Collection<IntWritable> datas) {
		FileSystem fs = null;
		SequenceFile.Writer writer = null;
		Path path = null;
		LongWritable idKey = new LongWritable(0);

		try {
			fs = FileSystem.get(conf);
			path = new Path(filePath);
			writer = SequenceFile.createWriter(fs, conf, path,
					LongWritable.class, IntWritable.class);

			for (IntWritable user : datas) {
				idKey.set(user.get()); // userID为Key
				writer.append(idKey, user);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * 从sequence file文件中读取数据
	 * 
	 * @param sequeceFilePath
	 * @param conf
	 * @return
	 */
	public static List<IntWritable> readSequenceFile(String sequeceFilePath,
			Configuration conf) {
		List<IntWritable> result = null;
		FileSystem fs = null;
		SequenceFile.Reader reader = null;
		Path path = null;
		Writable key = null;
		IntWritable value = new IntWritable();

		try {
			fs = FileSystem.get(conf);
			result = new ArrayList<IntWritable>();
			path = new Path(sequeceFilePath);
			reader = new SequenceFile.Reader(fs, path, conf);
			key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),
					conf); // 获得Key，也就是之前写入的userId
			while (reader.next(key, value)) {
				result.add(value);
				value = new IntWritable();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return result;
	}

	private static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		// conf.set("mapred.job.tracker", "local");
		// conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "192.168.12.200:9001");
		conf.set("fs.default.name", "hdfs://192.168.12.200:9000");
		// conf.set("io.compression.codecs",
		// "com.hadoop.compression.lzo.LzoCodec");
		return conf;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		readCompFile("/tmp/r-ab/part-00000");
		readCompFile("/tmp/r-ab/part-00001");
		readCompFile("/tmp/r-ab/part-00002");
		readCompFile("/tmp/r-ab/part-00003");
		readCompFile("/tmp/r-ab/part-00004");
		readCompFile("/tmp/r-ab/part-00005");
		readCompFile("/tmp/r-ab/part-00006");
	}

	private static void writeCompFile(String file) {
		Set<IntWritable> users = new HashSet<IntWritable>();
		IntWritable user = null;
		// 生成数据
		for (int i = 1; i <= 100; i++) {
			user = new IntWritable((int) (Math.random() * 50) + 10);
			users.add(user);
		}
		// 写入到sequence file
		write2SequenceFile(file, getDefaultConf(), users);

	}

	private static void readCompFile(String file) {
		// 从sequence file中读取
		List<IntWritable> readDatas = readSequenceFile(file, getDefaultConf());
		System.out.println("====");
		// 对比数据是否正确并输出
		for (IntWritable u : readDatas) {
			System.out.println(u.toString());
		}
	}

}
