package com.test.java;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/*
 第一个extent
 第一个block
 ==
 ==已经分配的extent，256，4
1个byte
256个byte



 第二个extent之后 每个extent的第一个block
==这个extent中，已经使用的块的最大编号

 */
public class TestRandomFile {

	private static Map<String, RandomAccessFile> raf_map = new HashMap<String, RandomAccessFile>();

	public static void main(String[] args) throws Exception {
		TestRandomFile t = new TestRandomFile();
		String space_file_name = "D:/temp/tab1.txt";
		if (t.crt(space_file_name, 1048576)) {
			RandomAccessFile raf = raf_map.get(space_file_name);
			boolean ret = t.allocate_extent(raf, 1);
		}

	}

	int block_size = 16 * 1024;// 最小4k，最大256k

	byte[] zero_byte = new byte[block_size];
	int block_num_in_extent = 64;

	// 创建制定大小的随机文件
	public boolean crt(String file_name, long file_size) {

		try {
			RandomAccessFile raf = new RandomAccessFile(file_name, "rw");

			if (file_size < block_size) {
				file_size = block_size;
			}

			long t = file_size % block_size;
			if (t != 0) {
				file_size = (file_size - t) + block_size;
			}

			long loop_num = file_size / block_size;
			for (int i = 0; i < loop_num; i++) {
				raf.seek(i * block_size);
				raf.write(zero_byte);
			}
			raf_map.put(file_name, raf);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	// 创建制定大小的随机文件
	public boolean allocate_extent(RandomAccessFile raf, int extent_num)
			throws Exception {

		try {
			byte[] block_byte = new byte[block_size];
			// 获取最大的分配位置
			raf.seek(0);
			raf.read(block_byte);
			// 初始化头

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
	

	public static void deal1() throws Exception {

		File f = new File("D:/temp/testraf.txt");

		RandomAccessFile file = null;

		file = new RandomAccessFile(f, "rw");

		byte[] b = { 48, 49, 50, 51 };

		file.write(b);
		file.seek(8);
		file.write(b);
		file.seek(16);
		file.write(b);
		file.seek(24);
		file.write(b);
		
		byte[] br = new byte[2];
		
		file.seek(2);
		file.read(br);
		System.out.println(Arrays.toString(br));
		
		file.seek(8);
		file.read(br);
		System.out.println(Arrays.toString(br));
		
		file.close();
	}
}
