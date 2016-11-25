package com.test.algorithm.huffman;

import java.io.IOException;

public class DecodeHuffman extends Thread {
	// 定义一个数组变量来存放各个字节数据对应的编码
	public HuffmanCode b[] = new HuffmanCode[256];
	public String path;
	// public ProgressBar proBar = new ProgressBar("解压缩");
	public int fileLen;// input文件长度

	/**
	 * 定义构造器,传入path
	 * 
	 * @param path
	 */
	public DecodeHuffman(String path) {
		this.path = path;
	}

	/**
	 * 写run
	 */
	public void run() {
		Decode();
	}

	/*
	 * 定义一个将int 转为8位的String 的方法
	 */
	public String changeint(int n) {
		int on = n;
		String s = "";
		for (int i = 0; i < 8; i++) {
			if ((on % 2) == 0) {
				s = '0' + s;
			} else if (on % 2 == 1) {
				s = '1' + s;
			}
			on = on / 2;
		}

		return s;
	}

	public int match(String ss) {

		for (int i = 0; i < 256; i++) {
			if ((b[i].n) == ss.length() && (b[i].node.equals(ss))) {
				return i;
			}
		}

		return -1;
	}

	/*
	 * 定义一个找点合适分开点n 的方法
	 */
	public int search(String s) {
		int n;
		String ss = "";
		int num = -1;
		for (n = 0; n < s.length(); n++) {
			ss = ss + s.charAt(n);
			num = match(ss);
			if (num >= 0) {
				break;
			}
		}
		// System.out.println(num);
		if (n < s.length()) {
			return num;
		} else {
			return -1;
		}

	}

	// public int toUnsigned(int s) {
	// return s & 0x0FFFF;
	// }

	/**
	 * 定义一个译码的方法
	 */
	public void Decode() {
		try {
			// 定义文件输入流
			java.io.FileInputStream fis = new java.io.FileInputStream(path);
			if (fis.read() != 'h') {
				throw new IOException(" Not a .hfrq file ");
			}
			if (fis.read() != 'f') {
				throw new IOException(" Not a .hfrq file ");
			}
			if (fis.read() != 'r') {
				throw new IOException(" Not a .hfrq file ");
			}
			if (fis.read() != 'q') {
				throw new IOException(" Not a .hfrq file ");
			}
			int len = fis.read();
			String fname = "";
			for (int i = 0; i < len; i++) {
				fname = fname + (char) fis.read();

			}
			System.out.println(fname + "  -n:" + len);
			// 找到点‘.’
			int dian;
			for (dian = path.length() - 1; dian >= 0; dian--) {
				if (path.charAt(dian) == '.') {
					break;
				}
			}

			String path2 = "";
			// 复制'.'以前的
			for (int i = 0; i <= dian - 1; i++) {
				path2 = path2 + path.charAt(i);
			}
			// //复制fname
			// for (int i=0;i<fname.length();i++){
			// path2=path2+fname.charAt(i);
			//
			System.out.println(path2);
			// 读b中的n
			for (int i = 0; i < 256; i++) {
				HuffmanCode hC = new HuffmanCode();
				hC.n = fis.read();
				hC.node = "";
				b[i] = hC;
			}
			System.out.println("各点有初值了！");
			int i = 0;
			int count = 0;
			String coms = "";
			// 读b的数据
			while (i < 256) {
				if (coms.length() >= b[i].n) {
					// 先把这b[i].n位给b[i].node
					for (int t = 0; t < b[i].n; t++) {
						b[i].node = b[i].node + coms.charAt(t);

					}
					System.out.println("b[" + i + "]:" + b[i].n + " "
							+ b[i].node);

					// 把coms前这几位去掉
					String coms2 = "";
					for (int t = b[i].n; t < coms.length(); t++) {
						coms2 = coms2 + coms.charAt(t);
					}
					coms = "";
					coms = coms2;
					i++;

				} else {
					coms = coms + changeint(fis.read());
				}
			}

			// 显示进度
			// proBar.start();
			// 得到文件大小
			fileLen = fis.available();

			// 读正式数据

			// 定义文件输出流
			java.io.FileOutputStream fos = new java.io.FileOutputStream(path2);

			// 定义一个rint 来存读进来的数
			// int rint;
			String rsrg;// 存转换成的Sting
			String compString = "";// 存要比较的字符串
			int intprogs = 0;
			while (fis.available() > 1) {

				float f = ((float) fileLen - (float) fis.available()) / fileLen;
				if ((int) ((((float) fileLen - (float) fis.available()) / fileLen) * 100) > intprogs) {
					intprogs = (int) ((((float) fileLen - (float) fis
							.available()) / fileLen) * 100);
					// proBar.jPM.setValue(intprogs);
					System.out.println(intprogs);
				}
				if (search(compString) >= 0) {
					int cint = search(compString);
					// System.out.println("写入了："+"int:"+cint+" "+changeint(cint)+" ="+compString);
					fos.write(cint);
					// 删掉前.n个数据
					String compString2 = "";
					for (int t = b[cint].n; t < compString.length(); t++) {
						compString2 = compString2 + compString.charAt(t);
					}
					compString = "";
					compString = compString2;
					// System.out.println(compString+" remain:"+fis.available());

				} else {
					compString = compString + changeint(fis.read());
					// System.out.println("读了   remain:"+fis.available());
				}

			}
			// System.out.println("还差一个字节就读完啦");
			int cint = fis.read();
			String compString2 = "";
			// System.out.println("re:"+cint+"S:"+compString);
			for (int t = 0; t < compString.length() - cint; t++) {
				compString2 = compString2 + compString.charAt(t);
			}
			compString = compString2;
			// System.out.println("还差："+compString);
			// 删掉前.n个数据
			while (compString.length() > 0) {
				int ccint = search(compString);
				fos.write(ccint);
				// System.out.println("写入了："+compString);
				compString2 = "";
				for (int t = b[ccint].n; t < compString.length(); t++) {
					compString2 = compString2 + compString.charAt(t);
				}
				compString = "";
				compString = compString2;
			}
			System.out.println("解码完毕！");
			// proBar.jPM.setString("解压缩完毕！");
		} catch (Exception ef) {
			ef.printStackTrace();
		}
	}

}
