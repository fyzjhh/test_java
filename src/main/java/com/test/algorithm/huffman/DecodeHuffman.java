package com.test.algorithm.other.huffman;

import java.io.IOException;

public class DecodeHuffman extends Thread {
	// ����һ�������������Ÿ����ֽ����ݶ�Ӧ�ı���
	public HuffmanCode b[] = new HuffmanCode[256];
	public String path;
	// public ProgressBar proBar = new ProgressBar("��ѹ��");
	public int fileLen;// input�ļ�����

	/**
	 * ���幹����,����path
	 * 
	 * @param path
	 */
	public DecodeHuffman(String path) {
		this.path = path;
	}

	/**
	 * дrun
	 */
	public void run() {
		Decode();
	}

	/*
	 * ����һ����int תΪ8λ��String �ķ���
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
	 * ����һ���ҵ���ʷֿ���n �ķ���
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
	 * ����һ������ķ���
	 */
	public void Decode() {
		try {
			// �����ļ�������
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
			// �ҵ��㡮.��
			int dian;
			for (dian = path.length() - 1; dian >= 0; dian--) {
				if (path.charAt(dian) == '.') {
					break;
				}
			}

			String path2 = "";
			// ����'.'��ǰ��
			for (int i = 0; i <= dian - 1; i++) {
				path2 = path2 + path.charAt(i);
			}
			// //����fname
			// for (int i=0;i<fname.length();i++){
			// path2=path2+fname.charAt(i);
			//
			System.out.println(path2);
			// ��b�е�n
			for (int i = 0; i < 256; i++) {
				HuffmanCode hC = new HuffmanCode();
				hC.n = fis.read();
				hC.node = "";
				b[i] = hC;
			}
			System.out.println("�����г�ֵ�ˣ�");
			int i = 0;
			int count = 0;
			String coms = "";
			// ��b������
			while (i < 256) {
				if (coms.length() >= b[i].n) {
					// �Ȱ���b[i].nλ��b[i].node
					for (int t = 0; t < b[i].n; t++) {
						b[i].node = b[i].node + coms.charAt(t);

					}
					System.out.println("b[" + i + "]:" + b[i].n + " "
							+ b[i].node);

					// ��comsǰ�⼸λȥ��
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

			// ��ʾ����
			// proBar.start();
			// �õ��ļ���С
			fileLen = fis.available();

			// ����ʽ����

			// �����ļ������
			java.io.FileOutputStream fos = new java.io.FileOutputStream(path2);

			// ����һ��rint �������������
			// int rint;
			String rsrg;// ��ת���ɵ�Sting
			String compString = "";// ��Ҫ�Ƚϵ��ַ���
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
					// System.out.println("д���ˣ�"+"int:"+cint+" "+changeint(cint)+" ="+compString);
					fos.write(cint);
					// ɾ��ǰ.n������
					String compString2 = "";
					for (int t = b[cint].n; t < compString.length(); t++) {
						compString2 = compString2 + compString.charAt(t);
					}
					compString = "";
					compString = compString2;
					// System.out.println(compString+" remain:"+fis.available());

				} else {
					compString = compString + changeint(fis.read());
					// System.out.println("����   remain:"+fis.available());
				}

			}
			// System.out.println("����һ���ֽھͶ�����");
			int cint = fis.read();
			String compString2 = "";
			// System.out.println("re:"+cint+"S:"+compString);
			for (int t = 0; t < compString.length() - cint; t++) {
				compString2 = compString2 + compString.charAt(t);
			}
			compString = compString2;
			// System.out.println("���"+compString);
			// ɾ��ǰ.n������
			while (compString.length() > 0) {
				int ccint = search(compString);
				fos.write(ccint);
				// System.out.println("д���ˣ�"+compString);
				compString2 = "";
				for (int t = b[ccint].n; t < compString.length(); t++) {
					compString2 = compString2 + compString.charAt(t);
				}
				compString = "";
				compString = compString2;
			}
			System.out.println("������ϣ�");
			// proBar.jPM.setString("��ѹ����ϣ�");
		} catch (Exception ef) {
			ef.printStackTrace();
		}
	}

}
