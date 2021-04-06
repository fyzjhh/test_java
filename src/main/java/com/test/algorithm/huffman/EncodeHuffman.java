package com.test.algorithm.other.huffman;


//дһ���������(ͷ���Ϊ��)
//ͬʱʵ�ֳ�ʼ��Huffman���Ĺ���
public class EncodeHuffman extends Thread {

	public HuffmanUserLineNode<HuffmanNode> current = null;
	public HuffmanUserLineNode<HuffmanNode> head = null;
	public int length = 0;
	public String path;
//	public ProgressBar proBar = new ProgressBar("ѹ��");
	public int fileLen;// input�ļ�����
	public String fname;

	// дһ��������������path
	public EncodeHuffman(String path, String fname) {
		this.path = path;
		this.fname = fname;
	}

	// public HuffmanTreeNode<HuffmanNode> root;
	// ����һ����������ʾ��������
	public int a[] = new int[256];// ��a[1]��ʾ1���ֵĴ���
	public HuffmanCode b[] = new HuffmanCode[256];

	// ����һ����������ȡ���ݲ���¼�����ֽ�������Ƶ��
	public void readFile() {
		try {
			// �����ļ�������
			java.io.FileInputStream fis = new java.io.FileInputStream(path);

			// ���ļ�����װ��һ������д�����������͵������
			// java.io.DataInputStream dis = new java.io.DataInputStream(fis);
			while (fis.available() > 0) {
				int i = fis.read();
				// System.out.println("����->"+i);
				a[i]++;
			}
			uNode();
			// ����������HUFFMAN���
			for (int i = 0; i < 256; i++) {
				HuffmanNode huffmanNode = new HuffmanNode(i, a[i]);
				shuxuIn(huffmanNode);
			}
			HuffmanNode root = utilTree();
			traverSalHuffman(root, "");

		} catch (Exception ef) {
			ef.printStackTrace();
		}
	}

	// 1.
	// 2.

	// public void testStart(){
	// ѹ��.start();
	// ��ʾ����.start();
	// }

	public void writeFile(String fileName) {
		try {
			// �����ļ�������
			java.io.FileInputStream fis = new java.io.FileInputStream(path);
			// �����ļ������
			java.io.FileOutputStream fos = new java.io.FileOutputStream(path
					+ ".hmrq");
			// д���ļ���ʶ��Ϣ
			fos.write((int) 'h');
			fos.write((int) 'f');
			fos.write((int) 'r');
			fos.write((int) 'q');
			// ���ļ������
			fos.write(fileName.length());
			for (int i = 0; i < fileName.length(); i++) {
				fos.write((int) fileName.charAt(i));
			}
			for (int i = 0; i < 256; i++) {
				fos.write(b[i].n);
			}
			int count = 0;
			int i = 0;
			String writes = "";
			String writes2 = "";// ��ת�ַ���
			String writess;
			while ((i < 256) || (count >= 8)) {

				if (count >= 8) {
					writess = "";// ���Ҫת���ĵ���
					for (int t = 0; t < 8; t++) {
						writess = writess + writes.charAt(t);
					}

					// ��writesǰ��λɾ��
					if (writes.length() > 8) {
						writes2 = "";
						for (int t = 8; t < writes.length(); t++) {
							writes2 = writes2 + writes.charAt(t);
						}
						writes = "";
						writes = writes2;
					} else {
						writes = "";
					}
					count = count - 8;
					int intw = changeString(writess);

					fos.write(intw);
					// System.out.println("д��->b["+i+"]:"+intw+"("+writess+")");
				} else {

					count = count + b[i].n;
					writes = writes + b[i].node;
					i++;
				}
			}
			// ��countʣ�µ�д��
			if (count > 0) {
				writess = "";// ���Ҫת���ĵ���
				for (int t = 0; t < 8; t++) {
					if (t < writes.length()) {
						writess = writess + writes.charAt(t);
					} else {
						writess = writess + '0';
					}
				}
				fos.write(changeString(writess));// д��
				System.out.println("д����->" + writess);
			}
			// ��ʾ����
//			proBar.start();
			// ��ʼ�������ݵ�д��
			fileLen = fis.available();// д�ļ��ĳ���
			count = 0;
			writes = "";
			writes2 = "";
			int idata = fis.read();
			int intprogs = 0;
			while ((fis.available() > 0) || (count >= 8)) {
				// System.out.println(fileLen+"  "+fis.available());
				float f = ((float) fileLen - (float) fis.available()) / fileLen;
				// System.out.println(f);
				// System.out.println((int)((float)((fileLen-fis.available())/fileLen)*100));
				if ((int) ((((float) fileLen - (float) fis.available()) / fileLen) * 100) > intprogs) {
					intprogs = (int) ((((float) fileLen - (float) fis
							.available()) / fileLen) * 100);
//					proBar.jPM.setValue(intprogs);
					System.out.println(intprogs);
				}

				if (count >= 8) {
					writess = "";// ���Ҫת���ĵ���
					for (int t = 0; t < 8; t++) {
						writess = writess + writes.charAt(t);
					}

					// ��writesǰ��λɾ��
					if (writes.length() > 8) {
						writes2 = "";
						for (int t = 8; t < writes.length(); t++) {
							writes2 = writes2 + writes.charAt(t);
						}
						writes = "";
						writes = writes2;
					} else {
						writes = "";
					}
					count = count - 8;
					int intw = changeString(writess);
					// System.out.println("д��->"+intw+"("+writess+")");
					fos.write(intw);
				} else {

					count = count + b[idata].n;
					writes = writes + b[idata].node;
					idata = fis.read();
				}
			}
			count = count + b[idata].n;
			writes = writes + b[idata].node;
			// ��countʣ�µ�д��
			int endsint = 0;
			if (count > 0) {
				writess = "";// ���Ҫת���ĵ���
				for (int t = 0; t < 8; t++) {
					if (t < writes.length()) {
						writess = writess + writes.charAt(t);
					} else {
						writess = writess + '0';
						endsint++;
					}
				}
				fos.write(changeString(writess));// д��
				System.out.println("д����->" + writess + "int:" + endsint);

			}
			// дһ��n����ʾǰһ���ֽ�����n��λ�����õ�
			fos.write(endsint);

//			proBar.jPM.setString("ѹ����ϣ�");
			System.out.println("ѹ����ϣ�");
		} catch (Exception ef) {
			ef.printStackTrace();
		}
	}

	/*
	 * util the frist node
	 */
	public void uNode() {
		head = new HuffmanUserLineNode<HuffmanNode>();
		// current=new UserLineNode<HuffmanNode>();
		head.data = null;
		// current=head;
	}

	/*
	 * add the Node
	 */
	// public void addNode(HuffmanNode huffmanNode){
	// UserLineNode<HuffmanNode> aNode =new UserLineNode<HuffmanNode>();
	// aNode.data=huffmanNode;
	// current.next=aNode;
	// current=aNode;
	// }

	/*
	 * ����С˳�����
	 */
	public void shuxuIn(HuffmanNode huffmanNode) {
		HuffmanUserLineNode<HuffmanNode> aNode = new HuffmanUserLineNode<HuffmanNode>();
		aNode.data = huffmanNode;
		HuffmanUserLineNode<HuffmanNode> now = new HuffmanUserLineNode<HuffmanNode>();
		if (head.next == null) {
			head.next = aNode;
			length++;
		} else {
			now = head.next;
			HuffmanUserLineNode<HuffmanNode> now2 = new HuffmanUserLineNode<HuffmanNode>();
			now2 = head;
			while ((aNode.data.times > now.data.times) && (now.next != null)) {
				now2 = now;
				now = now.next;
			}
			if ((now.next == null) && (now.data.times < aNode.data.times)) {
				now.next = aNode;
			} else {
				now2.next = aNode;
				aNode.next = now;
			}
			length++;
		}
	}

	/*
	 * ����һ�������ķ���
	 */
	public void traverSal() {
		HuffmanUserLineNode<HuffmanNode> now = new HuffmanUserLineNode<HuffmanNode>();
		if (head.next == null) {
			System.out.println("Ϊ��!");
		} else {
			now = head;
			for (int i = 0; i < length; i++) {
				now = now.next;
				System.out.println("����Ϊ��" + now.data.data + "times="
						+ now.data.times);

			}

		}
	}

	// ��һ����λ���ַ���ת��һ������
	public int changeString(String s) {
		return ((int) s.charAt(0) - 48) * 128 + ((int) s.charAt(1) - 48) * 64
				+ ((int) s.charAt(2) - 48) * 32 + ((int) s.charAt(3) - 48) * 16
				+ ((int) s.charAt(4) - 48) * 8 + ((int) s.charAt(5) - 48) * 4
				+ ((int) s.charAt(6) - 48) * 2 + ((int) s.charAt(7) - 48);

	}

	/*
	 * creat a methods , util a huffman Tree
	 */
	public HuffmanNode utilTree() {
		HuffmanNode root = (HuffmanNode) head.next.data;
		// if the Line has only one Node
		if (head.next.next == null) {
			return root;
		} else
			// if Line has two or more Nodes
			while ((head.next != null) && (head.next.next != null)) {

				// get the frist two Node 's Huffman data
				HuffmanNode h1 = (HuffmanNode) head.next.data;
				HuffmanNode h2 = (HuffmanNode) head.next.next.data;
				// creat a new Huffman data ,and set the values(1 +2 )
				HuffmanNode hmNode = new HuffmanNode(h1.data + h2.data,
						h1.times + h2.times);
				hmNode.lChild = h1;
				hmNode.rChild = h2;
				// delet the frist two and set the new in
				head.next = head.next.next.next;
				shuxuIn(hmNode);
			}

		return (HuffmanNode) head.next.data;

	}

	// set a traver huffman's methods and give values to each b[i]
	public void traverSalHuffman(HuffmanNode root, String s) {
		if ((root.lChild == null) && (root.rChild == null)) {
			HuffmanCode hc = new HuffmanCode();
			hc.node = s;
			hc.n = s.length();
			b[root.data] = hc;
			// System.out.println(b[root.data].node+"  "+b[root.data].n+"  "+root.data);
		}
		if (root.lChild != null) {
			traverSalHuffman(root.lChild, s + '0');
		}
		if (root.rChild != null) {
			traverSalHuffman(root.rChild, s + '1');
		}

	}

	/*
	 * дһ��run����
	 * 
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		readFile();
		writeFile(fname);
	}

	// public static void main(String args[]){
	// java.util.Scanner sc=new java.util.Scanner(System.in);
	// UtilHuffman uF=new UtilHuffman();
	// uF.uNode();
	// for(int i=0;i<4;i++){
	// int j=sc.nextInt();
	// int k=sc.nextInt();
	// HuffmanNode huffmanNode=new HuffmanNode(j,k);
	// uF.shuxuIn(huffmanNode);
	// System.out.println("j="+j+"k="+k);
	// }
	// uF.traverSal();
	// HuffmanNode root= uF.utilTree();
	// uF.traverSalHuffman(root);
	//

	// }

}
