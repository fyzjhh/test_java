package com.test.algorithm.other.huffman;

/**
 * ��ɣ� ��Ҫ��ϸ��
 * 
 * @author jq
 * 
 */

public class MyHuffman {
	public static void main(String args[]) {
		String op = "c";
		String path = "D:\\temp\\abc.txt";
		String filename = "abc.txt";
		if (op.equalsIgnoreCase("d")) {
			DecodeHuffman dH = new DecodeHuffman(args[0]);
			dH.run();
		}
		if (op.equalsIgnoreCase("c")) {
			EncodeHuffman uH = new EncodeHuffman(path, filename);
			uH.run();
		}

	}

}
