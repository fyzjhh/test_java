package com.test.algorithm.other.huffman;

public class HuffmanNode {
	public int data;
	public int times;
	public HuffmanNode lChild;
	public HuffmanNode rChild;

	public HuffmanNode(int data, int times) {
		this.data = data;
		this.times = times;
	}
}
