package com.test.algorithm.huffman;

/**
 * ��ɣ� ��Ҫ��ϸ��
 * 
 * @author jq
 * 
 */

public class HuffmanUI extends javax.swing.JFrame {

	private java.awt.Graphics g;

	// ���MenuBar�ķ���
	public java.awt.MenuBar creatMenubar() {
		java.awt.MenuBar mB = new java.awt.MenuBar();
		// ����һ���˵�
		java.awt.Menu mn1 = new java.awt.Menu("ѹ��");
		// ����˵�mn1��һ�� ������
		java.awt.MenuItem mn1_I2 = new java.awt.MenuItem("��");
		mn1_I2.addActionListener(new EncodeHuffmanListener());

		// ��I2����mn1
		mn1.add(mn1_I2);
		// ���˵�mn1����mB��
		mB.add(mn1);
		java.awt.Menu mn2 = new java.awt.Menu("��ѹ��");
		// ����mn2��������
		java.awt.MenuItem mn2_I2 = new java.awt.MenuItem("��");
		mn2_I2.addActionListener(new DecodeHuffmanListner());
		// ��I2����mn1
		mn2.add(mn2_I2);
		// ���˵�mn1����mB��
		mB.add(mn2);

		return mB;

	}

	// ��ʾ����ķ���
	public void showUI() {
		// ����һ������
		this.setTitle("������ѹ�����");
		// ����һ����С
		this.setSize(600, 600);
		// ��һ�����ֹ�����
		java.awt.FlowLayout fl = new java.awt.FlowLayout();
		this.setLayout(fl);
		// ��һ���˵�Bar
		// ��mB����FRAME��
		java.awt.MenuBar mB = this.creatMenubar();
		this.setMenuBar(mB);
		;
		this.setResizable(false);
		this.setDefaultCloseOperation(3);
		this.getContentPane().setBackground(java.awt.Color.WHITE);
		this.setVisible(true);
		g = this.getGraphics();
		g.setFont(new java.awt.Font("����", 12, 48));
		g.setColor(java.awt.Color.red);
		g.drawString("��ӭʹ�ã�", this.getHeight() / 2 - 100, this.getWidth() / 2);

	}

	public static void main(String args[]) {
		HuffmanUI userF = new HuffmanUI();
		userF.showUI();
	}

}
