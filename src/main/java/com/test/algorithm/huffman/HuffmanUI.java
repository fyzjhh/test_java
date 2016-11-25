package com.test.algorithm.huffman;

/**
 * 完成！ 需要加细节
 * 
 * @author jq
 * 
 */

public class HuffmanUI extends javax.swing.JFrame {

	private java.awt.Graphics g;

	// 添加MenuBar的方法
	public java.awt.MenuBar creatMenubar() {
		java.awt.MenuBar mB = new java.awt.MenuBar();
		// 定义一个菜单
		java.awt.Menu mn1 = new java.awt.Menu("压缩");
		// 定义菜单mn1的一个 下拉条
		java.awt.MenuItem mn1_I2 = new java.awt.MenuItem("打开");
		mn1_I2.addActionListener(new EncodeHuffmanListener());

		// 将I2加入mn1
		mn1.add(mn1_I2);
		// 将菜单mn1加入mB中
		mB.add(mn1);
		java.awt.Menu mn2 = new java.awt.Menu("解压缩");
		// 定义mn2的下拉条
		java.awt.MenuItem mn2_I2 = new java.awt.MenuItem("打开");
		mn2_I2.addActionListener(new DecodeHuffmanListner());
		// 将I2加入mn1
		mn2.add(mn2_I2);
		// 将菜单mn1加入mB中
		mB.add(mn2);

		return mB;

	}

	// 显示窗体的方法
	public void showUI() {
		// 设置一个名字
		this.setTitle("哈夫曼压缩软件");
		// 设置一个大小
		this.setSize(600, 600);
		// 加一个布局管理器
		java.awt.FlowLayout fl = new java.awt.FlowLayout();
		this.setLayout(fl);
		// 加一个菜单Bar
		// 将mB传入FRAME中
		java.awt.MenuBar mB = this.creatMenubar();
		this.setMenuBar(mB);
		;
		this.setResizable(false);
		this.setDefaultCloseOperation(3);
		this.getContentPane().setBackground(java.awt.Color.WHITE);
		this.setVisible(true);
		g = this.getGraphics();
		g.setFont(new java.awt.Font("黑体", 12, 48));
		g.setColor(java.awt.Color.red);
		g.drawString("欢迎使用！", this.getHeight() / 2 - 100, this.getWidth() / 2);

	}

	public static void main(String args[]) {
		HuffmanUI userF = new HuffmanUI();
		userF.showUI();
	}

}
