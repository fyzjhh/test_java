package com.test.algorithm.other.huffman;

import javax.swing.JProgressBar;

public class ProgressBar extends Thread {

	public JProgressBar jPM = new JProgressBar(0, 99);

	public String name;
	public javax.swing.JFrame jF = new javax.swing.JFrame();

	// ����һ������������������
	public ProgressBar(String name) {
		this.name = name;
	}

	// ����һ����ʾ����ķ���
	public void showUI() {

		// ����һ������
		jF.setTitle(name + "�С���");
		// ����һ����С
		jF.setSize(200, 200);
		// ��һ�����ֹ�����
		java.awt.FlowLayout fl = new java.awt.FlowLayout();
		jF.setLayout(fl);
		jF.setResizable(false);
		jF.setAlwaysOnTop(true);
		// jF.setDefaultCloseOperation(3);
		jF.getContentPane().setBackground(java.awt.Color.WHITE);
		jPM.setStringPainted(true);
		jPM.setSize(120, 15);
		jF.add(jPM);
		jF.setVisible(true);
	}

	public void run() {
		showUI();
		while (true) {
			try {
				Thread.sleep(100);
				// jPM.setValue(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			jF.repaint();
		}
	}

}
