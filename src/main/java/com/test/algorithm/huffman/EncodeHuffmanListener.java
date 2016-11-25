package com.test.algorithm.huffman;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
/**
 * 用来压缩
 * @author jq
 *
 */
public class EncodeHuffmanListener implements ActionListener {

	javax.swing.JFileChooser fileChooser =new javax.swing.JFileChooser();
	File f;
	//重写其中方法
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		//将fileChooser弹出一个对话框
		fileChooser.showOpenDialog(null);
		f=fileChooser.getSelectedFile();
		String s=f.getAbsolutePath();
		String fileName=f.getName();
		System.out.println(s);
		EncodeHuffman uH=new EncodeHuffman(s,fileName);
		uH.start();
		
		
		
	}

}