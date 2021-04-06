package com.test.algorithm.other.huffman;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
/**
 * ����ѹ��
 * @author jq
 *
 */
public class EncodeHuffmanListener implements ActionListener {

	javax.swing.JFileChooser fileChooser =new javax.swing.JFileChooser();
	File f;
	//��д���з���
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		//��fileChooser����һ���Ի���
		fileChooser.showOpenDialog(null);
		f=fileChooser.getSelectedFile();
		String s=f.getAbsolutePath();
		String fileName=f.getName();
		System.out.println(s);
		EncodeHuffman uH=new EncodeHuffman(s,fileName);
		uH.start();
		
		
		
	}

}