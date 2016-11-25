package com.test.db;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Vector;

import com.netease.cli.CmdWordReader;

/**
 * ��������������ߡ�
 * @author wy
 *
 */
public class JhhCmdHelper {
	/**
	 * ��ȡһ�����
	 * @param in ����
	 * @param delimiter ��������
	 * @param quoter �����ַ�
	 * @return ����Ѿ�������������ʱ����null
	 * @throws IOException IO����ʧ��
	 */
	public static byte[] readCommand(InputStream in, byte[] delimiter, byte quoter) throws IOException {
		byte[] endChars = delimiter;
		int endNChars = endChars.length;
		int endMatched = 0;
		byte[] buf = new byte[256];
		int size = buf.length;
		boolean inquote = false;
		int n = 0;
		boolean inEscape = false;
		while (true) {
			byte c = (byte)in.read();
//			if (c < 0) {
//				// û��������������������EOF�������жϵ�ǰ�����Ƿ�ȫ�ǿհ�
//				int i;
//				for (i = n - 1; i >= 0; i--) {
//					if (!Character.isWhitespace(buf[i]))
//						break;
//				}
//				if (i < 0)	// ���Կ����
//					return null;
//				break;
//			}
			if (n >= size - 1) {
				byte[] newBuf = new byte[size * 2];
				System.arraycopy(buf, 0, newBuf, 0, size);
				size *= 2;
				buf = newBuf;
			}
			buf[n] =  c;
//			if (c == quoter && !inEscape)	// ����ת��
//				inquote = !inquote;
			n++;
//			if (inEscape)
//				inEscape = false;
//			else if (c == '\\')
//				inEscape = true;
			if (!inquote && !inEscape) {
				if (c == endChars[endMatched]) {
					endMatched++;
					if (endMatched == endNChars)	// ƥ�䵽һ���ָ���
							break;
				} else
					endMatched = 0;
			} else
				endMatched = 0;
		}
		byte[] ret = new byte[n - endMatched];
		System.arraycopy(buf, 0, ret, 0, n - endMatched);
		return ret;
	}

	/**
	 * �ָ��������������������
	 * @param cmd ����
	 * @return ��������ĵ�������
	 */
	public static String[] splitCommand(String cmd) {
		CmdWordReader wr = new CmdWordReader(cmd);
		Vector<String> v = new Vector<String>();
		String word;
		while ((word = wr.next()) != null)
			v.add(word);
		String[] a = new String[v.size()];
		v.toArray(a);
		return a;
	}

	/**
	 * �ж�һ��cmd�Ƿ�Ϊexpectָ������������ִ�Сд�������ɶ��������ɣ�
	 * �����뵥��֮���ͨ��1�������հ׷ָ���
	 * @param cmd ���жϵ�����
	 * @param expect ����������
	 * @return cmd�Ƿ�Ϊexpectָ��������
	 */
	public static boolean isCommand(String cmd, String expect) {
		return isCommand(cmd, expect, null, -1, true);	
	}
	
	/**
	 * �ж�һ��cmd�Ƿ�Ϊexpectָ������������ͬʱ��������������������ɶ��������ɣ�
	 * �����뵥��֮���ͨ��1�������հ׷ָ���
	 * @param cmd ���жϵ�����
	 * @param expect ����������
	 * @param args �������������ָ���������ش��������
	 * @param expectedArgsNumber ��>=0���ʾԤ�ڲ��������������ʾ���жϲ�������
	 * @param ignoreCase �Ƿ���Դ�Сд
	 * @return cmd�Ƿ�Ϊexpectָ��������
	 */
	public static boolean isCommand(String cmd, String expect,
			List<String> args, int expectedArgsNumber, boolean ignoreCase) {
		String[] a1 = splitCommand(cmd);
		String[] a2 = splitCommand(expect);
		if (a1.length < a2.length)
			return false;
		if (ignoreCase) {
			for (int i = 0; i < a2.length; i++) 
				if (!a2[i].equalsIgnoreCase(a1[i]))
					return false;
		} else {
			for (int i = 0; i < a2.length; i++) 
				if (!a2[i].equals(a1[i]))
					return false;
		}
		
		if (expectedArgsNumber >= 0 && a1.length - a2.length != expectedArgsNumber)
			return false;
		if (expectedArgsNumber < 0)
			return true;
		if (args == null)
			return true;
		args.clear();
		for (int i = a2.length; i < a1.length; i++)
			args.add(a1[i]);
		return true;
	}

}
