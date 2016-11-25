package com.test.db;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Vector;

import com.netease.cli.CmdWordReader;

/**
 * 命令解析辅助工具。
 * @author wy
 *
 */
public class JhhCmdHelper {
	/**
	 * 读取一条命令。
	 * @param in 输入
	 * @param delimiter 语句结束符
	 * @param quoter 括起字符
	 * @return 命令，已经读完所有命令时返回null
	 * @throws IOException IO操作失败
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
//				// 没有遇到结束符而是遇到EOF结束，判断当前命令是否全是空白
//				int i;
//				for (i = n - 1; i >= 0; i--) {
//					if (!Character.isWhitespace(buf[i]))
//						break;
//				}
//				if (i < 0)	// 忽略空语句
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
//			if (c == quoter && !inEscape)	// 处理转义
//				inquote = !inquote;
			n++;
//			if (inEscape)
//				inEscape = false;
//			else if (c == '\\')
//				inEscape = true;
			if (!inquote && !inEscape) {
				if (c == endChars[endMatched]) {
					endMatched++;
					if (endMatched == endNChars)	// 匹配到一个分隔符
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
	 * 分割命令，处理引号括起的情况
	 * @param cmd 命令
	 * @return 构成命令的单词数组
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
	 * 判断一个cmd是否为expect指定的命令，不区分大小写。命令由多个单词组成，
	 * 单词与单词之间可通过1个或多个空白分隔。
	 * @param cmd 待判断的命令
	 * @param expect 期望的命令
	 * @return cmd是否为expect指定的命令
	 */
	public static boolean isCommand(String cmd, String expect) {
		return isCommand(cmd, expect, null, -1, true);	
	}
	
	/**
	 * 判断一个cmd是否为expect指定的命令，如果是同时分析出命令参数。命令由多个单词组成，
	 * 单词与单词之间可通过1个或多个空白分隔。
	 * @param cmd 待判断的命令
	 * @param expect 期望的命令
	 * @param args 输出参数。若是指定的命令，则回传命令参数
	 * @param expectedArgsNumber 若>=0则表示预期参数个数，否则表示不判断参数个数
	 * @param ignoreCase 是否忽略大小写
	 * @return cmd是否为expect指定的命令
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
