package com.test.java;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegExp {

	static class OOMObject {

	}

	public static long test12(String str) {
		long addr = 0L;
		try {
			String[] ipArr = str.split("\\.");
			addr += Long.parseLong(ipArr[3]);
			addr += Long.parseLong(ipArr[2]) * 256L;
			addr += Long.parseLong(ipArr[1]) * 256L * 256L;
			addr += Long.parseLong(ipArr[0]) * 256L * 256L * 256L;
			return addr;
		} catch (Exception e) {
			return -1L;
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println(test12("192.168.1.1"));
		System.out.println(test12("192.168.1.2"));
		System.out.println(test12("192.168.1.10"));

	}

	private static void test11() throws Exception {

		String str = "/data/tmp/tab_map_race_20151201.log";
		String regEx = "(.*)[/\\\\]([^/\\\\]+)_([^_]+)";
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		while (m.find()) {
			System.out.println(m.group(0));
			System.out.println(m.group(1));
			System.out.println(m.group(2));
		}

	}

	private static void test12() throws Exception {
		// ��ȡ�ַ� ��ȡ da12bka3434bdca4343bdca234bm
		// ��ȡ�����ַ�a��b֮������֣��������a֮ǰ���ַ�����c,b������ַ������d������ȡ��
		String str = "da12bc xca3434bd cdda4343bd  ca234bm";
		String regEx = "(?<!c)a(\\d+)bd";
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		while (m.find()) {
			System.out.println(m.group(1)); // ����ֻҪ������1�����ּ��ɡ���� 3434
			System.out.println(m.group(0)); // 0����������ʽ���������û��������(?<!c)���ַ�
											// �����
											// a3434bd
		}

	}

	private static void test2() throws Exception {

		String[] dataArr = { "a100", "b20", "c30", "df10000", "gh0t" };

		for (String str : dataArr) {
			String patternStr = "\\w+\\d+";

			boolean result = Pattern.matches(patternStr, str);
			if (result) {
				System.out.println("�ַ�" + str + "ƥ��ģʽ" + patternStr + "�ɹ�");
			} else {
				System.out.println("�ַ�" + str + "ƥ��ģʽ" + patternStr + "ʧ��");
			}
		}

	}

	private static void test3() throws Exception {

		String str = "нˮ,ְλ,����;���� �Ա�";
		String[] dataArr = str.split("[,\\s;]");
		for (String strTmp : dataArr) {
			System.out.println(strTmp);
		}

		// String���split����֧��������ʽ,������ģʽ��ƥ�䡱,��,�����ո�,��;���е�һ��,split�����ܰ�����������һ�������ָ���,��һ���ַ���ֳ��ַ�����.

	}

	private static void test4() throws Exception {

		String str = "10Ԫ 1000����� 10000Ԫ 100000RMB";
		str = str.replaceAll("(\\d+)(Ԫ|�����|RMB)", "\1��");
		System.out.println(str);

		// ������,ģʽ��(\d+)(Ԫ|�����|RMB)�������ŷֳ�������,��һ��\d+ƥ�䵥����������,�ڶ���ƥ��Ԫ,�����,RMB�е�����һ��,�滻���ֱ�ʾ��һ����ƥ��Ĳ��ֲ���,�������滻�ɣ�.
		//
		// �滻���strΪ��10 ��1000 ��10000 ��100000

	}

	private static void test5() throws Exception {
		Pattern p = Pattern.compile("m(o+)n", Pattern.CASE_INSENSITIVE);

		// ��Pattern���matcher()�������һ��Matcher����
		Matcher m = p.matcher("moon mooon xxx Mon mooooon Mooon yyy");
		StringBuffer sb = new StringBuffer();

		// ʹ��find()�������ҵ�һ��ƥ��Ķ���
		boolean result = m.find();

		// ʹ��ѭ���ҳ�ģʽƥ��������滻֮,�ٽ����ݼӵ�sb��
		while (result) {
			m.appendReplacement(sb, "moon");
			result = m.find();
		}
		// ������appendTail()���������һ��ƥ����ʣ���ַ�ӵ�sb�
		m.appendTail(sb);

		System.out.println("�滻��������" + sb.toString());
	}

	private static void test6() {

		String regex = "<(\\w+)>(\\w+)</(\\w+)>";
		Pattern pattern = Pattern.compile(regex);

		String input = "<name>Bill</name><salary>50000</salary><title>GM</title>";

		Matcher matcher = pattern.matcher(input);

		while (matcher.find()) {
			System.out.println(matcher.group(2));
		}
	}

	private static void test7() {
		String regex = "([a-zA-Z]+[0-9]+)";
		Pattern pattern = Pattern.compile(regex);

		String input = "age45 salary500000 50000 title";

		Matcher matcher = pattern.matcher(input);

		StringBuffer sb = new StringBuffer();

		while (matcher.find()) {
			String replacement = matcher.group(1).toUpperCase();
			matcher.appendReplacement(sb, replacement);
		}
		matcher.appendTail(sb);

		System.out.println("�滻����ִ�Ϊ" + sb.toString());
	}

	private static void test8() {
		String regex = "([a-zA-Z]+[0-9]+)";
		Pattern pattern = Pattern.compile(regex);

		String input = "age45 salary500000 50000 title";

		Matcher matcher = pattern.matcher(input);

		StringBuffer sb = new StringBuffer();

		while (matcher.find()) {
			String replacement = matcher.group(1).toUpperCase();
			matcher.appendReplacement(sb, replacement);
		}
		matcher.appendTail(sb);

		System.out.println("�滻����ִ�Ϊ" + sb.toString());
	}
}
