package com.test.db;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import com.netease.cli.CmdHelper;

/**
 * CSV�ļ���������
 * 
 * @author wy
 * 
 */
public class JhhCsvByteParser {
	private int columnCount;
	private InputStream in;
	private LinkedList<String> warnings;
	private String charset;
	private boolean smart;
	private byte attrQuoter;
	private byte[] attrDelimiter;
	private byte[] lineSeparator;
	private DelimiterCheck dlmCheck;

	public JhhCsvByteParser(String file, String charset, int columnCount,
			byte attrQuoter, byte[] attrDelimiter, byte[] lineSeparator)
			throws IOException {
		in = new FileInputStream(file);
		this.attrQuoter = attrQuoter;
		this.attrDelimiter = attrDelimiter;
		this.lineSeparator = lineSeparator;
		this.charset = charset;
		this.columnCount = columnCount;
		dlmCheck = new DelimiterCheck();
		warnings = new LinkedList<String>();
	}

	public byte[][] getNext() throws IOException {
		warnings.clear();
		byte[] row = JhhCmdHelper.readCommand(in, lineSeparator, attrQuoter);
		if (row == null)
			return null;
		byte[][] r = splitRecord(row);
		return r;
	}

	public List<String> getWarnings() {
		return warnings;
	}

	public void close() throws IOException {
		in.close();
	}

	public boolean isSmart() {
		return smart;
	}

	public void setSmart(boolean smart) {
		this.smart = smart;
	}

	/**
	 * ���ڶ�һ����ݽ������Էָ�ʱ��������Էָ��� ������splitRecord()���ֶԸ�����ĸ�ϸ���ѯ�Ҳ���Ծ�Ĵ�����ʽ
	 * 
	 * 
	 * 
	 */
	private final class DelimiterCheck {
		private int index = 0;
		private int length = 0;
		private boolean isDelimiter = false;

		DelimiterCheck() {
			this.length = attrDelimiter.length;
		}

		/**
		 * ��鵱ǰ�ַ��Ƿ�������ֶμ�����һ����
		 * 
		 * @param ch
		 * @return
		 */
		boolean mayBeDelimiter(byte ch) {
			if (ch == attrDelimiter[index]) {
				index++;
				if (index == length) {// ��ǰ�ַ��ϼ����
					isDelimiter = true;
					index = 0;
				}
				return true;
			} else if (ch == attrDelimiter[0]) {
				// �����ۻ�ıȽ��Ѿ���Ч�����������һ���ַ�Ƚ�
				index = 1;
				return true;
			} else {
				// ��һ�αȽϴӵ�һ���ַ�ʼ
				index = 0;
				return false;
			}
		}

		boolean matchHead(byte ch) {
			return ch == attrDelimiter[0];
		}

		boolean isDelimiter() {
			return isDelimiter;
		}

		/**
		 * �������¿�ʼ�������
		 */
		void reset() {
			index = 0;
			isDelimiter = false;
		}
	}

	/**
	 * �ָ�һ�м�¼��ȡ������ֶ�
	 * 
	 * @param record
	 *            ��¼
	 * @return ����ֶε�ֵ
	 */
	private byte[][] splitRecord(byte[] record) {
		byte[][] attrArray = new byte[columnCount][];
		int prevPos = -1;
		int length = record.length;
		int attrNo = 0;
		boolean inquote = false;
		boolean inEscape = false;

		dlmCheck.reset();

		for (int i = 0; i < length; i++) {
			byte bt = record[i];
			if (bt == attrQuoter && !inEscape) {
				// ��������Ĺ�ϵ����ʱ���ǻ�����?'Ҳ����ȫ�ţ�
				// �����inquote״̬�£�'�ĺ�һ���ַ���,��'��)���н����հף�
				// ���п����Ǵ��
				if (smart && inquote && i < length - 1) {
					byte nc = record[i + 1];
					if (!dlmCheck.matchHead(nc) && nc != attrQuoter
							&& !Character.isWhitespace(nc)) {
						// warnings.add("[WARN] ���������ű�����[\n  ��¼��ʼΪ: "
						// + record.substring(0, 50) + "\n  ��������Ϊ: "
						// + record.substring(i, i + 50) + "\n  �����¼Ϊ: "
						// + record + "]");
						// �������Էָ���ʶ�ļ��
						dlmCheck.reset();
						continue;
					}
				}
				inquote = !inquote;
			}
			if (inEscape)
				inEscape = false;
			else if (bt == '\\')
				inEscape = true;

			if (!inquote && !inEscape && dlmCheck.mayBeDelimiter(bt)) {
				// ��������Ĺ�ϵ����ʱ���ǻ�����?���,���治��,|'|����|\|�հ�|+|-��
				// ����ǰ������ݿ���ȥ���ǰ����ĵĻ�������Ϊ�������뵼�·�������
				if (smart && dlmCheck.isDelimiter() && i < length - 1
						&& i > prevPos + 100) {
					byte nc = record[i + 1];
					if (!dlmCheck.matchHead(nc) && nc != attrQuoter
							&& !Character.isDigit(nc) && nc != '\\'
							&& !Character.isWhitespace(nc) && nc != '+'
							&& nc != '-') {
						// �����������ֵ���ǲ��ǰ�һ�������
						int sampleSize = (i - prevPos - 1) / 20;
						if (sampleSize == 0)
							sampleSize = 1;
						int chineseCount = 0;
						for (int j = prevPos + 1; j < i; j += sampleSize) {
							if ((int) (record[j]) > 127)
								chineseCount++;
						}
						if (chineseCount > 10) {
							// warnings.add("[WARN] ���Էָ���ű�����[\n  ��¼��ʼΪ: "
							// + record.substring(0, 50) + "\n  ��������Ϊ: "
							// + record.substring(i, i + 50)
							// + "\n  �����¼Ϊ: " + record + "]");
							inquote = true;
							// �������Էָ���ʶ�ļ��
							dlmCheck.reset();
							continue;
						}
					}
				}
				if (dlmCheck.isDelimiter()) {
					if (attrNo == columnCount)
						throw new IllegalArgumentException("�����а��������: "
								+ record);
					System.arraycopy(attrArray[attrNo], 0, record, prevPos + 1,
							i - prevPos - dlmCheck.length + 1);
					// attrArray[attrNo] = record.substring(prevPos + 1, i
					// - dlmCheck.length + 1); // ȥ�����Էָ���ʶ
					attrNo++;
					prevPos = i;
					dlmCheck.reset();
				}
			}
		}
		if (attrNo == columnCount)
			throw new IllegalArgumentException("�����а��������: " + record);
		System.arraycopy(attrArray[attrNo], 0, record, prevPos + 1,
				record.length - prevPos);
		// attrArray[attrNo] = record.substring(prevPos + 1);
		attrNo++;
		if (attrNo != columnCount)
			throw new IllegalArgumentException("�����а���ٵ�����: " + record);
		return attrArray;
	}
}
