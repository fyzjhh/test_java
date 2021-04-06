package com.test.jhh;

public class ShopIdEncryptionUtil {
	/** 自定义进制(0,1没有加入,容易与o,l混淆) */
	private static final char[] RADOM_CODE = new char[] { 'q', 'e', '8', 'a',
			's', '2', 'd', 'z', 'x', '9', 'c', '7', 'p', '5', 'i', 'k', '3',
			'm', 'j', 'u', 'f', 'r', '4', 'v', 'y', 't', 'n', '6', 'b', 'g',
			'h' };

	/** 进制长度 */
	private static final int RANDOM_LENTH = RADOM_CODE.length;

	/** 序列最小长度 */
	private static final int SIZE = 7;

	/** (不能与自定义进制有重复) */
	private static final String COMPLEMENTED = "w";

	/**
	 * 根据ID生成六位随机码
	 * 
	 * @param id
	 *            ID
	 * @return 随机码
	 */
	public static String toSerialCode(int id) {
		char[] buf = new char[32];
		int charPos = 32;

		while ((id / RANDOM_LENTH) > 0) {
			int ind = (int) (id % RANDOM_LENTH);
			buf[--charPos] = RADOM_CODE[ind];
			id /= RANDOM_LENTH;
		}
		buf[--charPos] = RADOM_CODE[(int) (id % RANDOM_LENTH)];
		// 转换
		String str = new String(buf, charPos, (32 - charPos));
		return str;
	}

	public static int codeToId(String code) {
		char chs[] = code.toCharArray();
		int res = 0;
		for (int i = 0; i < chs.length; i++) {
			int ind = 0;
			for (int j = 0; j < RANDOM_LENTH; j++) {
				if (chs[i] == RADOM_CODE[j]) {
					ind = j;
					break;
				}
			}
			if (i > 0) {
				res = res * RANDOM_LENTH + ind;
			} else {
				res = ind;
			}
		}
		return res;
	}

	public static void main(String[] args) {
		String idCode = ShopIdEncryptionUtil.toSerialCode(4980);
		String userCode = ShopIdEncryptionUtil.toSerialCode(2632939);
		String inviteCode = idCode + COMPLEMENTED;
		int size = SIZE - inviteCode.length();
		if (userCode.length() < size) {
			inviteCode = inviteCode + userCode;
		} else {
			inviteCode = inviteCode
					+ userCode.substring(userCode.length() - size,
							userCode.length());
		}

		System.out.println(inviteCode);

	}

}
