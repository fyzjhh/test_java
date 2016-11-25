package com.test.zxing;

import java.io.File;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;

/**
 * @blog http://sjsky.iteye.com
 * @author Michael
 */
public class ZxingEAN13Encoder {

	/**
	 * 编码
	 * 
	 * @param contents
	 * @param width
	 * @param height
	 * @param imgPath
	 */
	public void encode(String contents, int width, int height, String imgPath) {
		int codeWidth = 3 + // start guard
				(7 * 6) + // left bars
				5 + // middle guard
				(7 * 6) + // right bars
				3; // end guard
		codeWidth = Math.max(codeWidth, width);
		try {
			BitMatrix bitMatrix = new MultiFormatWriter().encode(contents,
					BarcodeFormat.EAN_13, codeWidth, height, null);

			MatrixToImageWriter
					.writeToFile(bitMatrix, "png", new File(imgPath));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String imgPath = "d:/temp/zxing_EAN13.png";
		// 益达无糖口香糖的条形码
		String contents = "0123456789012";

		int width = 500, height = 200;
		ZxingEAN13Encoder handler = new ZxingEAN13Encoder();
		handler.encode(contents, width, height, imgPath);

		System.out.println("Michael ,you have finished zxing EAN13 encode.");
	}
}