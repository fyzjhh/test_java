package com.test.dfs.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;

import com.netease.backend.dfs.DFSException;
import com.netease.backend.dfs.FileStream;
import com.netease.backend.dfs.SNInfo;
import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSStream;
import com.netease.backend.sdfs.MFSFileSystem;

public class MfsSystemTest extends Thread {
	String filePath;

	public long sleep;

	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

	MfsSystemTest(String filePath, int sleep) {
		this.filePath = filePath;
		this.sleep = sleep;

	}

	MFSFileSystem ufsWrapManager = new MFSFileSystem();

	void initFm() {
		System.out.println(this.getClass() + " inin MFSFileSystem begin");
		try {
			boolean lauched = ufsWrapManager.launch("172.21.0.5:5558", "172.17.2.201:5558", "./",
					"./");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println(this.getClass() + " inin UFSManager over");
	}

	private void downLoadFile(FileStream doc) throws FileNotFoundException, IOException {
		InputStream in = doc.getInputStream();
		byte[] buffer = new byte[1024 * 1024];
		int v;
		int totalSize = 0;
		File f = new File("D:/dd.dd");
		if (f.isFile())
			f.delete();
		FileOutputStream fis = new FileOutputStream(f);
		while ((v = in.read(buffer)) != -1) {
			totalSize += v;
			fis.write(buffer, 0, v);
			// System.out.write(buffer, 0, v);
		}
		fis.close();
		System.out.println("file size : " + totalSize);
		System.out.flush();
	}

	public void run() {
		initFm();
		try {
			int i = 0;
			while (i < 1) {
				int d = (int) (100000 + (999999 - 100000) * Math.random());
				testUploadSdfs(i, "f2" + d + "fe1ae2caf120edcf4526f12f", i);
				Thread.sleep(50000);
			}
			System.out.println("shutdown over--");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void testUploadSdfs(int currUploadIndex, String md5, int userType) throws Exception {

	 	md5 = "f8a38192c99a797f5626353747525191";
		try {
			// System.out.println("recoveryFile = " +
			// ufsWrapManager.recoveryFile( 3096224743817566L));
			FileInputStream fis = new FileInputStream(filePath);
			boolean isTempFile = false;
			long expriedTime = System.currentTimeMillis() + (1000 * 80000);
			MFSStream mfsStream = new MFSStream(null, -1, md5, userType, isTempFile, expriedTime);
			mfsStream.setStorageType(2);
			// 建立空文件 第一个false 表示 往新的文件系统往,第2个false 表示全局去重
			DocID id = ufsWrapManager.insertFileHead(mfsStream, false);
			System.out.println(id.expirytime + " " + id.isMd5Exist);
			System.out.println("分配得到 id:" + id.docId);
			SNInfo sninfo = ufsWrapManager.getSNInfoById(id.docId);
			String hostname = InetAddress.getByName(sninfo.getSnIp()).getHostName();
			getAllSNInfoById(id.docId);

			int len;
			byte[] buffer = new byte[1024 * 1024 * 2];
			int totalBuffer = buffer.length;
			int uploadSize = 0;
			int offset = 0;
			ByteArrayOutputStream output = null;
			System.out.print("开始上传添加大小");
			while ((len = fis.read(buffer)) != -1) {
				uploadSize = uploadSize + buffer.length;
				output = new ByteArrayOutputStream();
				output.write(buffer, 0, len);
				System.out.print(".");
				InputStream is = new ByteArrayInputStream(output.toByteArray());
				mfsStream = new MFSStream(is, is.available(), md5, userType, isTempFile, expriedTime);
				// 这里一定别忘记设置 append 的区块信息
				mfsStream.setLastWrite(len < totalBuffer);
				mfsStream.setOffset(offset);
				id = ufsWrapManager.appendFile(id.docId, mfsStream, false);
				offset += len;
			}
			System.out.println();
			// 最后的插入
			long currId = ufsWrapManager.insertFileEnd(id.docId, mfsStream, false, false).docId;
			System.out.println(md5 + "  " + id.docId + " " + hostname + " "
					+ filePath.substring(filePath.lastIndexOf("/") + 1));
			System.out.println("-------------------------");
			getAllSNInfoById(currId);
			downLoadFile(ufsWrapManager.getFile(currId));
			// System.out.println("omit = " + ufsWrapManager.omitFile(currId));
			System.out.println("recoveryFile = " + ufsWrapManager.recoveryFile(currId));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Thread.sleep(10 * 1000);
		}

	}

	private void getAllSNInfoById(long docID) throws IOException {
		SNInfo sninfo = ufsWrapManager.getSNInfoById(docID);
		System.out.println(" uploadfile " + docID + "  " + sninfo.getSnIp() + " " + sninfo.getSnName() + " "
				+ sninfo.getSnDir() + "/" + "  " + format.format(System.currentTimeMillis()));
	}

	public static void main(String[] args) throws DFSException {
		try {
			MfsSystemTest instacne = new MfsSystemTest("E:/testFiles/SAM.jpg", 20000000);
			new Thread(instacne).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
