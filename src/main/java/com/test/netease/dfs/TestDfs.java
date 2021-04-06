package com.test.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.netease.backend.dfs.FileInfo;
import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSManager;
import com.netease.backend.mfs.MFSStream;
import com.netease.backend.sdfs.MFSFileSystem;
import com.test.dfs.util.CommonFunction;
import com.test.dfs.util.MFSCommonFunction;

public class TestDfs {

	public static void main(String[] args) throws Exception {
		testUploadAndGetFile("E:\\temp\\errordocid.txt", 1, 0);
	}

	public static void testUploadAndGetFile(String fileName, int storageType,
			int userType) throws IOException {

		MFSFileSystem manager = new MFSFileSystem();

		manager.launch("172.17.2.201:5577", "172.17.2.201:5557", "./", "./");

		boolean isOverDue = true;
		int interval = 60 * 1000;
		long overDueTime = System.currentTimeMillis() + interval;
		boolean result = false;
		FileInputStream fis = null;
		MFSStream mfsStream = null;
		long docid = -1;

		try {
			String md5 = MFSCommonFunction.getMD5(fileName);

			fis = new FileInputStream(fileName);
			int fileLen = fis.available();

			DocID docID = MFSCommonFunction.insertFileHead(manager, md5,
					storageType, userType, isOverDue, overDueTime, true);
			docid = docID.docId;

			MFSCommonFunction.appendFile(manager, fis, md5, docid, userType,
					isOverDue, overDueTime, true);

			mfsStream = new MFSStream(fis, fileLen, md5, userType, isOverDue,
					overDueTime);
			mfsStream.setStorageType(storageType);
			docID = manager.insertFileEnd(docid, mfsStream);

			FileInfo fi = manager.getFileInfo(docid);

			result = CommonFunction.compareDocument(manager, fileName, docid);

		} catch (Exception e) {
			// TODO Auto-generated catch block
		} finally {
			if (fis != null) {
				fis.close();
			}
			if (mfsStream != null) {
				mfsStream.close();
			}
			result = manager.forceDelDoc(docid);
		}
	}

}
