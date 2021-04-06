package com.filestation.touchMFS;

import com.google.gson.Gson;
import com.netease.backend.dfs.FileInfo;
import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSRecord;

import java.util.ArrayList;

/**
 * User: xzhou
 * Date: 12-3-8
 */
public class touchUpDoc {

	public static void main(String[] args) throws Exception {

		System.out.println("test begin");

		// launch fsi manager
		String masterIp = "172.17.2.47";
		String masterPort = "5555";
		String newMasterIp = "172.17.2.165:5558,172.17.2.122:5558,172.17.0.113:5558";
		String logDir = "c:/mfslogs/";
		boolean sdfsOverallDistinct = false;
		boolean dfsOverallDistinct = true;

		DFSWrapper dfsWrapper = new DFSWrapper();
		dfsWrapper.launch(masterIp, masterPort, newMasterIp, logDir, sdfsOverallDistinct, dfsOverallDistinct);

		// {0, 1, false} 杭州新SDFS线上 storageType = 1, userType = 0
		int userType = 0;
		int storageType = 1;
		boolean uploadOldMfs = false;

		String content = "hello world 2012.03.23: " + System.currentTimeMillis();
		DocID doc2 = dfsWrapper.insertFileHead(content, userType, storageType, uploadOldMfs);

		if (doc2.isMd5Exist) {
			System.out.print("exist doc: ");
			System.out.println(doc2.getDocId());
			return;
		} else {
			System.out.print("new doc: ");
			System.out.println(doc2.getDocId());
		}

		int start = 0;
		int step = content.length() / 3;
		RunOnSteps t = new RunOnSteps(content.length(), start, step);

		long realDocId2 = -1;
		while (t.next()) {
			int offset = t.getOffset();
			int length = t.getLength();

			System.out.print("[offset, length]: ");
			System.out.print(offset);
			System.out.print(", ");
			System.out.println(length);

			realDocId2 = dfsWrapper.uploadPartOfString(content, offset, length, doc2, userType);

			System.out.print("[docId]: ");
			System.out.println(realDocId2);
		}

		if (realDocId2 != -1) {

			String ip = dfsWrapper.querySNInetAddress(realDocId2);
			System.out.println(ip);

		}

		String md5 = Utils.generateDigest(content);
		ArrayList<MFSRecord> list = dfsWrapper.queryMD5(md5);
		if ((list != null) && (list.size() > 0)) {
			System.out.println(list.size());
			System.out.println(list.get(0).getDocID());

			long docId = list.get(0).getDocID();
			String ip = dfsWrapper.querySNInetAddress(docId);
			System.out.println(ip);

			// print doc info
			FileInfo fileInfo = dfsWrapper.getDocInfo(docId);
			Gson gson = new Gson();
			String j = gson.toJson(fileInfo, FileInfo.class);
			System.out.println(j);
		} else {
			System.out.println("null list");
		}

		System.out.println("the test end");
	}

}
