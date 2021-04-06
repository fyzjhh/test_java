package com.filestation.touchMFS;

import com.google.gson.Gson;
import com.netease.backend.dfs.FileInfo;
import com.netease.backend.mfs.MFSRecord;

import java.util.ArrayList;

/**
 * Date: 12-1-19
 */
public class touchQueryMD5 {

	public static void main(String[] args) throws Exception {

		String md5 = "f7940d2e3d1e802b3334663e4b780636";
		if (args.length >= 1) {
			md5 = args[0];
		}

		DFSWrapper dfsWrapper = new DFSWrapper();

		// launch fsi manager
		String masterIp = "172.17.2.47";
		String masterPort = "5555";
		String newMasterIp = "172.17.2.165:5558,172.17.2.122:5558,172.17.0.113:5558";
		String logDir = "c:/mfslogs/";
		boolean sdfsOverallDistinct = false;
		boolean dfsOverallDistinct = true;
		dfsWrapper.launch(masterIp, masterPort, newMasterIp, logDir, sdfsOverallDistinct, dfsOverallDistinct);

		System.out.println("begin query");

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
	}

}
