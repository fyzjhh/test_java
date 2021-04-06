package com.test.dfs.filestation;

import com.google.gson.Gson;
import com.netease.backend.dfs.FileInfo;

public class touchIt {

	public static void main(String[] args) throws Exception {

		long docId = 645140646620846414L;

		if (args.length >= 1) {
			docId = Long.valueOf(args[0]);
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

		// print sn ip
		String ip = dfsWrapper.querySNInetAddress(docId);
		System.out.println(ip);

		// print doc info
		FileInfo fileInfo = dfsWrapper.getDocInfo(docId);

		Gson gson = new Gson();
		String j = gson.toJson(fileInfo, FileInfo.class);
		System.out.println(j);

		long md5low = -6720716013347022113L;
		long md5high = -2412226901519899383L;
		byte[] bytes = Utils.long2byte(md5high, md5low);

		String hex = Utils.getBytesAsHexString(bytes, 0, bytes.length);

		System.out.println(hex);
	}

}
