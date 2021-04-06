package com.filestation.touchMFS;

import com.netease.backend.dfs.DFSException;
import com.netease.backend.dfs.FileInfo;
import com.netease.backend.dfs.FileStream;
import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSRecord;
import com.netease.backend.mfs.MFSStream;
import com.netease.backend.sdfs.MFSFileSystem;
import com.netease.backend.sdfs.mfs.MFSManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class DFSWrapper {

	public void launch(

			String masterIp, String masterPort, String newMasterIp,
			String logDir, boolean sdfsOverallDistinct, boolean dfsOverallDistinct) throws DFSException {

		mfsManager = new MFSFileSystem(sdfsOverallDistinct, dfsOverallDistinct);
		int[] overallList = new int[3];
		overallList[0] = 0;
		overallList[1] = 2;
		overallList[2] = 3;
		mfsManager.setOveralls(overallList);

		mfsManager.launch(masterIp + ":" + masterPort, newMasterIp, logDir + "oldMfsLog", logDir + "newMfsLog");
	}

	/*
	 * query sn ip by docid
	 */
	public String querySNInetAddress(long docId) throws IOException {

		return mfsManager.getSNInfoById(docId).getSnIp();
	}

	public FileInfo getDocInfo(long docId) throws IOException {

		return mfsManager.getFileInfo(docId);
	}

	public ArrayList<MFSRecord> queryMD5(String md5) throws Exception {

		byte[] bytes = Utils.decodeMD5HexString(md5);
		return mfsManager.queryMd5(bytes);
	}

//	public ArrayList<MFSRecord> queryMD5ByLong(long md5high, long md5low) throws Exception {
//
//		byte[] bytes  = Utils.long2byte(md5high, md5low);
//
//		return mfsManager.queryMd5(bytes);
//	}

//	public ArrayList<MFSRecord> queryMD5Bytes(byte[] bytes) throws Exception {
//
//		return mfsManager.queryMd5(bytes);
//	}

	public FileStream readFile(long fileId) throws IOException {

		if (this.mfsManager.whereIs(fileId) == MFSFileSystem.FS.NEWMFS) {
			MFSManager mgr = mfsManager.getNewMFSManager();
			return mgr.getFile(fileId);
		} else if (this.mfsManager.whereIs(fileId) == MFSFileSystem.FS.OLDMFS) {
			com.netease.backend.mfs.MFSManager mgr = mfsManager.getOldMFSManager();
			return mgr.getFile(fileId);
		} else {
			throw new IOException("bad docid: " + fileId);
		}
		//return this.mfsManager.readFileOffset(fileId, offset, length);
	}

	public DocID insertFileHead(String content, int userType, int storageType, boolean uploadOldMfs) throws Exception {

		String md5 = Utils.generateDigest(content);
		byte[] buffer = content.getBytes("UTF-8");

		MFSStream stream;
		try {
			stream = new MFSStream(true, -1, buffer.length, md5, userType);
		} catch (Exception e) {
			throw new DFSException(0, "failed to new mfs stream");
		}

		stream.setUserType(userType);
		stream.setStorageType(storageType);

		return mfsManager.insertFileHead(stream, uploadOldMfs);
	}

	public long uploadPartOfString(String content, int offSet, int length, DocID doc, int userType) throws Exception {

		String md5 = Utils.generateDigest(content);
		byte[] buffer = content.getBytes("UTF-8");

		assert (offSet >= 0);
		assert (length > 0);
		assert ((offSet + length) <= buffer.length);

		byte[] part = new byte[length + 2];
		System.arraycopy(buffer, offSet, part, 0, length);

		boolean overDue = true;
		long expiredTime = System.currentTimeMillis() + 86400;
		MFSStream stream = new MFSStream(new ByteArrayInputStream(part, 0, length), length, md5, userType, overDue, expiredTime);

		stream.setOffset(offSet);
		stream.setLastWrite((offSet + length) == buffer.length);
		long docId = doc.getDocId();
		mfsManager.appendFile(docId, stream, false);

		if ((offSet + length) == buffer.length) {
			stream.setSize(buffer.length);
			docId = mfsManager.insertFileEnd(docId, stream).docId;
		}

		return docId;
	}

	private MFSFileSystem mfsManager = null;
}
