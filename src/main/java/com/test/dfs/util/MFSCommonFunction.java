package com.test.dfs.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import org.apache.log4j.Logger;
import org.testng.Assert;

import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSManager;
import com.netease.backend.mfs.MFSStream;
import com.netease.backend.sdfs.MFSFileSystem;

public class MFSCommonFunction {
	private static Logger logger = Logger.getLogger(MFSCommonFunction.class);

	public static DocID insertFileHead(MFSFileSystem manager, String md5, int storageType, int userType, boolean isOverDue, long overDueTime, boolean oldMFS) throws Exception{
		logger.debug("insertFileHead.........................");

		MFSStream mfsStream = new MFSStream(null, -1, md5, userType, isOverDue, overDueTime);
		mfsStream.setStorageType(storageType);
		DocID docID = manager.insertFileHead(mfsStream, oldMFS);
		return docID;
	}
	
//	public static DocID appendFile(MFSFileSystem manager, long docid, MFSStream mfsStream,  boolean checkIfExists) throws IOException{
//		return manager.appendFile(docid, mfsStream, checkIfExists);
//	}
	
	public static void appendFile(MFSFileSystem manager, FileInputStream fis, String md5, long docId, int userType, boolean isOverDue, long overDueTime, boolean checkIfExists) throws Exception{
		byte[] buffer = new byte[1024 * 1024 * 2];
		ByteArrayOutputStream output = null;
		InputStream is = null;
		MFSStream mfsStream = null;
		int uploadSize = 0;
		int offset = 0;
		int len = 0;
		int i=0;
		
		len = fis.read(buffer);
		while (len != -1) {
			uploadSize = uploadSize + buffer.length;
			output = new ByteArrayOutputStream();
			output.write(buffer, 0, len);
			logger.debug(++i);
			is = new ByteArrayInputStream(output.toByteArray());
			mfsStream = new MFSStream(is, is.available(), md5, userType, true, overDueTime);
			
			mfsStream.setOffset(offset);
			offset += len;

			len = fis.read(buffer);
			if(len == -1){
				mfsStream.setLastWrite(true);
			}else{
				mfsStream.setLastWrite(false);
			}
			DocID docID = manager.appendFile(docId, mfsStream, checkIfExists);
			Assert.assertTrue(docID.docId == docId);
		}
	}
	
//	public static long insertFileEnd(MFSManager manager, long docid, MFSStream mfsStream) throws Exception{
//		return manager.insertFileEnd(docid, mfsStream).docId;
//	}
	
	public static DocID insertFileEnd(MFSFileSystem manager, long docid, MFSStream mfsStream) throws Exception{
		return manager.insertFileEnd(docid, mfsStream);
	}
	
	public static long uploadFile(MFSFileSystem manager, String fileName, int storageType, int userType, boolean isOverDue, long overDueTime, boolean oldMFS,  boolean checkIfExists) throws Exception {
		long docid = -1;
		String md5;
		FileInputStream fis = null;
		MFSStream mfsStream = null;
		try {
			md5 = MFSCommonFunction.getMD5(fileName);
			fis = new FileInputStream(fileName);
			int fileLen = fis.available();
			logger.debug("fileLen = " + fileLen);
			
			DocID docID = insertFileHead(manager, md5, storageType, userType, isOverDue, overDueTime, oldMFS);
			docid = docID.docId;		
			logger.debug("docid = " + docid);

			if(!docID.isMd5Exist){
				mfsStream = new MFSStream(fis, fileLen, md5, userType, isOverDue, overDueTime);
				mfsStream.setStorageType(storageType);
				
				appendFile(manager, fis, md5, docid, userType, isOverDue, overDueTime, checkIfExists);
				
				docID = manager.insertFileEnd(docid, mfsStream);
				logger.debug("Upload Successfully! docid = " + docid  + " isExist = " + manager.existId(docid));
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		} finally{
			if(fis != null){
				fis.close();
			}
			if(mfsStream != null){
				mfsStream.close();
			}
		}
		
		return docid;
	}
	
	public static long getDeletedFile(MFSFileSystem manager, String fileName, int storageType, int userType, boolean oldMFS, boolean checkIfExist) throws Exception{
		long docid = uploadPermanentFile(manager, fileName, storageType, userType, oldMFS, checkIfExist);
		
		logger.debug("getDeletedFile......docid = " + docid);
		
		boolean result = manager.deleteFile(docid);
		if(!result){
			throw new Exception("Failed to get a deleted file!");
		}
		return docid;
	}
	
	public static long getOmittedFile(MFSFileSystem manager, String fileName, int storageType, int userType, boolean oldMFS, boolean checkIfExist) throws Exception{
		long docid = uploadPermanentFile(manager, fileName, storageType, userType, oldMFS, checkIfExist);

		logger.debug("getOmittedFile.........................");
		boolean result = manager.omitFile(docid);
		if(!result){
			throw new Exception("Failed to get an omitted file!");
		}
		return docid;
	}
	
	public static long uploadPermanentFile(MFSFileSystem manager, String fileName, int storageType, int userType, boolean oldMFS, boolean checkIfExist) throws Exception{
		boolean isOverDue = false;	
		long overDueTime = 0;
		boolean result = false;
		
		long docid = uploadFile(manager, fileName, storageType, userType, isOverDue, overDueTime, oldMFS, checkIfExist);
		logger.debug("docid = " + docid);
		return docid;
	}
	
	public static long uploadTempFile(MFSFileSystem manager, String fileName, int storageType, int userType, boolean oldMFS, boolean checkIfExist) throws Exception{
		boolean isOverDue = true;
		int interval = 60*1000;
		long overDueTime = System.currentTimeMillis() + interval;
		
		return uploadFile(manager, fileName, storageType, userType, isOverDue, overDueTime, oldMFS, checkIfExist);
	}
//	
//	public static long[] uploadTempFileAndGetOverTime(MFSManager manager, String fileName, int storageType, int userType) throws Exception{
//		boolean isOverDue = true;	
//		int interval = 60*1000;
//		long overDueTime = System.currentTimeMillis() + interval;
//		long[] ret = new long[2];
//		
//		ret[0] = uploadFile(manager, fileName, storageType, userType, isOverDue, overDueTime);
//		ret[1] = overDueTime;
//		return ret;
//	}
//	
	public static void cleanFile(MFSFileSystem manager, long docid) throws Exception{
		logger.debug("cleanPermanentFile.........................");
		boolean result = false;
		
		if(!manager.existId(docid)){
			result = manager.recoveryFile(docid);
			logger.debug("recoveryFile = " + result);
		}
		
				
		result = manager.forceDelDoc(docid);
		if(!result){
			throw new Exception("Failed to cleanPermanentFile");
		}
	}
	
	public static void cleanTempFile(MFSManager manager, long docid) throws Exception{
		logger.debug("cleanTempFile.........................");
		
		boolean result = manager.forceDelDoc(docid); 

		if(!result){
			throw new Exception("Failed to cleanTempFile");
		}
	}
	
//	public static void cleanFile(MFSManager manager, String fileName, int storageType, int userType) throws Exception{
//		long docid = uploadPermanentFile(manager, fileName, storageType, userType);
//		boolean result = manager.forceDelDoc(docid);
//		if(!result){
//			throw new Exception("Failed to cleanPermanentFile");
//		}
//	}
	
	public static String getMD5(String fileName) throws Exception {
		FileInputStream fis = new FileInputStream(fileName);
		ByteArrayOutputStream output = null;
		byte[] MD5 = null;
		
		try {
				
				MessageDigest md5 = MessageDigest.getInstance("MD5");
				output = new ByteArrayOutputStream();
			
				

				byte[] buffer = new byte[1024];
				int len;

				while ((len = fis.read(buffer)) != -1) {

					output.write(buffer, 0, len);
					md5.update(buffer, 0, len);
				}
				output.flush();
				MD5 = md5.digest();
			} catch (Exception e) {
				throw e;
			} finally {
				try {
					if (fis != null) {
						fis.close();
					}
					if (output != null) {
						output.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		return getMD5HexString(MD5);
	}
	
	
	public static String getMD5HexString(byte[] MD5) throws Exception{
		
		return byteArrayToHexString(MD5);
	}
	
	private static String byteArrayToHexString(byte[] b) {
		StringBuffer resultSb = new StringBuffer();
		for (int i = 0; i < b.length; i++) {
			resultSb.append(byteToHexString(b[i]));
		}
		return resultSb.toString();
	}
	
	private static String byteToHexString(byte b) {
		int n = b;
		if (n < 0)
			n = 256 + n;
		int d1 = n / 16;
		int d2 = n % 16;
		return hexDigits[d1] + hexDigits[d2];
	}
	
	private final static String[] hexDigits = { "0", "1", "2", "3", "4", "5",
		"6", "7", "8", "9", "A", "B", "C", "D", "E", "F" };
	
	
	  /**
	   *2B44EFD9" --> byte[]{0x2B, 0x44, 0xEF, 0xD9}
	   * @param src String
	   * @return byte[]
	   */
	  public static byte[] MD5HexString2Bytes(String src) throws Exception{
		if(src.length() != 32) {
			throw new Exception("The length of MD5 string is not 32! ");
		}
	    byte[] ret = new byte[16];
	    byte[] tmp = src.getBytes();
	    for(int i = 0; i < 16; i++){
	      ret[i] = uniteBytes(tmp[i*2], tmp[i*2+1]);
	    }
	    return ret;
	  }
	  
	 private static byte uniteBytes(byte src0, byte src1) {
		 byte _b0 = Byte.decode("0x" + new String(new byte[]{src0})).byteValue();
		 _b0 = (byte)(_b0 << 4);
		 byte _b1 = Byte.decode("0x" + new String(new byte[]{src1})).byteValue();
		 byte ret = (byte)(_b0 ^ _b1);
		    
		 return ret;
	 }

}
