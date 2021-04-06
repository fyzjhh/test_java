package com.test.dfs.util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Random;

import org.apache.log4j.Logger;

import com.netease.backend.dfs.DFSManager;
import com.netease.backend.dfs.FileStream;
import com.netease.backend.sdfs.MFSFileSystem;
import com.netease.backend.ufs.UFSManager;
import com.netease.backend.ufs.UFSStream;

public class CommonFunction {
	private static Logger logger = Logger.getLogger(CommonFunction.class);
	
	/**
	 * UFSManager插入数据
	 * @interface：public long insertFile(UFSStream fs)， 它根据userType是否为0判断是否进行去重插入
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return docid
	 * @throws IOException
	 */
	public static long insertFile(UFSManager manager, String fileName, int storageType, int userType) throws IOException {
		long docid = UFSManager.INVALID_DOCID;
		FileInputStream fis = null;
		UFSStream ufsStream = null;
		try {
			fis = new FileInputStream(fileName);
			ufsStream =  new UFSStream(new FileStream(fis, fis.available(), storageType, userType));
			docid = manager.insertFile(ufsStream);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Failed to insert test Files");
		} finally{
			fis.close();
			ufsStream.close();
		}
		return docid;
	}
	
	/**
	 * UFSManager是否重复插入
	 * @interface：public long insertFile(UFSStream fs, boolean unduplicate)，它根据unduplicate判断是否进行去重插入
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @param unduplicate
	 * @return docid
	 * @throws IOException
	 */
	public static long insertFile(UFSManager manager, String fileName, int storageType, int userType, boolean unduplicate) throws IOException {
		long docid = UFSManager.INVALID_DOCID;
		FileInputStream fis = null;
		UFSStream ufsStream = null;
		try {
			fis = new FileInputStream(fileName);
			ufsStream =  new UFSStream(new FileStream(fis, fis.available(), storageType, userType));
			docid = manager.insertFile(ufsStream, unduplicate);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Failed to insert test Files");
		} finally{
			fis.close();
			ufsStream.close();
		}
		return docid;
	}
	
	/**
	 * UFSManager使用insertFile(UFSStream fs)接口插入去重文件
	 * @param manager
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static long insertFile(UFSManager manager, String fileName) throws IOException {
		return insertFile(manager, fileName, FileStream.STG_REPLICA, FileStream.USR_NORMAL);
	}
	
	/**
	 * DFSManager插入文件
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return
	 * @throws IOException
	 */
	public static long insertFile(DFSManager manager, String fileName, int storageType, int userType) throws IOException {
		long docid = UFSManager.INVALID_DOCID;
		FileInputStream fis = null;
		FileStream fs = null;

		try {
			fis = new FileInputStream(fileName);
			fs =  new FileStream(fis, fis.available(), storageType, userType);
			docid = manager.insertFile(fs);
			logger.debug("docid = " + docid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Failed to insert test Files");
		} finally{
			if(fs != null){
				fs.close();
			}if(fis != null){
				fis.close();
			}
		}
		return docid;
	}
	
	/**
	 * DFSManager获取删除文件的docid
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return
	 * @throws Exception
	 */
	public static long getDeletedId(DFSManager manager, String fileName, int storageType, int userType) throws Exception {
		long docId = insertFile(manager, fileName, storageType, userType);
		if(!manager.deleteFile(docId)){
			throw new Exception("Failed to get a deleted id!");
		}else{
			logger.debug("A deleted docid = " + docId);
			return docId;
		}
	}
	
	/**
	 * UFSManager获取删除文件的docid
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return
	 * @throws Exception
	 */
	public static long getDeletedId(UFSManager manager, String fileName, int storageType, int userType) throws Exception {
		long docid = insertFile(manager, fileName, storageType, userType);
		logger.debug("docid = " + docid);

//		deleteAll(manager, docid);
		if(!manager.forceDelDoc(docid)){
			throw new Exception("Failed to get a deleted id!");
		}
	
		docid = insertFile(manager, fileName, storageType, userType);
		int ref = manager.getDocRef(docid);
		logger.debug("ref = " + ref + " docid = " + docid);
		
//		if(!manager.forceDelDoc(docid)){
		if(!manager.deleteFile(docid)){

			throw new Exception("Failed to get a deleted id!");
		}else{
			return docid;
		}
	}
	
	/**
	 * UFSManager获取隐藏文件的docid
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return
	 * @throws Exception
	 */
	public static long getOmittedId(UFSManager manager, String fileName, int storageType, int userType) throws Exception {
		long docid = insertFile(manager, fileName, storageType, userType);
		logger.debug("omitted docid = " + docid);
//		deleteAll(manager, docId);
		if(!manager.forceDelDoc(docid)){
			throw new Exception("Failed to get an omitted id!");
		}
		docid = insertFile(manager, fileName, storageType, userType);
		logger.debug("The Ommited DocId = " + docid);
		
		if(!manager.omitFile(docid)){
			throw new Exception("Failed to get an omitted id!");
		}else{
			return docid;
		}
	}
	
	/**
	 * DFSManager获取删除文件的docid
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return
	 * @throws Exception
	 */
	public static long getOmittedId(DFSManager manager, String fileName, int storageType, int userType) throws Exception {
		long docId = insertFile(manager, fileName, storageType, userType);
//		docId = insertFile(manager, fileName, storageType, userType);
		logger.debug("The omitted docid = " + docId);
		
		if(!manager.omitFile(docId)){
			throw new Exception("Failed to get an omitted id!");
		}else{
			return docId;
		}
	}
	
	/**
	 * 返回指定范围内的一个随机数
	 * @param length
	 * @return
	 */
	public static int getRandNum(long length){
		Random rand = new Random();
		return rand.nextInt((int) length);
	}
	
	/**
	 * 比较Document
	 * docid用于下载文件
	 * @param manager
	 * @param fileName
	 * @param docId
	 * @return
	 */
	public static boolean compareDocument(MFSFileSystem manager, String fileName, long docId) throws IOException{
		boolean result = false;
		logger.debug("Start reading file............");
		FileStream fs = null;;
		FileInputStream fis = null;
		long fileLen = -1;
		
		try {
			fs = manager.getFile(docId);
			fis = new FileInputStream(fileName);

			fileLen = fis.available();
			if(fileLen != fs.getSize()){
				logger.error("Unequal File Length! expected = " + fileLen + " actual = " + fs.getSize() );
				result = false;
			}else{
				result = VerifyUtil.compare(fis, fs.getInputStream());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(fis != null){
				fis.close();
			}
			if(fs != null){
				fs.close();
			}
		}
	


		return result;
	}
	
	/**
	 * 使用同一个文件向同一个docid追加多次后进行比较
	 * docId用于下载文件
	 * @param manager
	 * @param fileName
	 * @param docId
	 * @param times
	 * @return
	 * @throws IOException
	 */
	public static boolean compareDocument(DFSManager manager, String fileName, long docId, int times) throws IOException{
		boolean result = false;
		FileStream fs = manager.getFile(docId);
		long fileLen = CommonFunction.getFileSize(fileName);
		logger.debug("expected Len = " + (fileLen*times) + " actual = " + fs.getSize()); 
		if((fileLen*times) != fs.getSize()){
			logger.error("Unequal File Length! expected = " + (fileLen*times) + " actual = " + fs.getSize() );
			result = false;
		}else{
			//比较文件内容
			result = VerifyUtil.compare(fileName, fs.getInputStream(), times);
		}
		fs.close();
		return result;
	}
	
	/**
	 * copyFile时，复制文件与原文件进行比较
	 * @param manager
	 * @param docId
	 * @param newDocId
	 * @return
	 * @throws IOException
	 */
	public static boolean compareDocument(DFSManager manager, long docId, long newDocId) throws IOException{
		boolean result = false;
		FileStream fs1 = manager.getFile(docId);
		FileStream fs2 = manager.getFile(newDocId);
		result = VerifyUtil.compare(fs1.getInputStream(), fs2.getInputStream());
		fs1.close();
		fs2.close();
		return result;
	}
	
	public static boolean compareDocument(String fileName1, String fileName2) throws IOException{
		boolean result = false;
		
		FileInputStream fis1 = new FileInputStream(fileName1);
		FileInputStream fis2 = new FileInputStream(fileName2);
		int fileLen1 = fis1.available();
		int fileLen2 = fis2.available();
		if( fileLen1 != fileLen2){
			logger.error("Unequal File Length! expect = " + fileLen1 + " actual = " + fileLen2);
			result = false;
		}else{
			result = VerifyUtil.compare(fis1, fis2);
		}
		fis1.close();
		fis2.close();
		return result;
	}
	/**
	 * 使用UFSManager返回指定文件的删除次数
	 * @param manager
	 * @param docId
	 * @return
	 * @throws IOException
	 */
	public static int deleteAll(UFSManager manager, long docId) throws IOException {
		logger.debug("Deleting the file completely. docid = " + docId);
		int count = 0;
		int total = 0;
		boolean result = false;
		
		count = manager.getDocRef(docId);
		total = count;

		if(!manager.existId(docId)){
			manager.recoveryFile(docId); //如果是隐藏文件，先恢复再删除
		}
		while(count != 0){
			logger.debug("count = " + count);
			result = manager.deleteFile(docId);
			if(!result){
				throw new IOException("deleteFile!=true");
			}
			count--;
		}

		return total;
	}
	
	/**
	 * 
	 * @param manager
	 * @param fileName
	 * @param storageType
	 * @param userType
	 * @return
	 * @throws IOException
	 */
	public static int deleteAll(UFSManager manager, String fileName, int storageType, int userType) throws IOException {
		int count = 0; 
		long docId = -1;
		boolean result = false;
		
		docId = insertFile(manager, fileName, storageType, userType);
		
		logger.debug("Deleting the file completely. docid = " + docId);
		
//		if(docId != DFSConstant.INVALID_CLIENTID && !manager.existId(docId)){
//			//如果是隐藏文件先恢复
//			result = manager.recoveryFile(docId);
//			if(!result){
//				throw new IOException("deleteFile != true");
//			}
//		}
		
		count = deleteAll(manager, docId);
		logger.debug("Complete Deleting. Deleting Times = " + count);

		return count;
	}
	
	
	public static String getMD5(String fileName) throws Exception{
		FileInputStream fis = new FileInputStream(fileName);
		return getMD5(fis);
	}
	/**
	 * 获取文件的MD5值
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public static String getMD5(InputStream input) throws Exception{
		byte[] MD5 = null;
		String result = null;

		if (MD5 == null) {
//			InputStream input = null;
			ByteArrayOutputStream output = null;
			try {
				MessageDigest md5 = MessageDigest.getInstance("MD5");
//				input = new FileInputStream(fileName);
				output = new ByteArrayOutputStream();

				byte[] buffer = new byte[1024*1024*4];
				int len;

				while ((len = input.read(buffer)) != -1) {
					output.write(buffer, 0, len);
					md5.update(buffer, 0, len);
				}
				output.flush();
				MD5 = md5.digest();
				result = new String(MD5, "UTF-8");
//				logger.debug("md5 = " + result + "md5.len = " + result.length());
				
				input.close();
				output.close();
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			} 
		}
		return result;
	}
	
	public static byte[] getANonExistentMD5() throws Exception{
		byte[] MD5 = null;
		String result = null;
		String fileName = "E:/testFiles/NonMD5.txt";

		if (MD5 == null) {
//			InputStream input = null;
			FileInputStream input = new FileInputStream(fileName);
			ByteArrayOutputStream output = null;
			try {
				MessageDigest md5 = MessageDigest.getInstance("MD5");
//				input = new FileInputStream(fileName);
				output = new ByteArrayOutputStream();

				byte[] buffer = new byte[1024*1024*4];
				int len;

				while ((len = input.read(buffer)) != -1) {
					output.write(buffer, 0, len);
					md5.update(buffer, 0, len);
				}
				output.flush();
				MD5 = md5.digest();
//				result = new String(MD5, "UTF-8");
//				logger.debug("md5 = " + result + "md5.len = " + result.length());
				
				input.close();
				output.close();
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			} 
		}
		return MD5;
	}
	
	
	
	/**
	 * 返回文件大小
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static int getFileSize(String fileName) throws IOException{
		int fileLen = 0;
		FileInputStream fis = new FileInputStream(fileName);
		fileLen = fis.available();
		fis.close();
		return fileLen;
	}
	
	public static int getErrorCode(String cause){
		//eg. e.getCause()=FsOptException(errCode:-19974, msg:other error, code = -1, docid = 114349209288706, errNum = -19974)
		int errorCode = -1;
		String startTag = "errCode:";
		String endTag = ", msg";
		int pos1 = cause.indexOf(startTag);
		int pos2 = cause.indexOf(endTag);
		errorCode = Integer.parseInt(cause.substring(pos1 + startTag.length(), pos2).trim());
		return errorCode;
	}
	
	
	// 字节数组到整数的转换
	public static long bytes2long(byte[] buffer, int offset, int length) {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.put(buffer, offset, length);
		bb.flip();
		return bb.getLong();

	}
	
	public static boolean existInArrays(int userType, int[] userTypes){
		for(int i=0; i<userTypes.length; i++){
			if(userType == userTypes[i]){
				return true;
			}
		}
		return false;
	}
}
