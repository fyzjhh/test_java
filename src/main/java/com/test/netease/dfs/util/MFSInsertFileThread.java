package com.test.dfs.util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSStream;
import com.netease.backend.sdfs.MFSFileSystem;

public class MFSInsertFileThread extends Thread {
	private static Logger logger = Logger.getLogger(MFSInsertFileThread.class);
	
	private int threadNo = -1;
	private int threadNum = -1;
	private CountDownLatch doneSignal;
	private MFSFileSystem manager = null;
	private String fileName = null;
	private int storageType = -1;
	private int userType = -1;
	private boolean isOverDue = false;
	private long overDueTime = -1;
	private boolean oldMFS;
	private boolean checkIfExists;
	private long[] docids;
	   
	
	public MFSInsertFileThread(int threadNo, int threadNum, CountDownLatch doneSignal, MFSFileSystem manager, String fileName, int storageType, int userType, boolean isOverDue, long overDueTime, boolean oldMFS,  boolean checkIfExists, long[] docids){
		this.threadNo = threadNo;
		this.threadNum = threadNum;
		this.manager = manager;
		this.fileName = fileName;
		this.storageType = storageType;
		this.userType = userType;
		this.isOverDue = isOverDue;
		this.overDueTime = overDueTime;
		this.docids = new long[this.threadNum];
		this.docids = docids;
		this.doneSignal = doneSignal;
	}
	
	public void run(){
		logger.info("Thread" + this.threadNo+ " insert file " + this.fileName+ " ......................");
		long docid = -1;
		//设置不同的sleepTime,方便查看是否在多线程执行
		FileInputStream fis = null;
		String md5 = null;
		MFSStream mfsStream = null;
		
		try {
			fis = new FileInputStream(fileName);
			md5 = getMD5(fileName);
			logger.debug("thread " + threadNo + "md5 = " + md5);
			mfsStream = new MFSStream(fis, fis.available(), md5, userType, isOverDue, overDueTime);
			mfsStream.setStorageType(storageType);
			
			docid = insertFileHead(md5, storageType, userType, isOverDue, overDueTime, oldMFS);
			logger.debug("docid = " + docid);

			docids[threadNo] = docid;
			appendFile(docid, mfsStream, checkIfExists);
			docid = insertFileEnd(docid, mfsStream);
			//最终的docid应该为insertFileEnd返回的docid
			//多线程上线不同的文件是docids[threadNo]与 docid肯定是相等的。
			//但是如果两个线程上传同一个文件，insertFileHead可以返回不同的md5
			//在insertFileEnd时，才会检查到md5已经存在，并返回之前的docid。
			if(docid != docids[threadNo]){
				logger.debug("docid = " + docid + " docid [" + threadNo + "] =" + docids[threadNo]);
			}
			docids[threadNo] = docid;	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			logger.info("Ending thread " + threadNo + "..........................");
				try {
					if(mfsStream != null){
						mfsStream.close();
					}
					if(fis != null){
						fis.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					doneSignal.countDown();
				}
		}
	}
	
	private long insertFileHead(String md5, int storageType, int userType, boolean isOverDue, long overDueTime, boolean oldMFS) throws Exception{
		logger.debug("insertFileHead.........................");

		MFSStream mfsStream = new MFSStream(null, -1, md5, userType, isOverDue, overDueTime);
		mfsStream.setStorageType(storageType);
		DocID docID = manager.insertFileHead(mfsStream, oldMFS);
		return docID.docId;
	}
	
	private DocID appendFile(long docid, MFSStream mfsStream,  boolean checkIfExists) throws IOException{
		return manager.appendFile(docid, mfsStream, checkIfExists);
	}
	
	private long insertFileEnd(long docid, MFSStream mfsStream) throws Exception{
		return manager.insertFileEnd(docid, mfsStream).docId;
	}

	
	private String getMD5(String fileName) throws Exception {
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
	
	
	private String getMD5HexString(byte[] MD5) throws Exception{
		
		return byteArrayToHexString(MD5);
	}
	
	private String byteArrayToHexString(byte[] b) {
		StringBuffer resultSb = new StringBuffer();
		for (int i = 0; i < b.length; i++) {
			resultSb.append(byteToHexString(b[i]));
		}
		return resultSb.toString();
	}
	
	private String byteToHexString(byte b) {
		int n = b;
		if (n < 0)
			n = 256 + n;
		int d1 = n / 16;
		int d2 = n % 16;
		return hexDigits[d1] + hexDigits[d2];
	}
	
	private final String[] hexDigits = { "0", "1", "2", "3", "4", "5",
		"6", "7", "8", "9", "A", "B", "C", "D", "E", "F" };
	
	
	  /**
	   *2B44EFD9" --> byte[]{0x2B, 0x44, 0xEF, 0xD9}
	   * @param src String
	   * @return byte[]
	   */
	  public byte[] MD5HexString2Bytes(String src) throws Exception{
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
