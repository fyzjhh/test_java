package com.test.dfs.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.testng.Assert;

import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSStream;
import com.netease.backend.sdfs.MFSFileSystem;

public class AppendFileThread extends Thread{
	private static Logger logger = Logger.getLogger(AppendFileThread.class);
	
	private int threadNo = -1;
	private int threadNum = -1;
	private CountDownLatch doneSignal;
	private MFSFileSystem manager = null;
	private long docid = -1;

	private MFSStream mfsStream = null;
	private boolean checkIfExists = false;
	   
	
	public AppendFileThread(int threadNo, int threadNum, CountDownLatch doneSignal, MFSFileSystem manager, long docid, MFSStream mfsStream, boolean checkIfExists){
		this.threadNo = threadNo;
		this.threadNum = threadNum;
		this.manager = manager;
		this.docid = docid;
		this.doneSignal = doneSignal;
		this.mfsStream = mfsStream;
	}
	
	public void run(){
		logger.info("Thread" + this.threadNo+ " insert data block ......................");
		
		try {
			DocID docID = appendFile(docid, mfsStream, checkIfExists);
			logger.debug("thread " + threadNo + " offset =" + mfsStream.getOffset());
			Assert.assertTrue(docID.docId == docid);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			logger.info("Ending thread " + threadNo + "..........................");
				try {
					if(this.mfsStream != null){
						this.mfsStream.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error("Caught Exception:", e);
				}finally{
					doneSignal.countDown();
				}
		}
	}
	
	
	private DocID appendFile(long docid, MFSStream mfsStream,  boolean checkIfExists) throws IOException{
		return manager.appendFile(docid, mfsStream, checkIfExists);
	}
}
