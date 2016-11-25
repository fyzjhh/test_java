package com.test.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
//import org.testng.Assert;

import com.netease.cloud.services.nos.NosClient;
import com.netease.cloud.services.nos.model.UploadPartRequest;
import com.netease.cloud.services.nos.model.UploadPartResult;

public class MultiPartThread extends Thread {
private static Logger logger = Logger.getLogger(MultiPartThread.class);
	
	private int threadNo = -1;
	private CountDownLatch doneSignal;
	private InputStream is = null;
	public UploadPartResult uploadPartResult = null;
	public UploadPartRequest uploadPartRequest = null;
	private NosClient clinet = null;
	
	public MultiPartThread(int threadNo, NosClient clinet, UploadPartRequest request, CountDownLatch doneSignal){
		this.threadNo = threadNo;
		this.uploadPartRequest = request;
		this.doneSignal = doneSignal;
		this.is = request.getInputStream();
		this.clinet = clinet;
	}
	
	public void run(){
		logger.info("Thread" + this.threadNo+ " insert data block ......................");
		
		try {
			uploadPartResult = clinet.uploadPart(uploadPartRequest);
		} catch (Exception e) {
			logger.error("Caught Exception:", e);
		}finally{
			logger.info("Ending thread " + threadNo + "..........................");
				try {
					if(this.is != null){
						this.is.close();
					}
				} catch (IOException e) {
					logger.error("Caught Exception:", e);
				}finally{
					doneSignal.countDown();
				}
		}
	}
	
}
