package com.test.dfs;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class MFS_SWAP_Cleaner extends MFSFileSystem {
	/**
	 * 根据过期时间字段expiredtime清理过期文件的接口
	 * 
	 * @return 返回被清理的文件的个数
	 */
	private final static Logger logger = Logger.getLogger(MFS_SWAP_Cleaner.class);

	/**
	 * clean outdated files
	 * 
	 * @param days  the additional days kept beyond expired date
	 * @return failed count files, need DBA attentions.
	 */
	public int clean(int days) {
		
		Calendar date = Calendar.getInstance();
		date.add(Calendar.DATE, -1 * days);
		final long time = date.getTimeInMillis();

		// sum up total data count cleaned.
		final AtomicInteger recordData = new AtomicInteger(0);
		// sum up total data size cleaned.
		final AtomicLong sizeData = new AtomicLong(0);
		// sum up total failed count.
		final AtomicInteger failedCount = new AtomicInteger(0);
		
		String threads = System.getProperty("swapcleaner.threads", "1");
		int loop = 1;
		try {
			loop = Integer.parseInt(threads);
		} catch (NumberFormatException e) {
		}
		
		List<Long> idList = newMFSManager.getDDBManager().queryOutdatedFiles(time);
		int count = 0;
		logger.info("begin to clean PhotoMD5 table, keep " + days + " days, ts = " + time);
		int round = 0;
		while (idList.size() != 0) {
			logger.info("Round " + ++round + " Found records " + idList.size() + " in PhotoMD5.");
			final AtomicInteger roundCount = new AtomicInteger(0);
			final Queue<Long> queue = new ConcurrentLinkedQueue<Long>();
			queue.addAll(idList);
			for (int i = 0; i < loop; i++) {
				new Thread(new Runnable() {
					public void run() {
						Long id = -1L;
						while ((id = queue.poll()) != null) {
							recordData.incrementAndGet();
							handleOutdatedFiles(id, time);
							int count = roundCount.incrementAndGet();
							if(count % 10000 == 0) {
								logger.info("Handle " + count +" OutDatedFiles");
							}
						}
					}

					private void handleOutdatedFiles(long id, long ts) {
						long[] size = new long[] { 0 };
						if (newMFSManager.getDDBManager().cleanRecord(id, ts, size)) {
							try {
								if (clearFile(id)) {
									logger.info("clean " + id + " success.");
								} else {
									logger.error("clean file " + id + " failed.");
									failedCount.incrementAndGet();
								}
							} catch (IOException e) {
								logger.error("clean file " + id + " failed.", e);
								failedCount.incrementAndGet();
							}
						}
						sizeData.addAndGet(size[0]);
					}
				}).start();
			}
			
			while (recordData.get() != idList.size()) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
				}
			}

			logger.info("#clean records size =  " + idList.size() + " really  docids " + recordData.get() + " success");
			count += recordData.get();
			idList = newMFSManager.getDDBManager().queryOutdatedFiles(time);
			recordData.set(0);
		}
		logger.info("clean photoMD5 table  total count: " + count + ", total size: " + sizeData.get());
		if (count == 0) {
			System.exit(2);
		}

		logger.info("begin to clean DocIncomplete table, keep " + days + " days, ts = " + time);
		List<Long> tmpID = newMFSManager.getDDBManager().queryOutdatedIncompleteFiles(time);
		count = 0;
		recordData.set(0);
		sizeData.set(0);
		round = 0;
		while (tmpID.size() > 0) {
			logger.info("Round " + ++round + " Found records " + tmpID.size() + " in DocIncomplete.");
			final Queue<Long> queue = new ConcurrentLinkedQueue<Long>();
			final AtomicInteger roundCount = new AtomicInteger(0);
			queue.addAll(tmpID);
			for (int i = 0; i < loop; i++) {
				new Thread(new Runnable() {
					public void run() {
						Long id = -1L;
						while ((id = queue.poll()) != null) {
							recordData.incrementAndGet();
							handleOutdatedIncompleteFiles(id, time);
							int count = roundCount.incrementAndGet();
							if(count % 10000 == 0) {
								logger.info("Handle " + count +" OutDatedInCompleteFiles");
							}
						}
					}

					private void handleOutdatedIncompleteFiles(Long id, long ts) {
						long[] size = new long[] { 0 };
						if (newMFSManager.getDDBManager().cleanIncompleteRecord(id, ts, size)) {
							try {
								if (clearFile(id)) {
									logger.info("clean tmp file " + id + " success.");
								} else {
									logger.error("clean tmp file " + id + " failed.");
									failedCount.incrementAndGet();
								}
							} catch (IOException e) {
								logger.error("clean tmp file " + id + " failed.", e);
								failedCount.incrementAndGet();
							}
						}
						sizeData.addAndGet(size[0]);					
					}
				}).start();
			}
			
			while (recordData.get() != tmpID.size()) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
				}
			}
			count += recordData.get();
			tmpID = newMFSManager.getDDBManager().queryOutdatedIncompleteFiles(time);
			recordData.set(0);
		}
		logger.info("clean DocIncomplete table total count: " + count);
		
		if (failedCount.get() > 0) {
			System.exit(3);
		}
		
		return failedCount.get();
	}

	/**
	 * Exit Code definition:
	 *  -1 arguments error
	 *  0 normal exit
	 *  1 DFS/DDB launch error
	 *  2 delete 0 file
	 *  3 some file delete failed with record cleaned.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length != 5) {
			logger.error("args error : MFSCleaner dfsMaster sdfsMdsAddrs MFSLogDir sdfsLogDir beforedays");
			System.exit(-1);
		}

		MFS_SWAP_Cleaner mfsManager = new MFS_SWAP_Cleaner();
		try {
			if (args.length >= 4) {
				mfsManager.launch(args[0], args[1], args[2], args[3]);
			}
			int days = 0;
			if ((days = Integer.parseInt(args[4])) < 3) {
				logger.error("can't clean less than  3 days");
				System.exit(-1);
			}
			mfsManager.clean(days);
		} catch (NumberFormatException nfe) {
			logger.error("args error : MFSCleaner dfsMaster sdfsMdsAddrs MFSLogDir sdfsLogDir beforedays", nfe);
			System.exit(-1);
		} catch (Exception e) {
			logger.error("MFS internal error", e);
			System.exit(1);
		}
		
		System.exit(0);
	}
}
