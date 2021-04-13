package com.test.netease.dfs;

public class SqlStatement {
  
	public static final String SN_QUERY_BY_SNNAME = "Select SNName, IsAlive, KernalNum From SNInfo Where SNNAME = ?";
	
	public static final String SN_QUERY = "Select SNName, IsAlive, KernalNum From SNInfo";

	public static final String SN_ADD = "Insert into SNInfo(SNName,IsAlive,KernalNum) values(?,?,?)";

	public static final String SN_DELETE = "Delete from SNInfo Where SNName = ?";

	public static final String SN_UPDATE_STATE = "Update SNInfo Set IsAlive = ? Where SNName = ?";

	public static final String UPDATE_DISKSTAGE = "Update DiskInfo Set DiskStage = ? Where DiskID = ?";

	public static final String UPDATE_DISKSTATE = "Update DiskInfo Set Status = ? Where DiskID = ?";

	public static final String ADD_DISK = "Insert into DiskInfo( DiskLocation, LocalPath, SeriaNum,  Status, DiskStage) values(?,?,?,?,?)";

	public static final String DELETE_DISK = "Delete From DiskInfo Where DiskID = ?";

	public static final String ADD_BUCKET = "Insert Into BucketInfo(BID,DPID,MaxID,UserType,isAlive) values(?,?,?,?,?)";

	public static final String DISK_QUERY = "Select DiskID,DiskLocation,LocalPath,SeriaNum,Status,DiskStage From DiskInfo";

	public static final String DP_ADD = "Insert Into DPInfo(DPID,Weight) values(?,?)";

	public static final String SELECT_DP = "Select DPID, Weight From DPInfo";
	
	public static final String ADD_DISK_DP ="Insert into DPMapping(DPID,DiskID) values(?,?)";

	public static final String DELETE_DP = "Delete From DPInfo Where DPID = ?";

	public static final String DELETE_DISK_DP ="Delete From DPMapping Where DPID = ? and DiskID = ?";

	public static final String DELETE_DISK_DP_BY_DP ="Delete From DPMapping Where DPID = ?";

	public static final String ADD_COPY = "Insert Into BucketCopy(BID,DiskID,Status) values(?,?,?)";

	public static final String DELETE_BUCKET = "Delete From BucketInfo Where BID = ?";

	public static final String SELECT_BUCKET_BY_BID = "Select BID , DPID , MaxID, UserType From BucketInfo where BID=?";
	
	public static final String SELECT_BUCKET_BY_BPID = "Select BID , DPID , MaxID, UserType From BucketInfo where DPID=?";

	public static final String SELETE_BUCKET = "Select BID , DPID , isAlive , MaxID, UserType From BucketInfo";

	public static final String UPDATE_BUCKET_DPID = "Update BucketInfo set DPID = ? where BID = ? ";

	public static final String DELETE_BUCKETCOPY = "Delete from BucketCopy where BID = ? and DiskID = ?";
	
	public static final String DELETE_BUCKETCOPY_BY_BUCKET = "Delete from BucketCopy where BID = ?";

	public static final String UPDATE_BUCKET_STATE = "Update BucketInfo set IsAlive = ? where BID = ? ";

	public static final String UPDATE_BUCKETCOPY_STATUS = "Update BucketCopy set Status = ? where BID = ?  and DiskID = ?";

	public static final String SELECT_ADMIN = "Select Account , Password , ACL From AdminInfo";

	public static final String INSERT_ADMIN = "Insert into AdminInfo(Account, Password, ACL) values(?,?,?)";

	public static final String UPDATE_ADMIN_PASSWORD = "Update admininfo set Password = ? where Account = ? ";

	public static final String DELETE_ADMIN = "Delete From AdminInfo where Account = ? ";

	public static final String UPDATE_ADMIN = "Update AdminInfo set ACL = ? where Account = ? ";

	public static final String INSERT_WARNNING ="Insert into warning(Reporter,Content,Type,Count) values(?,?,?,?)";

	public static final String DELETE_WARNNING = "Delete From warning where ID = ?";

	public static final String SELETE_WARNNING = "Select ID , Reporter, Content, Type ,Count From warning";

	public static final String UPDATE_WARNNING = "Update Warning set Count = ? where ID = ? ";

	public static final String SELETE_SYNC_BID = "Select distinct BID  From syncinfo";

	public static final String SELETE_SYNC_BY_BID = "Select Dest,OpType,SNSyncMode,Stage,IsSucc  From syncinfo";
	
	public static final String UPDATE_SYNC = "Update syncinfo set Stage = ?  where BID = ? and Dest = ?";

	public static final String DISK_BUCKET_QUERY = "Select BID  from BucketCopy where DiskID = ?";

	public static final String DISK_DP_QUERY = "Select DPID From DPMapping where DiskID = ?";

	public static final String DISKID_QUERY_BY_DISKLOCATION = "Select DiskID From DiskInfo where DiskLocation = ?";

	public static final String DP_DISK_QUERY = "Select DiskID From DPMapping where DPID = ?";

	public static final String BUCKET_DISK_QUERY = "Select DiskID  from BucketCopy where Bid = ?";

	public static final String DISK_QUERY_DISKID = "Select DiskID,DiskLocation,LocalPath,SeriaNum,Status,DiskStage From DiskInfo where DiskID=?";
	
	public static final String BUCKETCOPY_QUERY_DISKID_BID = "Select status From BucketCopy where BID=? and DiskID=?";
	
	public static final String DISK_DELETE = "Delete from DiskInfo where DiskLocation like ?";

	public static final String UPDATE_BUCKET_TYPE = "Update BucketInfo set UserType = ? where BID = ?";

	public static final String ADD_SYNC = "Insert into syncinfo(BID,Dest,OpType,SNSyncMode,Stage,IsSucc) values(?,?,?,?,?,?)";

	public static final String DELETE_SYNC = "Delete from syncinfo where BID = ? and Dest = ?";

	public static final String UPDATE_DP_WEIGHT = "Update DPInfo set Weight = ? where dpid = ?";

	public static final String UPDATE_DISK_SERIANUM ="Update DiskInfo set SeriaNum = ? where DiskID = ?";

	public static final String ADD_IP = "Insert into IPInfo(IP) values(INET_ATON(?))";

	public static final String SELECT_IP = "Select INET_NTOA(IP) from IPInfo";

	public static final String DELETE_IP = "Delete from IPInfo where IP = INET_ATON(?)";

	public static final String SELECT_ADMIN_BY_ACCOUNT = "Select Account , Password , ACL From AdminInfo where Account = ?";

	public static final String UPDATE_BUCKETINFO_MAXID = "Update BucketInfo set MaxID = ? where BID = ?";

	public static final String UPDATE_ALLOC_BUCKETINFO_MAXID = "Select maxID  from BucketInfo where bid =? for update";

	public static final String SELECT_DP_BY_DPID = "Select Weight From DPInfo where dpid = ?";
	
	public static final String DISK_QUERY_DISKLOCATION = "Select DiskID,DiskLocation,LocalPath,SeriaNum,Status,DiskStage From DiskInfo where DiskLocation=?";
	
	public static final String QUERY_DISKID = "Select DiskID from DiskInfo where DiskLocation = ?";
}
