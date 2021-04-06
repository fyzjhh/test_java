package com.test.dfs;

/**
 * MFS数据库管理器
 * 
 * @author lxh 2009/7/28
 * 
 */
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.netease.backend.dfs.DDBManager;
import com.netease.backend.dfs.DFSException;
import com.netease.backend.dfs.DFSManager;
import com.netease.backend.dfs.util.Util;
import com.netease.backend.mfs.MFSRecord;

public class MFSDBManager extends DDBManager {
	// 调试日志
	private final static Logger logger = Logger.getLogger(MFSDBManager.class);

	public final int INVALID_COUNT = -10000;

	/**
	 * 默认构造函数
	 */
	public MFSDBManager(String ddburl) {
		super();
		dbstr = ddburl;
	}

	/**
	 * 初始化函数
	 * 
	 * @return 是否成功
	 * @throws IOException
	 */
	public boolean init() throws Exception {

		if (dbstr != null && dbstr.startsWith("jdbc:netease:lightweight:"))
			Class.forName("com.netease.ddb.lightweight.LWDriver");
		else
			return super.init();
		return true;
	}

	/**
	 * MD5码查询函数
	 * 
	 * @param md5 需查询的md5码
	 * @return 该md5码对应的文件列表
	 * @throws IOException
	 */
	public ArrayList<MFSRecord> queryMD5(byte[] md5) throws IOException {

		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);

		Connection dbCon = null;
		ResultSet rs = null;
		PreparedStatement select = null;
		long PhotoID = DFSManager.INVALID_DOCID;
		MFSRecord record = null;
		ArrayList<MFSRecord> recordList = new ArrayList<MFSRecord>();
		int count = INVALID_COUNT;
		int userType = 0;
		long size = 0;
		long expiredtime = -1;
		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("select PhotoID,userType,count,size,expirytime from PhotoMD5 where md5High=? and md5Low=?");
			dbCon = DriverManager.getConnection(dbstr);
			select = dbCon.prepareStatement(sb.toString());
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			rs = select.executeQuery();

			while (rs.next()) {
				// 找到相关记录
				PhotoID = rs.getLong("PhotoID");
				count = rs.getInt("count");
				userType = rs.getInt("userType");
				size = rs.getLong("size");
				expiredtime = rs.getLong("expirytime");
				record = new MFSRecord(PhotoID, userType, count, md5high, md5low, size);
				record.setExpiredtime(expiredtime);
				recordList.add(record);
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}

		return recordList;
	}
	/**
	 * 文件查询函数
	 * 
	 * @param PhotoID 需查询的文件id
	 * @return UFSRecord, 包含该文件的MD5值
	 * @throws IOException
	 */
	public MFSRecord queryPhotoID(long PhotoID) throws IOException {
		return queryPhotoID(PhotoID, true);
	}
	/**
	 * 文件查询函数 不管文件count计数
	 * 
	 * @param PhotoID 需查询的文件id
	 * @return UFSRecord, 包含该文件的MD5值
	 * @throws IOException
	 */
	public MFSRecord queryPhotoIDWithoutCount(long PhotoID) throws IOException {
		return queryPhotoID(PhotoID, false);
	}
	/**
	 * 文件查询函数
	 * 
	 * @param PhotoID 需查询的文件id
	 * @return UFSRecord, 包含该文件的MD5值
	 * @throws IOException
	 */
	private MFSRecord queryPhotoID(long PhotoID, boolean checkForever) throws IOException {
		long md5high = 0;
		long md5low = 0;
		Connection dbCon = null;
		ResultSet rs = null;
		PreparedStatement select = null;
		int count = INVALID_COUNT;
		MFSRecord record = null;
		int userType = 0;
		long size = 0;
		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("select count,userType, md5high,md5low,size from PhotoMD5 where PhotoID=? "
					+ (checkForever ? " and count > 0" : ""));
			dbCon = DriverManager.getConnection(dbstr);
			select = dbCon.prepareStatement(sb.toString());
			select.setLong(1, PhotoID);
			rs = select.executeQuery();

			while (rs.next()) {
				// 找到相关记录
				count = rs.getInt("count");
				md5high = rs.getLong("md5high");
				md5low = rs.getLong("md5low");
				userType = rs.getInt("userType");
				size = rs.getLong("size");
				record = new MFSRecord(PhotoID, userType, count, md5high, md5low, size);
				break;
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return record;
	}

	/**
	 * 根据MD5码增加文件引用计数，
	 * 
	 * @param md5 需更新记录的md5码
	 * @return 如果记录不存在返回INVALID_DOCID，否则返回PhotoID
	 * @throws IOException
	 */
	public long incPhotoRef(int userType, byte[] md5) throws IOException {
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);

		Connection dbCon = null;
		int result = 0;
		ResultSet rs = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		long PhotoID = DFSManager.INVALID_DOCID;
		int count = 0;

		try {
			StringBuffer selectSB = new StringBuffer(512);
			selectSB
					.append("select PhotoID,count from PhotoMD5 where md5High=? and md5Low=? and userType =? for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			select.setInt(3, userType);
			rs = select.executeQuery();

			while (rs.next()) {
				PhotoID = rs.getLong("PhotoID");
				count = rs.getInt("count");
				// logger.info(PhotoID);
				break;
			}
			if (PhotoID != DFSManager.INVALID_DOCID) {
				// 找到相关记录，更新引用数
				StringBuffer updateSB = new StringBuffer(512);

				if (count < 0)
					count = 1;
				else
					count++;

				updateSB.append("update PhotoMD5 set count=? where PhotoID = ? and md5High = ?");

				update = dbCon.prepareStatement(updateSB.toString());
				update.setInt(1, count);
				update.setLong(2, PhotoID);
				update.setLong(3, md5high);
				result = update.executeUpdate();
				if (result == 0) {
					logger.error("Record: " + PhotoID + " update failed! It can not happen!");
					PhotoID = DFSManager.INVALID_DOCID;
					throw new IOException("record found, but update failed!");
				}
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				PhotoID = DFSManager.INVALID_DOCID;
			}
		}
		return PhotoID;
	}

	/**
	 * 根据photoID增加文件引用计数，
	 * 
	 * @param PhotoID 需更新记录的photoID
	 * @return 如果记录不存在抛异常
	 * @throws IOException
	 */
	public int incIDRef(long PhotoID) throws IOException {

		Connection dbCon = null;
		int result = 0;
		PreparedStatement update = null;

		try {

			StringBuffer updateSB = new StringBuffer(512);
			updateSB.append("update PhotoMD5 set count=count+1 where PhotoID = ? ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			update = dbCon.prepareStatement(updateSB.toString());
			update.setLong(1, PhotoID);
			result = update.executeUpdate();
			if (result == 0) {
				logger.error("Record: " + PhotoID + " update failed! ");
				throw new IOException("record not found!");
			}

		} catch (SQLException e) {
			// e.printStackTrace();
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {

				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return result;
	}

	/**
	 * 根据MD5码减少文件引用计数，
	 * 
	 * @param PhotoID 需更新记录的md5码
	 * @return UFSRecord, 包含该文件的引用计数和MD5值
	 * @throws IOException
	 */
	public MFSRecord decPhotoRef(long PhotoID) throws IOException {

		Connection dbCon = null;
		ResultSet rs = null;
		int result = 0;
		PreparedStatement select = null;
		PreparedStatement update = null;
		MFSRecord record = null;
		int count = INVALID_COUNT;
		long md5high = 0;
		long md5low = 0;
		int userType = 0;
		long size = 0;
		long expiredtime = 0;
		try {
			StringBuffer selectSB = new StringBuffer(512);
			selectSB
					.append("select userType,count,md5high,md5low,size,expirytime from PhotoMD5 where PhotoID=? for update");

			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, PhotoID);
			rs = select.executeQuery();
			while (rs.next()) {
				count = rs.getInt("count");
				md5high = rs.getLong("md5high");
				md5low = rs.getLong("md5low");
				userType = rs.getInt("userType");
				size = rs.getLong("size");
				expiredtime = rs.getLong("expirytime");
				break;
			}
			if (count == INVALID_COUNT) { // 记录没有找到，重复删除？老文件删除？
				logger.info("Record: " + PhotoID + " not found!");
				record = new MFSRecord(PhotoID, userType, count, md5high, md5low, size);
			} else { // 减少引用计数

				if (count < 1)
					count = 0;
				else
					count--;

				StringBuffer updateSB = new StringBuffer(512);
				updateSB.append("update PhotoMD5 set count=? where PhotoID = ? and md5high = ?");
				update = dbCon.prepareStatement(updateSB.toString());
				update.setInt(1, count);
				update.setLong(2, PhotoID);
				update.setLong(3, md5high);
				result = update.executeUpdate();
				if (result == 0) {
					logger.error("Record: " + PhotoID + " update failed! It can not happen!");
					throw new IOException("record found, but update failed!");
				}
				record = new MFSRecord(PhotoID, userType, count, md5high, md5low, size);
				record.setExpiredtime(expiredtime);
			}

		} catch (SQLException e) {
			// e.printStackTrace();
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {

				if (select != null) {
					select.close();
				}
				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				record.setCount(-10);
			}
		}

		return record;
	}

	/**
	 * 往photoMD5表加入新的纪录 专用于临时文件 expiredtime字段取值-1时表示未设置过期时间
	 * 
	 * @param PhotoID 文件ID
	 * @param userType 文件用户定义类型
	 * @param md5 md5码
	 * @param size 文件大小
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public int insertPhotoRecord4Temp(long PhotoID, int userType, byte[] md5, long size, long expiredtime)
			throws IOException {
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		Connection dbCon = null;
		int result = 0;
		PreparedStatement prepare = null;

		try {
			StringBuffer sb = new StringBuffer(512);
			sb
					.append("insert into PhotoMD5(PhotoID, userType, md5high, md5low, count, size, expirytime) values( ?,  ?, ?, ?, ?, ?,?) ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			prepare = dbCon.prepareStatement(sb.toString());
			prepare.setLong(1, PhotoID);
			prepare.setInt(2, userType);
			prepare.setLong(3, md5high);
			prepare.setLong(4, md5low);
			prepare.setInt(5, 0);
			prepare.setLong(6, size);
			prepare.setLong(7, expiredtime);
			result = prepare.executeUpdate();

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
				
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				result = 0;
			}
		}
		return result;
	}
	/**
	 * 文件查询函数
	 * 
	 * @param PhotoID 需查询的文件id
	 * @return UFSRecord, 包含该文件的MD5值
	 * @throws IOException
	 */
	public MFSRecord queryInComplete(long PhotoID) throws IOException {
		long md5high = 0;
		long md5low = 0;
		Connection dbCon = null;
		ResultSet rs = null;
		PreparedStatement select = null;
		int count = INVALID_COUNT;
		MFSRecord record = null;
		int userType = 0;
		long size = 0;
		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("select userType, md5high, md5low, size, createtime, expirytime  from DocIncomplete where id=? ");
			dbCon = DriverManager.getConnection(dbstr);
			select = dbCon.prepareStatement(sb.toString());
			select.setLong(1, PhotoID);
			rs = select.executeQuery();
			while (rs.next()) {
				md5high = rs.getLong("md5high");
				md5low = rs.getLong("md5low");
				userType = rs.getInt("userType");
				size = rs.getLong("size");
				record = new MFSRecord(PhotoID, userType, count, md5high, md5low, size);
				break;
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return record;
	}
	
	public long queryInComplete(byte[] md5, int userType, boolean overall_distinct) throws IOException {
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		Connection dbCon = null;
		ResultSet rs = null;
		PreparedStatement select = null;
		long PhotoID = DFSManager.INVALID_DOCID;
		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("select id from DocIncomplete where md5High=? and md5Low=? ");
			sb.append(overall_distinct ? (" and usertype = ? ") : "");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			select = dbCon.prepareStatement(sb.toString());
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			if (overall_distinct) {
				select.setInt(3, userType);
			}
			rs = select.executeQuery();
			while (rs.next()) {
				PhotoID = rs.getLong("id");
				break;
			}
		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}

		return PhotoID;
	}
	
	/**
	 * 往InComplete表加入新的纪录
	 * 
	 * @param PhotoID 文件ID
	 * @param userType 文件用户定义类型
	 * @param md5 md5码
	 * @param size 文件大小
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public int insertInComplete(long PhotoID, int userType, byte[] md5, long size) throws IOException {
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		Connection dbCon = null;
		int result = 0;
		PreparedStatement prepare = null;

		try {
			StringBuffer sb = new StringBuffer(512);
			sb
					.append("insert into DocIncomplete(id, userType, md5high, md5low, size, createtime, expirytime) values( ?, ?, ?, ?, ?, ?, ?)");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			prepare = dbCon.prepareStatement(sb.toString());
			prepare.setLong(1, PhotoID);
			prepare.setInt(2, userType);
			prepare.setLong(3, md5high);
			prepare.setLong(4, md5low);
			prepare.setLong(5, size);
			prepare.setLong(6, System.currentTimeMillis());
			prepare.setLong(7, 0);
			result = prepare.executeUpdate();

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				result = 0;
			}
		}
		return result;
	}

	/**
	 * 往photoMD5表加入新的纪录
	 * 
	 * @param PhotoID 文件ID
	 * @param userType 文件用户定义类型
	 * @param md5 md5码
	 * @param size 文件大小
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public int insertPhotoRecord(long PhotoID, int userType, byte[] md5, long size) throws IOException {
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		Connection dbCon = null;
		int result = 0;
		PreparedStatement prepare = null;

		try {
			StringBuffer sb = new StringBuffer(512);
			sb
					.append("insert into PhotoMD5(PhotoID, userType, md5high, md5low, count, size, expirytime) values( ?, ?, ?, ?, ?, ?, ?) ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			prepare = dbCon.prepareStatement(sb.toString());
			prepare.setLong(1, PhotoID);
			prepare.setInt(2, userType);
			prepare.setLong(3, md5high);
			prepare.setLong(4, md5low);
			prepare.setInt(5, 1);
			prepare.setLong(6, size);
			prepare.setLong(7, -1);
			result = prepare.executeUpdate();

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				result = 0;
			}
		}
		return result;
	}

	/**
	 * 删除InComplete表中上传的的纪录
	 * 
	 * @param PhotoID 文件ID
	 * @param md5high md5码高位值
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public int deleteInComplete(long PhotoID) throws IOException {

		Connection dbCon = null;
		int result = 0;
		PreparedStatement prepare = null;

		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("Delete from DocIncomplete where id =? ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			prepare = dbCon.prepareStatement(sb.toString());
			prepare.setLong(1, PhotoID);
			result = prepare.executeUpdate();

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				result = 0;
			}
		}

		return result;
	}

	/**
	 * 删除photoMD5表中的纪录
	 * 
	 * @param PhotoID 文件ID
	 * @param md5high md5码高位值
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public int deletePhotoRecord(long PhotoID, long md5high) throws IOException {

		Connection dbCon = null;
		int result = 0;
		PreparedStatement prepare = null;

		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("Delete from PhotoMD5 where PhotoID =? and  md5High = ? and count = 0 ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			prepare = dbCon.prepareStatement(sb.toString());
			prepare.setLong(1, PhotoID);
			prepare.setLong(2, md5high);
			result = prepare.executeUpdate();

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				result = 0;
			}
		}

		return result;
	}

	/**
	 * 强制删除photoMD5表中的纪录，慎用！
	 * 
	 * @param PhotoID 文件ID
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public int forceDelPhotoRecord(long PhotoID) throws IOException {

		Connection dbCon = null;
		int result = 0;
		PreparedStatement prepare = null;

		try {
			StringBuffer sb = new StringBuffer(512);
			sb.append("Delete from PhotoMD5 where PhotoID =?");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			prepare = dbCon.prepareStatement(sb.toString());
			prepare.setLong(1, PhotoID);
			result = prepare.executeUpdate();

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				result = 0;
			}
		}

		return result;
	}

	/**
	 * 
	 * @throws IOException
	 * @throws SQLException
	 */
	public Connection startTransaction() throws IOException, SQLException {
		Connection dbConn = null;
		dbConn = DriverManager.getConnection(dbstr);
		return dbConn;
	}

	/**
	 * 根据photoID增加指定数量的文件引用计数 只针对测试
	 * 
	 * @param PhotoID 需更新记录的photoID
	 * @return 如果记录不存在抛异常
	 * @throws IOException
	 */
	public boolean incIDRef(long PhotoID, int count) throws IOException {
		Connection dbCon = null;
		PreparedStatement update = null;

		try {
			StringBuffer updateSB = new StringBuffer(512);
			updateSB.append("update PhotoMD5 set count=count+? where PhotoID = ? ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			update = dbCon.prepareStatement(updateSB.toString());
			update.setInt(1, count);
			update.setLong(2, PhotoID);
			if (update.executeUpdate() == 0) {
				logger.error("Record: " + PhotoID + " update failed! ");
				return false;
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {

				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return true;
	}

	/**
	 * @deprecated 尝试往photoMD5表加入新的纪录, 需要先判断MD5值是否存在.
	 * 
	 * @param PhotoID 文件ID
	 * @param userType 用户定义文件类型
	 * @param md5 md5码
	 * @param size 文件大小
	 * @return 0 for failed, and others for successful
	 * @throws IOException
	 */
	public long tryInsertPhotoRecord(long PhotoID, int userType, byte[] md5, long size) throws IOException {
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement prepare = null;

		long finalID = DFSManager.INVALID_DOCID;
		int count = 0;

		try {
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);

			String selectStr = "select PhotoID,count from PhotoMD5 where md5High=? and md5Low=? and userType = ?  for update";
			select = dbCon.prepareStatement(selectStr);
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			select.setInt(3, userType);
			ResultSet rs = select.executeQuery();
			while (rs.next()) {
				finalID = rs.getLong("PhotoID");
				count = rs.getInt("count");
				break;
			}

			if (finalID != DFSManager.INVALID_DOCID) {
				// 找到相关记录，更新引用数
				if (count < 0)
					count = 1;
				else
					count++;

				String str = "update PhotoMD5 set count=? where PhotoID = ? and md5High = ?";
				prepare = dbCon.prepareStatement(str);
				prepare.setInt(1, count);
				prepare.setLong(2, finalID);
				prepare.setLong(3, md5high);
			} else {
				finalID = PhotoID;

				String str = "insert into PhotoMD5(PhotoID, userType, md5high, md5low, count, size, updatetime) values( ?, ?, ?, ?, ?, ?, ?)";
				prepare = dbCon.prepareStatement(str);
				prepare.setLong(1, finalID);
				prepare.setInt(2, userType);
				prepare.setLong(3, md5high);
				prepare.setLong(4, md5low);
				prepare.setInt(5, 1);
				prepare.setLong(6, size);
				prepare.setLong(7, System.currentTimeMillis());
			}
			prepare.executeUpdate();

			return finalID;
		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (prepare != null) {
					prepare.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
				return DFSManager.INVALID_DOCID;
			}
		}
	}

	/**
	 * @deprecated 更新photomd5表的updatetime字段
	 * @param docId 需要更新的docId
	 * @return 更新成功返回1
	 * @throws IOException 更新异常
	 */
	public int refreshModifiedTime(long docId) throws IOException {

		Connection dbCon = null;
		int result = 0;
		PreparedStatement update = null;

		try {

			StringBuffer updateSB = new StringBuffer(512);
			updateSB.append("update PhotoMD5 set updatetime = " + System.currentTimeMillis() + " where PhotoID = ? ");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			update = dbCon.prepareStatement(updateSB.toString());
			update.setLong(1, docId);
			result = update.executeUpdate();
			if (result == 0) {
				logger.error("Record: " + docId + " update failed! ");
				throw new IOException("record not found!");
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {

				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return result;
	}

	/**
	 * @deprecated
	 * @param docId
	 * @param md5
	 * @return
	 * @throws IOException
	 */
	public int refreshModifiedTime(long docId, byte[] md5) throws IOException {

		Connection dbCon = null;
		int result = 0;
		PreparedStatement update = null;
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		try {

			StringBuffer updateSB = new StringBuffer(512);
			updateSB.append("update PhotoMD5 set updatetime = " + System.currentTimeMillis()
					+ " where  md5High=? and md5Low=? for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			update = dbCon.prepareStatement(updateSB.toString());
			update.setLong(1, md5high);
			update.setLong(1, md5low);
			result = update.executeUpdate();
			if (result == 0) {
				logger.error("Record: " + docId + " update failed! ");
				throw new IOException("record not found!");
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {

				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return result;
	}
   /**
	 * @param md5
	 * @param userType
	 * @param newExpiredTime
	 * @return
	 * @throws IOException
	 */
	public long updateFileLevel(byte[] md5, int userType, long newExpiredTime) throws IOException {

		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		ResultSet rs = null;
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		try {

			StringBuffer selectSB = new StringBuffer(512);
			selectSB
					.append("select expirytime,count from PhotoMD5 where md5High=? and md5Low=? and userType =?  for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			select.setLong(3, userType);
			rs = select.executeQuery();
			while (rs.next()) {
				long expiredTime = rs.getLong(1);
				int count = rs.getInt(2);
				if(count > 1)
					break;
				//如果当前文件是永久存储文件，且引用计数为1，则将引用计数置为0，同时设置过期时间；
				if (count == 1) {
					update = dbCon
							.prepareStatement("update PhotoMD5 set expirytime=? ,count=0  where md5High=? and md5Low=? and userType =? ");
					update.setLong(1, newExpiredTime);
					update.setLong(2, md5high);
					update.setLong(3, md5low);
					update.setLong(4, userType);
					int row = update.executeUpdate();
					if (row > 0) {
						return newExpiredTime;
					} else {
						return expiredTime;
					}
				}
				if (newExpiredTime > 0 && newExpiredTime > expiredTime) {
					update = dbCon
							.prepareStatement("update PhotoMD5 set expirytime=? where md5High=? and md5Low=? and userType =? ");
					update.setLong(1, newExpiredTime);
					update.setLong(2, md5high);
					update.setLong(3, md5low);
					update.setLong(4, userType);
					int row = update.executeUpdate();
					if (row > 0) {
						return newExpiredTime;
					} else {
						return expiredTime;
					}
				} else {
					return expiredTime;
				}
			}
			return -1;
		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
	}
	public long updateExpiredTime(byte[] md5, int userType, long newExpiredTime) throws IOException {

		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		ResultSet rs = null;
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		try {

			StringBuffer selectSB = new StringBuffer(512);
			selectSB
					.append("select expirytime,count from PhotoMD5 where md5High=? and md5Low=? and userType =?  for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			select.setLong(3, userType);
			rs = select.executeQuery();
			while (rs.next()) {
				long expiredTime = rs.getLong(1);
				if (newExpiredTime > 0 && newExpiredTime > expiredTime) {
					update = dbCon
							.prepareStatement("update PhotoMD5 set expirytime=? where md5High=? and md5Low=? and userType =? ");
					update.setLong(1, newExpiredTime);
					update.setLong(2, md5high);
					update.setLong(3, md5low);
					update.setLong(4, userType);
					int row = update.executeUpdate();
					if (row > 0) {
						return newExpiredTime;
					} else {
						return expiredTime;
					}
				} else {
					return expiredTime;
				}
			}
			return -1;
		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
	}
	
	/**
	 * 更新未完成文件的记录的过期时间字段
	 * 
	 * @param docid 文件ID
	 * @param newExpiredTime 新的过期时间
	 * @return
	 * @throws IOException
	 */
	public long updateIncompleteFileExpiredTime(long docid, long newExpiredTime) throws IOException {

		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		ResultSet rs = null;
		try {

			StringBuffer selectSB = new StringBuffer(512);
			selectSB
					.append("select expirytime from DocIncomplete where id=?  for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, docid);
			rs = select.executeQuery();
			while (rs.next()) {
				long expiredTime = rs.getLong(1);
				if (newExpiredTime > 0 && newExpiredTime > expiredTime){
					update = dbCon
							.prepareStatement("update DocIncomplete set expirytime=? where id=? ");
					update.setLong(1, newExpiredTime);
					update.setLong(2, docid);
					int row = update.executeUpdate();
					if (row > 0) {
						return newExpiredTime;
					} else {
						return expiredTime;
					}
				} else {
					return expiredTime;
				}
			}
			return -1;
		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
	}

	public long updateExpiredTimeAndGetId(byte[] md5, int userType, long newExpiredTime) throws IOException {
		long docid = DFSManager.INVALID_DOCID;
		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		ResultSet rs = null;
		long md5high = Util.bytes2long(md5, 0, 8);
		long md5low = Util.bytes2long(md5, 8, 8);
		try {

			StringBuffer selectSB = new StringBuffer(512);
			selectSB
					.append("select expirytime,PhotoID,count from PhotoMD5 where md5High=? and md5Low=? and userType =? for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, md5high);
			select.setLong(2, md5low);
			select.setLong(3, userType);
			rs = select.executeQuery();
			while (rs.next()) {
				long expiredTime = rs.getLong(1);
				//int count = rs.getInt(3);
				// 如果当前文件是永久存储文件，且引用计数为1，则将引用计数置为0，同时设置过期时间；
				// if (count == 1) {
				// update = dbCon
				// .prepareStatement("update PhotoMD5 set expirytime=? ,count=0
				// where md5High=? and md5Low=? and userType =? ");
				// update.setLong(1, newExpiredTime);
				// update.setLong(2, md5high);
				// update.setLong(3, md5low);
				// update.setLong(4, userType);
				// update.executeUpdate();
				//				}
				//count == 0 && 
				if (newExpiredTime > 0 && newExpiredTime > expiredTime) {
					update = dbCon
							.prepareStatement("update PhotoMD5 set expirytime =?   where md5High=? and md5Low=? and userType =? ");
					update.setLong(1, newExpiredTime);
					update.setLong(2, md5high);
					update.setLong(3, md5low);
					update.setLong(4, userType);
					update.executeUpdate();
				}
				return rs.getLong(2);
			}

		} catch (SQLException e) {
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}
				if (update != null) {
					update.close();
				}
				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return docid;
	}

	/**
	 * 查询所有过期的文件ID
	 * 
	 * @param timeLine 过期时刻
	 * @return 过期文件ID集合
	 */
	public List<Long> queryOutdatedFiles(long timeLine) {
		List<Long> idList = new ArrayList<Long>();
		Connection dbCon = null;
		ResultSet result = null;
		PreparedStatement query = null;

		try {

			StringBuffer querySB = new StringBuffer(512);
			querySB.append("select PhotoID from PhotoMD5 where expirytime<? and expirytime>0 and count=0");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			query = dbCon.prepareStatement(querySB.toString());
			query.setLong(1, timeLine);
			query.setFetchSize(100000); // 一次最多取10w条记录，防止记录过多，内存爆掉
			result = query.executeQuery();
			while (result.next()) {
				idList.add(result.getLong("PhotoID"));
			}

		} catch (SQLException e) {
		} finally {
			try {
				if (query != null) {
					query.close();
				}
				if (result != null) {
					result.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return idList;
	}
	
	/**
	 * 
	 * @param timeLine
	 * @return
	 */
	public List<Long> queryOutdatedIncompleteFiles(long timeLine) {
		List<Long> idList = new ArrayList<Long>();
		Connection dbCon = null;
		ResultSet result = null;
		PreparedStatement query = null;

		try {

			StringBuffer querySB = new StringBuffer(512);
			querySB.append("select id from DocIncomplete where expirytime <? and expirytime>0");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(true);
			query = dbCon.prepareStatement(querySB.toString());
			query.setLong(1, timeLine);
			query.setFetchSize(100000); // 一次最多取10w条记录，防止记录过多，内存爆掉
			result = query.executeQuery();
			while (result.next()) {
				idList.add(result.getLong("id"));
			}

		} catch (SQLException e) {
		} finally {
			try {
				if (query != null) {
					query.close();
				}
				if (result != null) {
					result.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return idList;
	}

	/**
	 * 删除指定的docId对应的记录，删除前检查记录是否过期。
	 * 
	 * @param docId 可能过期的ID
	 * @param timeLine 过期时间
	 * @return 记录删除成功返回true，否则返回false
	 */
	public boolean cleanRecord(long docId, long timeLine, long[] size) {
		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		ResultSet rs = null;
		try {
			StringBuffer selectSB = new StringBuffer(512);
			selectSB.append("select size from PhotoMD5 where PhotoID = ? and expirytime < ? and count = 0 for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, docId);
			select.setLong(2, timeLine);
			rs = select.executeQuery();
			while (rs.next()) {
				long docSize = rs.getLong(1);
				if (docSize > 0) {
					size[0] += docSize;
				}
				update = dbCon.prepareStatement("delete from PhotoMD5 where PhotoId = ?");
				update.setLong(1, docId);
				int row = update.executeUpdate();
				if (row == 1) {
					logger.info("delete record id " + docId + " size " + docSize);
					return true;
				} else {
					logger.error("clean record " + docId + "error");
				}
				break;
			}

		} catch (SQLException e) {
			logger.error("clean db record exception", e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}

				if (update != null) {
					update.close();
				}

				if (rs != null) {
					rs.close();
				}

				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return false;
	}
	
	public boolean cleanIncompleteRecord(long docId, long timeLine, long[] size) {
		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement update = null;
		ResultSet rs = null;
		try {
			StringBuffer selectSB = new StringBuffer(512);
			selectSB.append("select size from DocIncomplete where id = ? and expirytime < ? for update");
			dbCon = DriverManager.getConnection(dbstr);
			dbCon.setAutoCommit(false);
			select = dbCon.prepareStatement(selectSB.toString());
			select.setLong(1, docId);
			select.setLong(2, timeLine);
			rs = select.executeQuery();
			while (rs.next()) {
				long docSize = rs.getLong(1);
				if (docSize > 0) {
					size[0] += docSize;
				}
				update = dbCon.prepareStatement("delete from DocIncomplete where id = ?");
				update.setLong(1, docId);
				int row = update.executeUpdate();
				if (row == 1) {
					logger.info("delete record id " + docId + " size " + docSize);
					return true;
				} else {
					logger.error("clean record " + docId + "error");
				}
				break;
			}

		} catch (SQLException e) {
			logger.error("clean db record exception", e);
		} finally {
			try {
				if (select != null) {
					select.close();
				}

				if (update != null) {
					update.close();
				}

				if (rs != null) {
					rs.close();
				}

				if (dbCon != null) {
					dbCon.commit();
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
		return false;
	}

	/**
	 * 根据指定的srcId， 复制新的一条md5记录到另外一个分区。 新纪录的引用计数为0。
	 * 如果记录不存在，或者userType相等，则复制操作失败，抛出异常。
	 * 
	 * @param srcId 源文档ID
	 * @param newID 新的文档ID
	 * @param userType 用户类型
	 */
	public void copyPhotoRecord(long srcId, byte[] md5, long newID, int srcUserType, int destUserType, long expiredTime)
			throws IOException {
		Connection dbCon = null;
		PreparedStatement select = null;
		PreparedStatement selectUserType = null;
		PreparedStatement insertStateMent = null;
		ResultSet rs = null, userRs = null;
		long md5high = 0;
		long md5low = 0;
		long size = 0;
		long oldExpiredTime = 0;
		int olduUserType = 0;
		try {
			dbCon = DriverManager.getConnection(dbstr);
			String selectStr = "select userType,count,md5high,md5low,size,expirytime from PhotoMD5 ";
			if (srcId == -10) {
				selectStr += " where md5High=? and md5Low=? and userType=? "; // 支持md5方式的copyfile分发
				select = dbCon.prepareStatement(selectStr);
				select.setLong(1, Util.bytes2long(md5, 0, 8));
				select.setLong(2, Util.bytes2long(md5, 8, 8));
				select.setInt(3, srcUserType);
			} else {
				selectStr += " where PhotoID=? ";// 支持docid 方式的copyfile 分发
				select = dbCon.prepareStatement(selectStr);
				select.setLong(1, srcId);
			}
			rs = select.executeQuery();
			while (rs.next()) {
				md5high = rs.getLong("md5high");
				md5low = rs.getLong("md5low");
				olduUserType = rs.getInt("userType");
				size = rs.getLong("size");
				oldExpiredTime = rs.getLong("expirytime");
				break;
			}
			if (md5high == 0 && md5low == 0) {
				throw new DFSException(DFSException.EC_C_GET_FILE, srcId + " is not exist in PhotoMD5");
			}
			if (olduUserType == destUserType) {
				throw new DFSException(DFSException.EC_C_USRTYPE, "userType : " + destUserType
						+ " is exist in PhotoMD5");
			}
			selectUserType = dbCon
					.prepareStatement("select userType,PhotoID from PhotoMD5 where md5High=? and md5Low=? ");
			selectUserType.setLong(1, md5high);
			selectUserType.setLong(2, md5low);
			userRs = selectUserType.executeQuery();
			while (userRs.next()) {
				if (userRs.getInt("userType") == destUserType) {
					throw new DFSException(DFSException.EC_C_USRTYPE, "userType : " + destUserType
							+ " is exist in PhotoMD5");
				}
			}
			String str = "insert into PhotoMD5(PhotoID, userType, md5high, md5low, count, size, expirytime) values( ?, ?, ?, ?, ?, ? ,?)";
			dbCon.setAutoCommit(true);
			insertStateMent = dbCon.prepareStatement(str);
			insertStateMent.setLong(1, newID);
			insertStateMent.setInt(2, destUserType);
			insertStateMent.setLong(3, md5high);
			insertStateMent.setLong(4, md5low);
			insertStateMent.setInt(5, 0);// 新userType的 count 为1
			insertStateMent.setLong(6, size);
			insertStateMent.setLong(7, expiredTime == -10 ? oldExpiredTime : expiredTime);
			insertStateMent.executeUpdate();
		} catch (SQLException e) {
			logger.error("copyPhotoRecord exception", e);
			throw new DFSException(DFSException.EC_S_NODEF, e.getMessage(), e);
		} finally {
			try {
				if (selectUserType != null) {
					selectUserType.close();
				}
				if (select != null) {
					select.close();
				}
				if (insertStateMent != null) {
					insertStateMent.close();
				}
				if (dbCon != null) {
					dbCon.close();
				}
			} catch (SQLException ex) {
				logger.error("finally{SQLException}", ex);
			}
		}
	}
}
