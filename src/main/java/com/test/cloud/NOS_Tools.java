package com.test.cloud;

import java.io.DataInputStream;
import java.sql.Blob;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.netease.backend.db.DBConnection;
import com.test.dfs.DocIDEntity;

public class NOS_Tools {

	static String sql = " select LinkID,Size,DocIDList from NOS_ObjectLink where LinkID=? limit 100 ;";
	static List<Object> params;

	static byte[] docidBytes = new byte[] {(byte)0x00,
		(byte)0x04,
		(byte)0x46,
		(byte)0x00,
		(byte)0x00,
		(byte)0x00,
		(byte)0x0E,
		(byte)0x54,};

	static byte[] offsetBytes = new byte[] {(byte)0x00,
		(byte)0x00,
		(byte)0x00,
		(byte)0x00,
		(byte)0x00,
		(byte)0x00,
		(byte)0x66,
		(byte)0x91,
};

	public static void main(String[] args) throws Exception {
		// migrateBucket();
		System.out.println(byteArrayToLong(docidBytes, 0));
		System.out.println(byteArrayToLong(offsetBytes, 0));
	}

	public static long byteArrayToLong(byte[] data, int offset) {
		long r = data[offset++];
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		r = (r << 8) | ((long) (data[offset++]) & 0xff);
		return r;
	}

	private static void doWork() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		DBConnection conn = (DBConnection) DriverManager.getConnection(
				"10.120.202.6:8882?key=src/secret.key", "jhhtest", "jhhtest");
		PreparedStatement ps = conn.prepareStatement(sql);
		Object[] objs = params.toArray();
		for (int i = 0; i < objs.length; i++) {
			ps.setObject(i + 1, objs[i]);
		}

		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			long docid = rs.getLong(1);
			long size = rs.getLong(2);
			System.out.print(docid + "\t" + size + "\t");
			Blob blob = rs.getBlob("DocIDList");
			DataInputStream in = new DataInputStream(blob.getBinaryStream());
			List<DocIDEntity> enList = unSerialize(in);
			for (Iterator<DocIDEntity> it = enList.iterator(); it.hasNext();) {
				DocIDEntity di = (DocIDEntity) it.next();
				System.out.print(di + ",");
			}
			System.out.println();
		}
		conn.close();
	}

	/**
	 * 对一个流中的所有数据反序列化成为一个List
	 * 
	 * @param in
	 * @return
	 */
	public static List<DocIDEntity> unSerialize(DataInputStream in) {
		LinkedList<DocIDEntity> retList = new LinkedList<DocIDEntity>();
		try {
			while (true) {
				long docid = in.readLong();
				long size = in.readLong();
				retList.add(new DocIDEntity(docid, size));
			}
		} catch (Exception e) {
		}
		return retList;
	}

}
