package com.test.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Test {
	static String[] whos = {};
	static String ufile = "D:/temp/jpm/uinfo";
	static String whopath = "D:/temp/jpm/";
	static Hashtable<String, String> toMap = new Hashtable<String, String>();
	static String basefd = "D:/temp/jpm/";
	private static String hosts = "db-33.photo.163.org:5558";
	static Connection conn = null;

	public static void main(String[] args) {
		try {
			test1();
			System.out.println("====success====");
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private static void test1() throws Exception {

		TTransport tr = new TFramedTransport(new TSocket("172.17.2.163", 9160));
		TProtocol proto = new TBinaryProtocol(tr);

		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		if (!tr.isOpen()) {
			System.out.println("failed to connect server!");
			return;
		}
		String keyspace = "demo";
		client.set_keyspace(keyspace);

		long temp = System.currentTimeMillis();
		ColumnParent parent = new ColumnParent("Student");// column family

		/*
		 * 这里我们插入100万条数据到Student内 每条数据包括id和name
		 */
		String key_user_id = "a";
		for (int i = 0; i < 1000; i++) {
			String k = key_user_id + i;// key
			long timestamp = System.currentTimeMillis();// 时间戳

			Column idColumn = new Column(toByteBuffer("id"));// column name
			idColumn.setValue(toByteBuffer(i + ""));// column value
			idColumn.setTimestamp(timestamp);
			client.insert(toByteBuffer(k), parent, idColumn,
					ConsistencyLevel.ONE);

			Column nameColumn = new Column(toByteBuffer("name"));
			nameColumn.setValue(toByteBuffer("student" + i));
			nameColumn.setTimestamp(timestamp);
			client.insert(toByteBuffer(k), parent, nameColumn,
					ConsistencyLevel.ONE);
		}

		/*
		 * 读取某条数据的单个字段
		 */
		ColumnPath path = new ColumnPath("Student");// 设置读取Student的数据
		path.setColumn(toByteBuffer("id")); // 读取id
		String key3 = "a1";// 读取key为a1的那条记录
		System.out.println(toString(client.get(toByteBuffer(key3), path,
				ConsistencyLevel.ONE).column.value));

		/*
		 * 读取整条数据
		 */
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange(toByteBuffer(""),
				toByteBuffer(""), false, 10);
		predicate.setSlice_range(sliceRange);
		List<ColumnOrSuperColumn> results = client.get_slice(
				toByteBuffer(key3), parent, predicate, ConsistencyLevel.ONE);

		for (ColumnOrSuperColumn result : results) {
			Column column = result.column;
			System.out.println(toString(column.name) + " -> "
					+ toString(column.value));
		}

		long temp2 = System.currentTimeMillis();
		System.out.println("time: " + (temp2 - temp) + " ms");// 输出耗费时间

		tr.close();
	}

	/*
	 * 将String转换为bytebuffer，以便插入cassandra
	 */
	public static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	/*
	 * 将bytebuffer转换为String
	 */
	public static String toString(ByteBuffer buffer)
			throws UnsupportedEncodingException {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes, "UTF-8");
	}
}
