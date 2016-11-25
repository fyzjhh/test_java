package com.test.hbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	private static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.12.210");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master.port", "60000");
		conf = HBaseConfiguration.create(conf);

	}

	/**
	 * 创建表操作
	 * 
	 * @throws IOException
	 */
	public void createTable(String tablename, String[] cfs) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
			System.out.println(" 表创建成功！");
		}
		admin.close();
	}

	/**
	 * 删除表操作
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public void deleteTable(String tablename) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (!admin.tableExists(tablename)) {
			System.out.println("表不存在, 无需进行删除操作！");
		} else {
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			System.out.println(" 表删除成功！");
		}
		admin.close();

	}

	public void insertRow() throws Exception {
		HTable table = new HTable(conf, "test");
		Put put = new Put(Bytes.toBytes("row3"));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("444"),
				Bytes.toBytes("value444"));
		table.put(put);
		table.close();
	}

	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 * @param cfs
	 * @throws Exception
	 */
	public void writeRow(String tablename, String[] cfs) throws Exception {

		HTable table = new HTable(conf, tablename);
		Put put = new Put(Bytes.toBytes("rows3"));
		for (int j = 0; j < cfs.length; j++) {
			put.add(Bytes.toBytes(cfs[j]),
					Bytes.toBytes(cfs[j] + String.valueOf(1)),
					Bytes.toBytes(cfs[j] + "value"));

			table.put(put);
		}
		System.out.println("写入成功！");
		table.close();
	}

	// 写多条记录
	public void writeMultRow(String tablename, String[][] cfs) throws Exception {

		HTable table = new HTable(conf, tablename);
		List<Put> lists = new ArrayList<Put>();
		for (int i = 0; i < cfs.length; i++) {
			Put put = new Put(Bytes.toBytes(cfs[i][0]));
			put.add(Bytes.toBytes(cfs[i][1]), Bytes.toBytes(cfs[i][2]),
					Bytes.toBytes(cfs[i][3]));
			lists.add(put);
		}
		table.put(lists);
		System.out.println("写入成功！");
		table.close();
	}

	/**
	 * 删除一行记录
	 * 
	 * @param tablename
	 * @param rowkey
	 * @throws IOException
	 */
	public void deleteRow(String tablename, String rowkey) throws IOException {
		HTable table = new HTable(conf, tablename);
		List<Delete> list = new ArrayList<Delete>();
		Delete d1 = new Delete(rowkey.getBytes());
		list.add(d1);
		table.delete(list);
		System.out.println("删除行成功！");
		table.close();
	}

	/**
	 * 查找一行记录
	 * 
	 * @param tablename
	 * @param rowkey
	 */
	public static void selectRow(String tablename, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get(rowKey.getBytes());
		// g.addColumn(Bytes.toBytes("cf:1"));
		Result rs = table.get(g);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + "  ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + "  ");
			System.out.print(kv.getTimestamp() + "  ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}

	public static void selectColumn(String tablename, String cf, String c)
			throws IOException {
		HTable table = new HTable(conf, tablename);
		Get g = new Get();
		g.addColumn(cf.getBytes(), c.getBytes());
		// g.addColumn(Bytes.toBytes("cf:1"));
		Result rs = table.get(g);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + "  ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + "  ");
			System.out.print(kv.getTimestamp() + "  ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}

	public static void testjdbc() throws Exception {
		Class.forName("org.apache.hadoop.hbase.jdbc.Driver");

		// Get a connection with an HTablePool size of 10
		Connection conn = DriverManager
				.getConnection("jdbc:hbql;maxtablerefs=10");

		// // or
		// Connection conn2 =
		// DriverManager.getConnection("jdbc:hbql;maxtablerefs=10;hbase.master=192.168.1.90:60000");
		//
		// // or if you want to connect with a HBaseConfiguration object, then
		// you would call:
		// Configuration config = HBaseConfiguration.create();
		// Connection conn3 =
		// org.apache.hadoop.hbase.jdbc.Driver.getConnection("jdbc:hbql;maxtablerefs=10",
		// config);

		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE table12 (f1(), f3()) IF NOT tableexists('table12')");

		stmt.execute("CREATE TEMP MAPPING sch9 FOR TABLE table12" + "("
				+ "keyval key, " + "f1 (" + "    val1 string alias val1, "
				+ "    val2 string alias val2 " + "), " + "f3 ("
				+ "    val1 int alias val5, " + "    val2 int alias val6 "
				+ "))");

		ResultSet rs = stmt.executeQuery("select * from sch9");

		while (rs.next()) {
			int val5 = rs.getInt("val5");
			int val6 = rs.getInt("val6");
			String val1 = rs.getString("val1");
			String val2 = rs.getString("val2");

			System.out.print("val5: " + val5);
			System.out.print(", val6: " + val6);
			System.out.print(", val1: " + val1);
			System.out.println(", val2: " + val2);
		}

		rs.close();

		stmt.execute("DISABLE TABLE table12");
		stmt.execute("DROP TABLE table12");
		stmt.close();

		conn.close();
	}

	/**
	 * 查询表中所有行
	 * 
	 * @param tablename
	 * @throws Exception
	 */
	public void scaner(String tablename) throws Exception {

		HTable table = new HTable(conf, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for (Result r : rs) {
			KeyValue[] kv = r.raw();
			for (int i = 0; i < kv.length; i++) {
				System.out.print(new String(kv[i].getRow()) + "  ");
				System.out.print(new String(kv[i].getFamily()) + ":");
				System.out.print(new String(kv[i].getQualifier()) + "  ");
				System.out.print(kv[i].getTimestamp() + "  ");
				System.out.println(new String(kv[i].getValue()));
			}
		}
		table.close();

	}

	public static void main(String[] args) throws Exception {
		String tablename = "medicinelog";
		Test cli = new Test();

		// 删除表
		cli.deleteTable(tablename);

		// 创建表
		cli.createTable(tablename, new String[] { "cf" });

		// 写多条记录
		cli.writeMultRow(tablename, new String[][] {
				{ "rows1", "cf", "1", "value1" },
				{ "rows1", "cf", "2", "value2" },
				{ "rows2", "cf", "2", "value2" } });

		// 写一条记录
		cli.writeRow(tablename, new String[] { "cf" });

		System.out.println("\n查询一个rows1:");
		Test.selectRow(tablename, "rows1");
		System.out.println("\n查询一个rows1:");
		Test.selectColumn(tablename, "cf", "2");

		System.out.println("\n查询所有：");
		cli.scaner(tablename);

		// // 查询所有
		// Test.showAllRecords("student");
		//
		// // 根据主键rowKey查询一行数据
		// Test.showOneRecordByRowKey("student", "200977100709");
		//
		// // 根据主键查询某行中的一列
		// Test.showOneRecordByRowKey_cloumn("student", "200977100709", "name");
		// Test.showOneRecordByRowKey_cloumn("student", "200977100709",
		// "info:age");
	}

	/****
	 * 使用scan查询所有数据
	 * 
	 * @param tableName
	 */
	public static void showAllRecords(String tableName) {
		System.out.println("start==============show All Records=============");

		HTablePool pool = new HTablePool(conf, 1000);
		// 创建table对象
		HTable table = (HTable) pool.getTable(tableName);

		try {
			// Scan所有数据
			Scan scan = new Scan();
			ResultScanner rss = table.getScanner(scan);

			for (Result r : rss) {
				System.out.println("\n row: " + new String(r.getRow()));

				for (KeyValue kv : r.raw()) {

					System.out.println("family=>"
							+ new String(kv.getFamily(), "utf-8") + "  value=>"
							+ new String(kv.getValue(), "utf-8")
							+ "  qualifer=>"
							+ new String(kv.getQualifier(), "utf-8")
							+ "  timestamp=>" + kv.getTimestamp());
				}
			}
			rss.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("\n end==============show All Records=============");
	}

	/***
	 * 根据主键rowKey查询一行数据 get 'student','010'
	 */
	public static void showOneRecordByRowKey(String tableName, String rowkey) {
		HTablePool pool = new HTablePool(conf, 1000);
		HTable table = (HTable) pool.getTable(tableName);

		try {
			Get get = new Get(rowkey.getBytes()); // 根据主键查询
			// Get get1 = new Get(); // 根据主键查询
			// get1.addColumn(family, qualifier)
			Result r = table.get(get);

			System.out.println("start===showOneRecordByRowKey==row: " + "\n");
			System.out.println("row: " + new String(r.getRow(), "utf-8"));

			for (KeyValue kv : r.raw()) {
				// 时间戳转换成日期格式
				String timestampFormat = new SimpleDateFormat(
						"yyyy-MM-dd HH:MM:ss").format(new Date(kv
						.getTimestamp()));
				// System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
				System.out.println("\nKeyValue: " + kv);
				System.out.println("key: " + kv.getKeyString());

				System.out.println("family=>"
						+ new String(kv.getFamily(), "utf-8") + "  value=>"
						+ new String(kv.getValue(), "utf-8") + "  qualifer=>"
						+ new String(kv.getQualifier(), "utf-8")
						+ "  timestamp=>" + timestampFormat);

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end===========showOneRecordByRowKey");
	}

	/**
	 * 根据rowkey,一行中的某一列簇查询一条数据 get 'student','010','info' student
	 * sid是010的info列簇（info:age,info:birthday）
	 * 
	 * get 'student','010','info:age' student sid是010的info:age列,quafilier是age
	 */
	// public static void showOneRecordByRowKey_cloumn(String tableName,String
	// rowkey,String column,String quafilier)
	public static void showOneRecordByRowKey_cloumn(String tableName,
			String rowkey, String column) {
		System.out.println("start===根据主键查询某列簇showOneRecordByRowKey_cloumn");

		HTablePool pool = new HTablePool(conf, 1000);
		HTable table = (HTable) pool.getTable(tableName);

		try {
			Get get = new Get(rowkey.getBytes());
			get.addFamily(column.getBytes()); // 根据主键查询某列簇
			// get.addColumn(Bytes.toBytes(column),Bytes.toBytes(quafilier));
			// ////根据主键查询某列簇中的quafilier列
			Result r = table.get(get);

			for (KeyValue kv : r.raw()) {
				System.out.println("KeyValue---" + kv);
				System.out.println("row=>" + new String(kv.getRow()));
				System.out.println("family=>"
						+ new String(kv.getFamily(), "utf-8") + ": "
						+ new String(kv.getValue(), "utf-8"));
				System.out.println("qualifier=>"
						+ new String(kv.getQualifier()) + "\n");

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end===========showOneRecordByRowKey_cloumn");
	}

	// （1）时间戳到时间的转换.单一的时间戳无法给出直观的解释。
	public String GetTimeByStamp(String timestamp) {
		long datatime = Long.parseLong(timestamp);
		Date date = new Date(datatime);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss");
		String timeresult = format.format(date);
		System.out.println("Time : " + timeresult);
		return timeresult;

	}

	// （2）时间到时间戳的转换。注意时间是字符串格式。字符串与时间的相互转换，此不赘述。
	public String GetStampByTime(String time) {
		String Stamp = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date;
		try {
			date = sdf.parse(time);
			Stamp = date.getTime() + "000";
			System.out.println(Stamp);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return Stamp;
	}

}