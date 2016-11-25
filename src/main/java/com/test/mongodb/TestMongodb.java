package com.test.mongodb;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

public class TestMongodb {
	static Mongo m;

	static String start_datetime;
	static String stop_datetime;

	public static void main(String[] args) throws Exception {
		test_update();

	}

	@SuppressWarnings({ "rawtypes", "unchecked", "static-access" })
	private static void testmongodb() throws Exception {
		ServerAddress sa1 = new ServerAddress("192.168.12.210", 27017);
		ServerAddress sa2 = new ServerAddress("192.168.12.210", 27027);
		ServerAddress sa3 = new ServerAddress("192.168.12.210", 27037);
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		salist.add(sa2);
		salist.add(sa3);
		// new Mongo();
		m = new Mongo(salist);
		
		DB db = m.getDB("db2");
		// char[] pass = "db2".toCharArray();
		// if (db.authenticate("db2", pass)) {

		List<String> cols = new ArrayList<String>();
		for (int j = 1; j < 2; j++) {
			cols.add("tab_" + j);
		}

		DBCollection col = null;
		for (Iterator<String> iterator = cols.iterator(); iterator.hasNext();) {
			String colname = (String) iterator.next();
			System.out.println("create===========" + colname);
			col = db.getCollection(colname);
			BasicDBObject info = new BasicDBObject();

			List list = new ArrayList();
			for (int i = 0; i < 50; i++) {
				if (i % 10 == 9) {
					col.insert(list);
					Thread.currentThread().sleep(100);
				}
				info = new BasicDBObject();
				info.put("id", i);
				info.put("time", new Date());
				info.put("name", "name_" + i);
				info.put("type", (int) (Math.random() * 8));

				list.add(info);
			}
		}
		// } else {
		// System.out.println("auth failed");
		// }
	}

	private static void test1() throws UnknownHostException {
		ServerAddress sa1 = new ServerAddress("192.168.164.63", 17001);
		// ServerAddress sa2 = new ServerAddress("192.168.164.64", 24018);
		// ServerAddress sa3 = new ServerAddress("192.168.164.65", 24018);
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		// salist.add(sa2);
		// salist.add(sa3);
		m = new Mongo(salist);

		List<String> cols = new ArrayList<String>();
		for (int j = 1; j < 4; j++) {
			cols.add("xxxx_" + j);
		}
		crtCol(cols);

		// rmCol(m);
	}

	static DateFormat spacedatetimeformat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	private static void testupsertandsave() throws UnknownHostException {
		ServerAddress sa1 = new ServerAddress("192.168.12.210", 20001);
		// ServerAddress sa2 = new ServerAddress("192.168.164.64", 24018);
		// ServerAddress sa3 = new ServerAddress("192.168.164.65", 24018);
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		// salist.add(sa2);
		// salist.add(sa3);
		m = new Mongo(salist);
		DB db = m.getDB("db1");
		DBCollection tab0 = db.getCollection("tab0");
		DBCollection tab1 = db.getCollection("tab1");

		// String s1 = spacedatetimeformat.format(new Date());
		//
		// for (int i = 0; i < 50000; i++) {
		// BasicDBObject info = new BasicDBObject();
		// info.put("id", i);
		// info.put("time", new Date());
		// info.put("type", i % 10);
		// tab0.insert(info);
		// }
		// String e1 = spacedatetimeformat.format(new Date());
		// System.out.println(s1 + "\t" + e1);
		//
		// String s2 = spacedatetimeformat.format(new Date());
		//
		// for (int i = 0; i < 50000; i++) {
		// BasicDBObject info = new BasicDBObject();
		// info.put("id", i);
		// info.put("time", new Date());
		// info.put("type", i % 10);
		// tab1.save(info);
		// }
		// String e2 = spacedatetimeformat.format(new Date());
		// System.out.println(s2 + "\t" + e2);

		// String s1 = spacedatetimeformat.format(new Date());
		//
		// for (int i = 0; i < 20000; i++) {
		// BasicDBObject q = new BasicDBObject();
		// BasicDBObject u = new BasicDBObject();
		// q.put("id", i);
		// u.put("id", i);
		// u.put("time", new Date());
		// u.put("type", i % 3);
		// tab0.update(q, u, true, false);
		// }
		// String e1 = spacedatetimeformat.format(new Date());
		// System.out.println(s1 + "\t" + e1);

		String s2 = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 2000; i++) {
			BasicDBObject info = new BasicDBObject();
			info.put("id", i);
			info.put("time", new Date());
			info.put("type", i % 6);
			tab1.save(info);
		}
		String e2 = spacedatetimeformat.format(new Date());

		System.out.println(s2 + "\t" + e2);

	}

	private static void testjoin() throws UnknownHostException {
		ServerAddress sa1 = new ServerAddress("192.168.12.210", 20001);
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		m = new Mongo(salist);
		DB db = m.getDB("db1");
		DBCollection c1 = db.getCollection("c1");
		DBCollection c2 = db.getCollection("c2");

		String s1 = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 20; i++) {
			BasicDBObject info = new BasicDBObject();
			info.put("id", i);
			info.put("time1", new Date());
			info.put("type1", i % 10);
			c1.insert(info);
		}
		String e1 = spacedatetimeformat.format(new Date());
		System.out.println(s1 + "\t" + e1);

		String s2 = spacedatetimeformat.format(new Date());

		for (int i = 10; i < 100; i++) {
			BasicDBObject info = new BasicDBObject();
			info.put("id", i);
			info.put("time2", new Date());
			info.put("type2", i % 10);
			c2.save(info);
		}
		String e2 = spacedatetimeformat.format(new Date());
		System.out.println(s2 + "\t" + e2);

		start_datetime = spacedatetimeformat.format(new Date());
		DBCursor cur1 = c1.find();

		while (cur1.hasNext()) {
			DBObject dbo1 = cur1.next();
			int id1 = (Integer) dbo1.get("id");
			String time1 = dbo1.get("time1").toString();
			String type1 = dbo1.get("type1").toString();
			BasicDBObject q_dbo = new BasicDBObject();
			q_dbo.put("id", id1);
			DBCursor cur2 = c2.find(q_dbo);
			while (cur2.hasNext()) {
				DBObject dbo2 = cur2.next();
				String id2 = dbo2.get("id").toString();
				String time2 = dbo2.get("time2").toString();
				String type2 = dbo2.get("type2").toString();
				System.out.println(id1 + "\t" + time1 + "\t" + type1 + "\t"
						+ id2 + "\t" + time2 + "\t" + type2);
			}
		}
		stop_datetime = spacedatetimeformat.format(new Date());
		System.out.println(start_datetime + "\t" + stop_datetime);
	}

	private static void testmapred() throws UnknownHostException {
		ServerAddress sa1 = new ServerAddress("192.168.12.210", 20001);
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		m = new Mongo(salist);
		DB db = m.getDB("db1");
		DBCollection c1 = db.getCollection("c1");
		DBCollection c2 = db.getCollection("c2");

		String s1 = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 10000; i++) {
			BasicDBObject info = new BasicDBObject();
			info.put("id", i);
			info.put("time1", new Date());
			info.put("type1", i % 3);
			c1.insert(info);
		}
		String e1 = spacedatetimeformat.format(new Date());
		System.out.println(s1 + "\t" + e1);

		String s2 = spacedatetimeformat.format(new Date());

		for (int i = 5000; i < 15000; i++) {
			BasicDBObject info = new BasicDBObject();
			info.put("id", i);
			info.put("time2", new Date());
			info.put("type2", i % 4);
			c2.save(info);
		}
		String e2 = spacedatetimeformat.format(new Date());
		System.out.println(s2 + "\t" + e2);

		start_datetime = spacedatetimeformat.format(new Date());
		// String map = "";
		// String reduce = "";
		// MapReduceCommand cmd = new MapReduceCommand(c1, map, reduce, null,
		// MapReduceCommand.OutputType.INLINE, null);
		//
		// MapReduceOutput out = c1.mapReduce(cmd);
		//
		// for (DBObject o : out.results()) {
		// System.out.println(o.toString());
		// }

		String map1 = " function(){  var output = {id1: this.id, time1: this.time1, type1: this.type1, id2: -1, time2: new Date(1970,1,1), type2: -1};  emit(this.id, output);};";
		String map2 = " function(){  var output = {id1: -1, time1: new Date(1970,1,1), type1: -1, id2: this.id, time2: this.time2, type2: this.type2};  emit(this.id, output);};";
		String reduce = " function(key, values) { var outs = { id1: -1, time1: new Date(1970,1,1), type1: -1, id2: -1, time2: new Date(1970,1,1), type2: -1 }; values.forEach(function(v){ if (v.id2 == -1) { outs.id1 = v.id1; outs.time1 = v.time1; outs.type1 = v.type1; } if (v.id1 == -1) { outs.id2 = v.id2; outs.time2 = v.time2; outs.type2 = v.type2; } }); return outs; }; ";
		MapReduceCommand cmd1 = new MapReduceCommand(c1, map1, reduce, "j",
				MapReduceCommand.OutputType.REDUCE, null);
		MapReduceCommand cmd2 = new MapReduceCommand(c2, map2, reduce, "j",
				MapReduceCommand.OutputType.REDUCE, null);
		c1.mapReduce(cmd1);
		c2.mapReduce(cmd2);

		DBCollection j = db.getCollection("j");
		BasicDBObject o = new BasicDBObject();
		o.put("$where", "this.value.id1==this.value.id2");
		DBCursor cur = j.find(o);

		while (cur.hasNext()) {
			System.out.println(cur.next());
		}

		stop_datetime = spacedatetimeformat.format(new Date());
		System.out.println(start_datetime + "\t" + stop_datetime);
	}

	private static void test_insertcascade() throws UnknownHostException {
		// ServerAddress sa1 = new ServerAddress("192.168.12.210", 28117);
		ServerAddress sa1 = new ServerAddress("192.168.12.210:38001");
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		m = new Mongo(salist);
		DB db = m.getDB("db1");

		DBCollection c1 = db.getCollection("c1");
		DBCollection c2 = db.getCollection("c2");
		c1.drop();
		c2.drop();

		start_datetime = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 500000; i++) {
			// BasicDBObject v = new BasicDBObject();
			// v.put("key", (int) (Math.random() * 10));
			// v.put("value", (int) (Math.random() * 100));
			BasicDBObject info = new BasicDBObject();
			info.put("_id", i);
			info.put("key", (int) (Math.random() * 50));
			info.put("value", (int) (Math.random() * 1000));
			// info.put("map1", v);
			info.put("datetime1", spacedatetimeformat.format(new Date()));
			c1.insert(info);
		}
		stop_datetime = spacedatetimeformat.format(new Date());
		System.out.println(start_datetime + "\t" + stop_datetime);

		start_datetime = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 0; i++) {
			// BasicDBObject v = new BasicDBObject();
			// v.put("key", (int) (Math.random() * 10));
			// v.put("value", (int) (Math.random() * 100));
			BasicDBObject info = new BasicDBObject();
			info.put("_id", i);
			info.put("key", (int) (Math.random() * 50));
			info.put("value", (int) (Math.random() * 1000));
			// info.put("map2", v);
			info.put("datetime2", spacedatetimeformat.format(new Date()));
			c2.insert(info);
		}
		stop_datetime = spacedatetimeformat.format(new Date());
		System.out.println(start_datetime + "\t" + stop_datetime);

	}

	private static void test_update() throws UnknownHostException {
		// ServerAddress sa1 = new ServerAddress("192.168.12.210", 28117);
		ServerAddress sa1 = new ServerAddress("192.168.12.210:20001");
		List<ServerAddress> salist = new ArrayList<ServerAddress>();
		salist.add(sa1);
		m = new Mongo(salist);
		DB db = m.getDB("db1");

		DBCollection c1 = db.getCollection("c1");
		c1.drop();

		start_datetime = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 500; i++) {
			BasicDBObject info = new BasicDBObject();
			info.put("_id", i);
			
			info.put("key", (int) (Math.random() * 50));
			info.put("value", (int) (Math.random() * 1000));
			// info.put("map1", v);
			info.put("datetime1", spacedatetimeformat.format(new Date()));
			c1.insert(info);
		}
		stop_datetime = spacedatetimeformat.format(new Date());
		System.out.println(start_datetime + "\t" + stop_datetime);

		start_datetime = spacedatetimeformat.format(new Date());

		for (int i = 0; i < 50; i++) {
			BasicDBObject qry = new BasicDBObject();
			qry.put("_id", i);

			BasicDBObject res = new BasicDBObject();
			res.put("value", i + 10);

			c1.update(qry, res, false, true);
		}
		stop_datetime = spacedatetimeformat.format(new Date());
		System.out.println(start_datetime + "\t" + stop_datetime);

	}

	private static void crtCol(List<String> cols) {
		DB db = m.getDB("testmongo");

		DBCollection col = null;
		for (Iterator<String> iterator = cols.iterator(); iterator.hasNext();) {
			String colname = (String) iterator.next();
			System.out.println("create===========" + colname);
			col = db.getCollection(colname);
			BasicDBObject info = new BasicDBObject();

			// col.remove(info);
			for (int i = 0; i < 10000; i++) {
				info = new BasicDBObject();
				info.put("id", i);
				info.put("time", new Date());

				info.put("type", (int) (Math.random() * 8));
				col.insert(info);
			}
		}

	}

	private static void dropCol(List<String> cols) {
		DB db = m.getDB("testshard");
		// boolean authed = db.authenticate("jhh", "jhh".toCharArray());
		// System.out.println(authed);
		DBCollection col = null;
		for (Iterator<String> iterator = cols.iterator(); iterator.hasNext();) {
			String colname = (String) iterator.next();
			System.out.println("drop===========" + colname);
			col = db.getCollection(colname);
			col.drop();
		}
	}

	private static void rmCol(Mongo m) {
		DB db = m.getDB("test");

		DBCollection col1 = db.getCollection("col1");
		BasicDBObject query = new BasicDBObject();

		query.put("id", new BasicDBObject("$gt", 50));
		col1.remove(query);

	}

	private static void shardCol(List<String> cols) {
		DB db = m.getDB("admin");

		DBCollection col = null;
		for (Iterator<String> iterator = cols.iterator(); iterator.hasNext();) {
			String colname = (String) iterator.next();
			System.out.println("set shard for col :" + colname);

			db.command("{ shardcollection : \"testshard." + colname
					+ "\",key : {id: 1} } ");
		}

	}

	private static void testinsert1(DB db) {
		// for (int i = 0; i < 100; i++) {
		// info = new BasicDBObject();
		// info.put("id", i+100);
		// info.put("user", "user_"+i);
		// info.put("content", "this is the content: "+i +" ...");
		//
		// col2.insert(info);
		// }

		DBCollection dbcoll = db.getCollection("test");

		BasicDBObject doc = new BasicDBObject();

		doc.put("name", "MongoDB");
		doc.put("type", "database");
		doc.put("count", 1);

		BasicDBObject info = new BasicDBObject();

		info.put("x", 203);
		info.put("y", 102);

		doc.put("info", info);

		dbcoll.insert(doc);

		DBObject myDoc = dbcoll.findOne();
		System.out.println(myDoc);
	}

	private static void testquery1(Mongo m) {
		DB db = m.getDB("test");
		DBCollection coll = db.getCollection("test");

		System.out.println(coll.getCount());
		DBCursor cur = coll.find();

		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	private static void testquery2(Mongo m) {
		DB db = m.getDB("test");
		DBCollection coll = db.getCollection("test");

		// System.out.println(coll.getCount());

		BasicDBObject query = new BasicDBObject();
		query.put("id", 71);
		DBCursor cur = coll.find(query);
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	private static void testquery3(Mongo m) {
		DB db = m.getDB("test");

		DBCollection col1 = db.getCollection("col1");
		BasicDBObject query = new BasicDBObject();

		query.put("x", new BasicDBObject("$gt", 2));
		// query.put("j", new BasicDBObject("$gt", 10));

		DBCursor cur = col1.find(query);
		col1.findAndRemove(query)
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}
}
