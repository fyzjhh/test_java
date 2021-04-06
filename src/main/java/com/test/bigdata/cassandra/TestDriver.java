package com.test.cassandra;

import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class TestDriver {

	public static void main(String[] args) {
		try {
			test_insert();

			System.out.println("====success====");
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void test_select() {
		Cluster cluster = Cluster.builder().addContactPoint("192.168.12.221")
				.build();
		Session session = cluster.connect("mykeyspace");
		String cql = "select * from mykeyspace.t where b>=100 and b<=200;";
		ResultSet result = session.execute(cql);

		Iterator<Row> iterator = result.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			int a = row.getInt("a");
			int b = row.getInt("b");
			System.out.println(a + "\t" + b);
		}
		session.close();
		cluster.close();
	}

	private static void test_insert() {
		Cluster cluster = Cluster.builder().addContactPoint("192.168.12.221")
				.build();
		Session session = cluster.connect("mykeyspace");
		for (int i = 1; i <= 100000; i++) {
			int a = i;
			int b = i % 10;
			int c = i % 100;
			session.execute(QueryBuilder.insertInto("mykeyspace", "t2").values(
					new String[] { "a", "b", "c" }, new Object[] { a, b, c }));
			if (i % 100 == 0) {
				System.out.println("insert " + i+ " reconds====");
			}
		}
		session.close();
		cluster.close();

	}
}
