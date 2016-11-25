package com.test.db.mysqlcluster;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

public class TestCluster {

	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };

	public static void main(String[] args) throws Exception {

		testbatchinsert();
	}

	private static void testbatchinsert() throws Exception {

		Properties props = new Properties();

		props.put("com.mysql.clusterj.connectstring", "172.17.2.163:1186");
		props.put("com.mysql.clusterj.database", "test");

		SessionFactory factory = ClusterJHelper.getSessionFactory(props);

		Session session = factory.getSession();
		Employee newEmployee = session.newInstance(Employee.class);
		newEmployee.setId(988);
		newEmployee.setFirst("John");
		newEmployee.setLast("Jones");

		session.persist(newEmployee);
		Employee theEmployee = session.find(Employee.class, 988);
		System.out.println(theEmployee.getId() + "\t" + theEmployee.getFirst()
				+ "\t" + theEmployee.getLast());
	}

}
