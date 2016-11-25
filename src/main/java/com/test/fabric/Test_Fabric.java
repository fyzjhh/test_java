package com.test.fabric;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.fabric.jdbc.FabricMySQLConnection;

public class Test_Fabric {
	public static void main(String args[]) throws Exception {
		test2();
	}

	private static void test2() throws Exception {

		Class.forName("com.mysql.fabric.jdbc.FabricMySQLDriver");
		String hostname = "192.168.12.220";
		String port = "32274";

		String fabricUsername = "admin";
		String fabricPassword = "admin";
		String database = "employees";
		String username = "fabric";
		String password = "123456";
		// String hostname =
		// System.getProperty("com.mysql.fabric.testsuite.hostname");
		// String port =
		// Integer.valueOf(System.getProperty("com.mysql.fabric.testsuite.port"));
		// String fabricUsername =
		// System.getProperty("com.mysql.fabric.testsuite.fabricUsername");
		// String fabricPassword =
		// System.getProperty("com.mysql.fabric.testsuite.fabricPassword");
		// String username =
		// System.getProperty("com.mysql.fabric.testsuite.username");
		// String password =
		// System.getProperty("com.mysql.fabric.testsuite.password");
		// String database =
		// System.getProperty("com.mysql.fabric.testsuite.database");

		// com.mysql.fabric.testsuite.hostname=localhost
		// com.mysql.fabric.testsuite.port=32274
		//
		// com.mysql.fabric.testsuite.fabricUsername=admin
		// com.mysql.fabric.testsuite.fabricPassword=secret
		//
		// com.mysql.fabric.testsuite.username=root
		// com.mysql.fabric.testsuite.password=
		// com.mysql.fabric.testsuite.database=employees
		// com.mysql.fabric.testsuite.global.host=127.0.0.1
		// com.mysql.fabric.testsuite.global.port=3401
		// com.mysql.fabric.testsuite.shard1.host=127.0.0.1
		// com.mysql.fabric.testsuite.shard1.port=3402
		// com.mysql.fabric.testsuite.shard2.host=127.0.0.1
		// com.mysql.fabric.testsuite.shard2.port=3403

		String baseJdbcUrl = "jdbc:mysql:fabric://" + hostname + ":" + port
				+ "/" + database;
		baseJdbcUrl = baseJdbcUrl + "?fabricUsername=" + fabricUsername
				+ "&fabricPassword=" + fabricPassword;
		baseJdbcUrl = baseJdbcUrl + "&fabricServerGroup=group_id-g";
		Connection c = DriverManager.getConnection(baseJdbcUrl, username,
				password);
		ResultSet rs = c.createStatement().executeQuery(
				"select * from employees limit 100");
		while (rs.next()) {
			String s = rs.getInt(1) + "," + rs.getString(2) + ","
					+ rs.getString(3);
			System.out.println(s);
		}
		rs.close();
		c.close();
	}

	private static void test1() throws Exception {
		String hostname = "192.168.12.220";
		String port = "32274";
		String database = "employees";
		String user = "fabric";
		String password = "123456";

		// String hostname =
		// System.getProperty("com.mysql.fabric.testsuite.hostname");
		// String port = System.getProperty("com.mysql.fabric.testsuite.port");
		// String database =
		// System.getProperty("com.mysql.fabric.testsuite.database");
		// String user =
		// System.getProperty("com.mysql.fabric.testsuite.username");
		// String password =
		// System.getProperty("com.mysql.fabric.testsuite.password");

		String baseUrl = "jdbc:mysql:fabric://" + hostname + ":"
				+ Integer.valueOf(port) + "/";

		// Load the driver if running under Java 5
		if (!com.mysql.jdbc.Util.isJdbc4()) {
			Class.forName("com.mysql.fabric.jdbc.FabricMySQLDriver");
		}

		Class.forName("com.mysql.jdbc.Driver");
		// 1. Create database and table for our demo
		Connection rawConnection = DriverManager.getConnection(baseUrl
				+ "mysql?fabricServerGroup=group_id-g", user, password);
		Statement statement = rawConnection.createStatement();
		statement.executeUpdate("create database if not exists employees");
		statement.close();
		rawConnection.close();

		// We should connect to the global group to run DDL statements,
		// they will be replicated to the server groups for all shards.

		// The 1-st way is to set it's name explicitly via the
		// "fabricServerGroup" connection property
		rawConnection = DriverManager.getConnection(baseUrl + database
				+ "?fabricServerGroup=group_id-g", user, password);
		statement = rawConnection.createStatement();
		statement.executeUpdate("create database if not exists employees");
		statement.close();
		rawConnection.close();

		// The 2-nd way is to get implicitly connected to global group
		// when the shard key isn't provided, ie. set "fabricShardTable"
		// connection property but don't set "fabricShardKey"
		rawConnection = DriverManager.getConnection(baseUrl + "employees"
				+ "?fabricShardTable=employees.employees", user, password);
		// At this point, we have a connection to the global group for
		// the `employees.employees' shard mapping.
		statement = rawConnection.createStatement();
		statement.executeUpdate("drop table if exists employees");
		statement
				.executeUpdate("create table employees (emp_no int not null, first_name varchar(50), last_name varchar(50), primary key (emp_no))");

		// 2. Insert data

		// Cast to a Fabric connection to have access to specific methods
		FabricMySQLConnection connection = (FabricMySQLConnection) rawConnection;

		// example data used to create employee records
		Integer ids[] = new Integer[] { 1, 2, 10001, 10002 };
		String firstNames[] = new String[] { "John", "Jane", "Andy", "Alice" };
		String lastNames[] = new String[] { "Doe", "Doe", "Wiley", "Wein" };

		// insert employee data
		PreparedStatement ps = connection
				.prepareStatement("INSERT INTO employees.employees VALUES (?,?,?)");
		for (int i = 0; i < 4; ++i) {
			// choose the shard that handles the data we interested in
			connection.setShardKey(ids[i].toString());

			// perform insert in standard fashion
			ps.setInt(1, ids[i]);
			ps.setString(2, firstNames[i]);
			ps.setString(3, lastNames[i]);
			ps.executeUpdate();
		}

		// 3. Query the data from employees
		System.out.println("Querying employees");
		System.out.format("%7s | %-30s | %-30s%n", "emp_no", "first_name",
				"last_name");
		System.out
				.println("--------+--------------------------------+-------------------------------");
		ps = connection
				.prepareStatement("select emp_no, first_name, last_name from employees where emp_no = ?");
		for (int i = 0; i < 4; ++i) {

			// we need to specify the shard key before accessing the data
			connection.setShardKey(ids[i].toString());

			ps.setInt(1, ids[i]);
			ResultSet rs = ps.executeQuery();
			rs.next();
			System.out.format("%7d | %-30s | %-30s%n", rs.getInt(1),
					rs.getString(2), rs.getString(3));
			rs.close();
		}
		ps.close();

		// 4. Connect to the global group and clean up
		connection.setServerGroupName("group_id-g");
		statement.executeUpdate("drop table if exists employees");
		statement.close();
		connection.close();
	}
}