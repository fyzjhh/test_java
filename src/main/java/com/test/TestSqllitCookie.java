package com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TestSqllitCookie {

	public static void main(String[] args) throws Exception {
		test2();
	}

	private static void test1() throws ClassNotFoundException {
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
		Connection connection = null;
		try {
			// create a database connection
			connection = DriverManager
					.getConnection("jdbc:sqlite:D:/data/Cookies");// D:/CF/HttpClient/1.db这个文件就是sqlite文件的存储地址，客户端下载的时候，jsp也是从这个地址获取
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);

			
			

			ResultSet rs = statement.executeQuery("SELECT * FROM sqlite_master WHERE type='table';");
			ResultSetMetaData metadata = rs.getMetaData();
			
			int cols = metadata.getColumnCount();
			for (int i = 0; i < cols; i++) {
				String name = metadata.getColumnName(1+i);
				String type = metadata.getColumnTypeName(1+i);
				System.out.print(name + ","+type+"\t");
			}
			System.out.println();
			while (rs.next()) {
				// read the result set
				String name = rs.getString(1);
				String value = rs.getString(2);
				String host_key = rs.getString(3);
				int creation_utc = rs.getInt(4);
				String path = rs.getString(5);
				System.out.println(name + "\t" + value + "\t[" + creation_utc+ "]\t"+host_key+ "\t"+path);
			}
			
			/*ResultSet rs = statement.executeQuery("SELECT name,value,creation_utc,host_key,path FROM cookies order by host_key , name");
			ResultSetMetaData metadata = rs.getMetaData();
			
			int cols = metadata.getColumnCount();
			for (int i = 0; i < cols; i++) {
				String name = metadata.getColumnName(1+i);
				String type = metadata.getColumnTypeName(1+i);
				System.out.print(name + ","+type+"\t");
			}
			System.out.println();
			while (rs.next()) {
				// read the result set
				String name = rs.getString(1);
				String value = rs.getString(2);
				int creation_utc = rs.getInt(3);
				String host_key = rs.getString(4);
				String path = rs.getString(5);
				System.out.println(name + "\t" + value + "\t[" + creation_utc+ "]\t"+host_key+ "\t"+path);
			}*/
		} catch (SQLException e) {
			// if the error message is "out of memory"
			// it probably means no database file is found
			System.err.println(e.getMessage());
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				// connection close failed
				System.err.println(e);
			}
		}
	}
	

	private static void test2() throws ClassNotFoundException {
		Class.forName("org.sqlite.JDBC");
		Connection connection = null;
		try {
			// create a database connection
			connection = DriverManager
					.getConnection("jdbc:sqlite:D:/data/Cookies");
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);


			
			ResultSet rs = statement.executeQuery("SELECT * FROM cookies order by host_key , name");
			ResultSetMetaData metadata = rs.getMetaData();
			
			int cols = metadata.getColumnCount();
			for (int i = 0; i < cols; i++) {
				String name = metadata.getColumnName(1+i);
				String type = metadata.getColumnTypeName(1+i);
				System.out.print(name + ","+type+"\t");
			}
			System.out.println();
			while (rs.next()) {
				String tmp="";
				for (int i = 0; i < cols; i++) {
					tmp=tmp+"[  "+metadata.getColumnName(1+i)+",,,"+rs.getObject(i+1).toString()+"  ]\t";
				}
				System.out.println(tmp);
			}
		} catch (SQLException e) {
			// if the error message is "out of memory"
			// it probably means no database file is found
			System.err.println(e.getMessage());
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				// connection close failed
				System.err.println(e);
			}
		}
	}
}
