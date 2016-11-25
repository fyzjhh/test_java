package com.test.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class TestOracle {
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";

	public static void main(String[] args) throws Exception {
		testOracle();

	}

	public static void testOracle() {
		Connection con = null;// 创建一个数据库连接
		PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
		ResultSet result = null;// 创建一个结果集对象
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
			System.out.println("开始尝试连接数据库！");
			String url = "jdbc:oracle:thin:@192.168.18.210:1521:dh";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
			String user = "stats";// 用户名,系统默认的账户名
			String password = "stats_dh5";// 你安装时选设置的密码
			con = DriverManager.getConnection(url, user, password);// 获取连接
			System.out.println("连接成功！");
			String sql = "select * from emp where emp_name=?";// 预编译语句，“？”代表参数
			pre = con.prepareStatement(sql);// 实例化预编译语句
			pre.setString(1, "allen");// 设置参数，前面的1表示参数的索引，而不是表中列名的索引
			result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
			while (result.next())
				// 当结果集不为空时
				System.out.println("id:" + result.getInt("id") + " emp_name:"
						+ result.getString("emp_name") + " emp_job:"
								+ result.getString("emp_job"));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
				// 注意关闭的顺序，最后使用的最先关闭
				if (result != null)
					result.close();
				if (pre != null)
					pre.close();
				if (con != null)
					con.close();
				System.out.println("数据库连接已关闭！");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void testinsert() throws Exception {
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@192.168.18.210:1521:dh";
		String user = "scott";
		String pass = "tiger";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, user, pass);
		conn.setAutoCommit(false);

		PreparedStatement jhhhdomain = (PreparedStatement) conn
				.prepareStatement("insert into JHH2 values (?,?)");

		Long startTime = System.currentTimeMillis();

		Random random = new Random();

		for (int i = 0; i <= 300; i++) {
			int rnddomains = Math.abs(random.nextInt());

			String d_domainname = getRndStr(rnddomains % 6 + 2);
			String d_descstr = getRndStr(rnddomains % 4 + 6);

			jhhhdomain.setInt(1, i);
			jhhhdomain.setString(2, d_descstr);

			jhhhdomain.executeUpdate();

			if (i % 100 == 0) {
				System.out.println(dtft.format(new Date()) + " user : " + i);
			}
			conn.commit();
		}

		Long endTime = System.currentTimeMillis();
		System.out
				.println("��ʱ��" + dtft.format(new Date(endTime - startTime)));

		if (jhhhdomain != null) {
			jhhhdomain.close();
			jhhhdomain = null;
		}

		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static String getRndStr(int len) {

		Random rndstr = new Random();
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < len; j++) {
			int number = rndstr.nextInt(base.length());
			sb.append(base.charAt(number));
		}
		return sb.toString();
	}

	private static void testconn() throws ClassNotFoundException, SQLException {
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@192.168.1.191:1521:jhhora1";
		String user = "scott";
		String pass = "tiger";
		Class.forName(driver);
		Connection con = DriverManager.getConnection(url, user, pass);
		Statement state = con.createStatement();// ����Statement����
		String sql = "create table jhh2(id int , tno varchar2(10)) ";// ����TEACHER��
		state.execute(sql);

		con.close();
	}
}
