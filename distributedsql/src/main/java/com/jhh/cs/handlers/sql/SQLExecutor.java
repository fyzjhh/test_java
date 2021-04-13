package com.jhh.cs.handlers.sql;

import com.jhh.hdb.proxyserver.commands.MySQLCmd;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * SQL���ִ����
 * 
 *
 */
public interface SQLExecutor {
	public MySQLCmd executeQuery() throws SQLException;

	public MySQLCmd executeInsert() throws SQLException;

	public MySQLCmd execute() throws SQLException;
	
	public Connection getConnection();
	
	public void setConnection(Connection con) throws SQLException;
}
