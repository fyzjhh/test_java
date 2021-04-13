package com.jhh.cs.handlers.sql;

import com.jhh.hdb.proxyserver.session.SessionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class SQLExecutorFactoryImpl implements SQLExecutorFactory {

	public SQLExecutor newExecutor(SessionContext sessionContext, String sql,
			Connection con) {
		return new SQLExecutorStmt(sessionContext, sql, con);
	}

	public SQLExecutor newExecutor(SessionContext sessionContext,
			PreparedStatement pstmt) {
		return new SQLExecutorPStmt(sessionContext, pstmt);
	}

}
