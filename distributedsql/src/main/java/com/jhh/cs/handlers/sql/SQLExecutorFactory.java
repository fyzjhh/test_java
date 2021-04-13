package com.jhh.cs.handlers.sql;

import com.jhh.hdb.proxyserver.session.SessionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * SQLExecutor�๤��
 * 
 *
 */
public interface SQLExecutorFactory {
	public SQLExecutor newExecutor(SessionContext sessionContext, String sql,
                                   Connection con);

	public SQLExecutor newExecutor(SessionContext sessionContext,
                                   PreparedStatement pstmt);
}
