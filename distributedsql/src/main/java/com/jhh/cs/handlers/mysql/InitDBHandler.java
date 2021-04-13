package com.jhh.cs.handlers.mysql;

import com.jhh.hdb.proxyserver.commands.InitDBCmd;
import com.jhh.hdb.proxyserver.commands.MySQLCmd;
import com.jhh.hdb.proxyserver.commands.response.OkCmd;
import com.jhh.hdb.proxyserver.session.SessionContext;

/**
 * �����û�������ݿ��������
 * 
 *
 */
public class InitDBHandler implements MySQLCmdHandler {

	public MySQLCmd handleMessage(SessionContext sessionContext, MySQLCmd cmd) {
		InitDBCmd initDBCmd = (InitDBCmd) cmd;
		String database = initDBCmd.getDatabase();
		/*if (database == null || database.length() == 0) {
			return new ErrorCmd(ErrorNoDef.ERROR_GENERAL,
					ErrorNoDef.SQL_GENERAL,
					"database name can not be null or empty");
		}

		if (!GlobalContext.getInstance().getQsConfig().getDdbMap().containsKey(
				database)) {
			return new ErrorCmd(ErrorNoDef.ERROR_GENERAL,
					ErrorNoDef.SQL_GENERAL,
					"database name can not be found on server");
		}

		String dburl = GlobalContext.getInstance().getQsConfig().getDdbMap()
				.get(database);
		try {
			String url = QsUtils.getQsConnectUrl(dburl, sessionContext.getClientIp(),
					sessionContext.getRealPasswordSeed(), 
					GlobalContext.getInstance().getQsConfig().getPort());
			Connection conn = DriverManager.getConnection(url,
					sessionContext.getUsername(), sessionContext
							.getEncryptedPassword());
			
			sessionContext.setDbConnection(conn);
			sessionContext.setPassword(AServer.getInstance().getContext()
					.getDBIContext(database).getClientUserManager().getUser(
							sessionContext.getUsername()).getPassword());

		} catch (SQLException e) {
			return new ErrorCmd(e);
		}*/
		return new OkCmd(sessionContext.getServerStatus().getStatus(), "initDB success");
	}

}
