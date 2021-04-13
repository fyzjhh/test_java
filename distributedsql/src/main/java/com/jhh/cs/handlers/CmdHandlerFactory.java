package com.jhh.cs.handlers;

import com.jhh.hdb.proxyserver.handlers.mysql.*;
import org.apache.mina.core.service.IoHandler;

public class CmdHandlerFactory {

	public static IoHandler getCmdHandler() {
		CmdHandler handler = new CmdHandler();
		handler.setHandshakeHandler(new HandshakeHandler());
		handler.setAuthenticationHandler(new AuthenticationHandler());
		handler.setInitDBHandler(new InitDBHandler());
		handler.setQueryHandler(new QueryHandler());
//		handler.setPingHandler(new PingHandler());
		handler.setStmtCloseHandler(new StmtCloseHandler());
		handler.setStmtExecuteHandler(new StmtExecuteHandler());
		handler.setStmtFetchHandler(new StmtFetchHandler());
//		handler.setStmtPrepareHandler(new StmtPrepareHandler());
//		handler.setStmtResetHandler(new StmtResetHandler());
//		handler.setStmtLongdataHandler(new StmtSendLongDataHandler());
		return handler;
	}
}
