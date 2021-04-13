package com.jhh.cs.commands;

/**
 * 执行ping操作命令
 * 
 *
 */
public class PingCmd extends CommandCmd {

	@Override
	public MySQLCmdType getType() {
		return MySQLCmdType.ping;
	}
	
	@Override
	public String toString() {
		return "[PingCmd] Ping";
	}
}
