package com.test.db;

import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.*;
import javax.transaction.xa.*;

public class XATransTest {
	public static void main(String[] args) {
		XATransTest mdt = new XATransTest();
		try {
			mdt.test1();
		} catch (Exception ex) {
			System.out.println("");
			Logger.getLogger(XATransTest.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}

	class MyXid implements Xid {
		int formatId;
		byte globalTransactionId[];
		byte branchQualifier[];

		public MyXid() {

		}

		public MyXid(int formatId, byte[] globalTransactionId,
				byte[] branchQualifier) {
			this.formatId = formatId;
			this.globalTransactionId = globalTransactionId;
			this.branchQualifier = branchQualifier;
		}

		public int getFormatId() {
			return this.formatId;
		}

		public void setFormatId(int formatId) {
			this.formatId = formatId;
		}

		public byte[] getGlobalTransactionId() {
			return this.globalTransactionId;
		}

		public void setGlobalTransactionId(byte[] globalTransactionId) {
			this.globalTransactionId = globalTransactionId;
		}

		public byte[] getBranchQualifier() {
			return this.branchQualifier;
		}

		public void setBranchQualifier(byte[] branchQualifier) {
			this.branchQualifier = branchQualifier;
		}
	}

	@SuppressWarnings("static-access")
	public void test1() {
		Connection mysqlCn = null;

		Connection mysqlCn2 = null;

		MysqlXADataSource mysqlDs = null;

		MysqlXADataSource mysqlDs2 = null;

		XAConnection xamysqlCn = null;

		XAConnection xamysqlCn2 = null;

		XAResource xamysqlRes = null;

		XAResource xamysqlRes2 = null;

		Xid mysqlXid = null;

		Xid mysqlXid2 = null;

		Statement mysqlpst = null;

		Statement mysqlpst2 = null;

		try {
			mysqlDs = new MysqlXADataSource();
			mysqlDs.setURL("jdbc:mysql://172.17.2.163:4530/test");
			mysqlDs2 = new MysqlXADataSource();
			mysqlDs2.setURL("jdbc:mysql://172.17.2.163:4531/test");

			xamysqlCn = mysqlDs.getXAConnection("test", "test");
			System.out.println("xamysqlCn: " + xamysqlCn);

			xamysqlCn2 = mysqlDs2.getXAConnection("test", "test");
			System.out.println("xamysqlCn2: " + xamysqlCn2);

			mysqlCn = xamysqlCn.getConnection();

			mysqlCn2 = xamysqlCn2.getConnection();

			mysqlpst = mysqlCn.createStatement();

			mysqlpst2 = mysqlCn2.createStatement();

			xamysqlRes = xamysqlCn.getXAResource();

			xamysqlRes2 = xamysqlCn2.getXAResource();

			mysqlXid = new MyXid(0, new byte[] { 0x01 }, new byte[] { 0x02 });
			mysqlXid2 = new MyXid(0, new byte[] { 0x01 }, new byte[] { 0x04 });

			xamysqlRes.start(mysqlXid, XAResource.TMNOFLAGS);
			mysqlpst.executeUpdate("insert into t values(1002,'Class4')");
			xamysqlRes.end(mysqlXid, XAResource.TMSUCCESS);

			xamysqlRes2.start(mysqlXid2, XAResource.TMNOFLAGS);
			mysqlpst2
					.executeUpdate("insert into t values(4002,'Class111')");
			xamysqlRes2.end(mysqlXid2, XAResource.TMSUCCESS);

			int mysqlRea = xamysqlRes.prepare(mysqlXid);

			int mysqlRea2 = xamysqlRes2.prepare(mysqlXid2);

			if (mysqlRea == xamysqlRes.XA_OK && mysqlRea2 == xamysqlRes.XA_OK) {

				xamysqlRes.commit(mysqlXid, false);
				System.out.println("Mysql �����ύ�ɹ���");

				xamysqlRes2.commit(mysqlXid2, false);
				System.out.println("Mysql2 �����ύ�ɹ���");

			} else {
				xamysqlRes.rollback(mysqlXid);

				xamysqlRes2.rollback(mysqlXid2);

				System.out.println("����ع��ɹ���");
			}
		} catch (SQLException ex) {
			Logger.getLogger(XATransTest.class.getName()).log(
					Level.SEVERE, null, ex);
			try {
				xamysqlRes.rollback(mysqlXid);

				xamysqlRes2.rollback(mysqlXid2);

			} catch (XAException e) {
				System.out.println("�ع�Ҳ������~");
				e.printStackTrace();
			}
		} catch (XAException ex) {
			Logger.getLogger(XATransTest.class.getName()).log(
					Level.SEVERE, null, ex);
		} finally {
			try {
				mysqlpst.close();
				mysqlCn.close();
				xamysqlCn.close();

				mysqlpst2.close();
				mysqlCn2.close();
				xamysqlCn2.close();

			} catch (SQLException ex) {
				Logger.getLogger(XATransTest.class.getName()).log(
						Level.SEVERE, null, ex);
			}
		}
	}
}
