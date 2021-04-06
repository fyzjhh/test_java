package com.test.dfs;

import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import com.netease.backend.sdfs.rpc.CommonException;
import com.netease.backend.sdfs.rpc.mds.admin.AdminMasterService.Client;

public class ChgMaxID {
	public static void main(String[] args) throws CommonException, TException {
		int[] bs = { 491520};
		String mdsHost = "db-33.photo.163.org";
		int mdsPort = 54163;
		TSocket socket = new TSocket(mdsHost, mdsPort);
		socket.open();
		TBinaryProtocol protocol = new TBinaryProtocol(socket);
		Client client = new Client(protocol);
		client.logIn("sdfs", "jEEaie1thG8GTtDezbo6hX8NFmc=");
		List<Integer> buckets = new LinkedList<Integer>();
		for (int bno : bs) {
			buckets.add(bno);
		}
		client.chgMaxId(buckets, 80000000);
	}
}
