package com.test.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Client {

	public void startClient() {
		TTransport transport;
		try {
			transport = new TSocket("localhost", 1234);
			TProtocol protocol = new TBinaryProtocol(transport);
			Test.Client client = new Test.Client(protocol);
			transport.open();
			client.ping(22);
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Client client = new Client();
		client.startClient();
	}
}