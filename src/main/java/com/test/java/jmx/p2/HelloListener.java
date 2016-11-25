package com.test.java.jmx.p2;

import javax.management.Notification;
import javax.management.NotificationListener;

import com.test.java.jmx.p1.Hello;

public class HelloListener implements NotificationListener {
	public void handleNotification(Notification n, Object handback) {
		System.out.println("type=" + n.getType());
		System.out.println("source=" + n.getSource());
		System.out.println("seq=" + n.getSequenceNumber());
		System.out.println("send time=" + n.getTimeStamp());
		System.out.println("message=" + n.getMessage());

		if (handback != null) {
			if (handback instanceof Hello) {
				Hello hello = (Hello) handback;
				hello.printHello(n.getMessage());
			}
		}
	}
}