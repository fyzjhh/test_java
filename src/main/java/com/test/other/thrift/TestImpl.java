package com.test.thrift;

import org.apache.thrift.TException;


public class TestImpl implements Test.Iface {

   public void ping(int length) throws TException {
       System.out.println("calling ping ,length=" + length);
   }

}
