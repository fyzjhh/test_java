package com.test.java.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Administrator
 * @version
 */
public class TestJavaNio {

	/** Creates new NBTest */
	public TestJavaNio() {
	}

	public void startServer() throws Exception {
		int channels = 0;
		int nKeys = 0;
		int currentSelector = 0;

		// ʹ��Selector
		Selector selector = Selector.open();

		// ����Channel ���󶨵�9000�˿�
		ServerSocketChannel ssc = ServerSocketChannel.open();
		InetSocketAddress address = new InetSocketAddress(
				InetAddress.getLocalHost(), 9000);
		ssc.socket().bind(address);

		// ʹ�趨non-blocking�ķ�ʽ��
		ssc.configureBlocking(false);

		// ��Selectorע��Channel����������Ȥ���¼�
		SelectionKey s = ssc.register(selector, SelectionKey.OP_ACCEPT);
		printKeyInfo(s);

		while (true) // ���ϵ���ѯ
		{
			debug("NBTest: Starting select");

			// Selectorͨ��select����֪ͨ�������Ǹ���Ȥ���¼������ˡ�
			nKeys = selector.select();
			// ���������ע������鷢���ˣ����Ĵ���ֵ�ͻ����0
			if (nKeys > 0) {
				debug("NBTest: Number of keys after select operation: " + nKeys);

				// Selector����һ��SelectionKeys
				// ���Ǵ���Щkey�е�channel()������ȡ�����Ǹո�ע���channel��
				Set selectedKeys = selector.selectedKeys();
				Iterator i = selectedKeys.iterator();
				Map map = new HashMap();
				while (i.hasNext()) {
					s = (SelectionKey) i.next();
					printKeyInfo(s);
					debug("NBTest: Nr Keys in selector: "
							+ selector.keys().size());

					// һ��key��������ɺ󣬾Ͷ����Ӿ����ؼ��֣�ready keys���б��г�ȥ
					i.remove();
					if (s.isAcceptable()) {
						// ��channel()��ȡ�����Ǹո�ע���channel��
						Socket socket = ((ServerSocketChannel) s.channel())
								.accept().socket();
						SocketChannel sc = socket.getChannel();

						sc.configureBlocking(false);
						sc.register(selector, SelectionKey.OP_READ
								| SelectionKey.OP_WRITE);
						// map.put(sc, new Handle());//��socket��handle���а�
						System.out.println(++channels);
					}// ��map�е�handle����read��write�¼�,��ģ�����ļ�ͬʱ��������
					if (s.isReadable() || s.isWritable()) {
						// SocketChannel socketChannel = (SocketChannel)
						// s.channel();
						// final Handle handle = map.get(socketChannel);
						// if(handle != null)
						// handle.handle(s);
					} else {
						debug("NBTest: Channel not acceptable");
					}
				}
			} else {
				debug("NBTest: Select finished without any keys.");
			}

		}

	}

	private static void debug(String s) {
		System.out.println(s);
	}

	private static void printKeyInfo(SelectionKey sk) {
		String s = new String();

		s = "Att: " + (sk.attachment() == null ? "no" : "yes");
		s += ", Read: " + sk.isReadable();
		s += ", Acpt: " + sk.isAcceptable();
		s += ", Cnct: " + sk.isConnectable();
		s += ", Wrt: " + sk.isWritable();
		s += ", Valid: " + sk.isValid();
		s += ", Ops: " + sk.interestOps();
		debug(s);
	}

	/**
	 * @param args
	 *            the command line arguments
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		TestJavaNio nbTest = new TestJavaNio();

		nbTest.testnio();

	}

	public void testnio() throws Exception {
		String infile = "E:\\temp\\zoodata\\version-2\\log.1";
		String outfile = "E:\\temp\\zoodata\\version-2\\log.2";
		// ��ȡԴ�ļ���Ŀ���ļ������������
		FileInputStream fin = new FileInputStream(infile);
		FileOutputStream fout = new FileOutputStream(outfile);
		// ��ȡ�������ͨ��
		FileChannel fcin = fin.getChannel();
		FileChannel fcout = fout.getChannel();
		// ����������
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		while (true) {
			// clear�������軺������ʹ�����Խ��ܶ��������
			buffer.clear();
			// ������ͨ���н����ݶ���������
			int r = fcin.read(buffer);
			// read�������ض�ȡ���ֽ���������Ϊ�㣬�����ͨ���ѵ�������ĩβ���򷵻�-1
			if (r == -1) {
				break;
			}
			// flip�����û��������Խ��¶��������д����һ��ͨ��
			buffer.flip();
			// �����ͨ���н�����д�뻺����
			fcout.write(buffer);
		}
	}

	private Charset charset = Charset.forName("GBK");// ����GBK�ַ���
	private SocketChannel channel;

	public void readHTMLContent() {
		try {
			InetSocketAddress socketAddress = new InetSocketAddress(
					"www.baidu.com", 80);
			// step1:������
			channel = SocketChannel.open(socketAddress);
			// step2:��������ʹ��GBK����
			channel.write(charset.encode("GET " + "/ HTTP/1.1" + "\r\n\r\n"));
			// step3:��ȡ����
			ByteBuffer buffer = ByteBuffer.allocate(1024);// ����1024�ֽڵĻ���
			while (channel.read(buffer) != -1) {
				buffer.flip();// flip�����ڶ��������ֽڲ���֮ǰ���á�
				System.out.println(charset.decode(buffer));
				// ʹ��Charset.decode�������ֽ�ת��Ϊ�ַ���
				buffer.clear();// ��ջ���
			}
		} catch (IOException e) {
			System.err.println(e.toString());
		} finally {
			if (channel != null) {
				try {
					channel.close();
				} catch (IOException e) {
				}
			}
		}
	}

}