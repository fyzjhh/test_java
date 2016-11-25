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

		// 使用Selector
		Selector selector = Selector.open();

		// 建立Channel 并绑定到9000端口
		ServerSocketChannel ssc = ServerSocketChannel.open();
		InetSocketAddress address = new InetSocketAddress(
				InetAddress.getLocalHost(), 9000);
		ssc.socket().bind(address);

		// 使设定non-blocking的方式。
		ssc.configureBlocking(false);

		// 向Selector注册Channel及我们有兴趣的事件
		SelectionKey s = ssc.register(selector, SelectionKey.OP_ACCEPT);
		printKeyInfo(s);

		while (true) // 不断的轮询
		{
			debug("NBTest: Starting select");

			// Selector通过select方法通知我们我们感兴趣的事件发生了。
			nKeys = selector.select();
			// 如果有我们注册的事情发生了，它的传回值就会大于0
			if (nKeys > 0) {
				debug("NBTest: Number of keys after select operation: " + nKeys);

				// Selector传回一组SelectionKeys
				// 我们从这些key中的channel()方法中取得我们刚刚注册的channel。
				Set selectedKeys = selector.selectedKeys();
				Iterator i = selectedKeys.iterator();
				Map map = new HashMap();
				while (i.hasNext()) {
					s = (SelectionKey) i.next();
					printKeyInfo(s);
					debug("NBTest: Nr Keys in selector: "
							+ selector.keys().size());

					// 一个key被处理完成后，就都被从就绪关键字（ready keys）列表中除去
					i.remove();
					if (s.isAcceptable()) {
						// 从channel()中取得我们刚刚注册的channel。
						Socket socket = ((ServerSocketChannel) s.channel())
								.accept().socket();
						SocketChannel sc = socket.getChannel();

						sc.configureBlocking(false);
						sc.register(selector, SelectionKey.OP_READ
								| SelectionKey.OP_WRITE);
						// map.put(sc, new Handle());//把socket和handle进行绑定
						System.out.println(++channels);
					}// 用map中的handle处理read和write事件,以模拟多个文件同时进行下载
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
		// 获取源文件和目标文件的输入输出流
		FileInputStream fin = new FileInputStream(infile);
		FileOutputStream fout = new FileOutputStream(outfile);
		// 获取输入输出通道
		FileChannel fcin = fin.getChannel();
		FileChannel fcout = fout.getChannel();
		// 创建缓冲区
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		while (true) {
			// clear方法重设缓冲区，使它可以接受读入的数据
			buffer.clear();
			// 从输入通道中将数据读到缓冲区
			int r = fcin.read(buffer);
			// read方法返回读取的字节数，可能为零，如果该通道已到达流的末尾，则返回-1
			if (r == -1) {
				break;
			}
			// flip方法让缓冲区可以将新读入的数据写入另一个通道
			buffer.flip();
			// 从输出通道中将数据写入缓冲区
			fcout.write(buffer);
		}
	}

	private Charset charset = Charset.forName("GBK");// 创建GBK字符集
	private SocketChannel channel;

	public void readHTMLContent() {
		try {
			InetSocketAddress socketAddress = new InetSocketAddress(
					"www.baidu.com", 80);
			// step1:打开连接
			channel = SocketChannel.open(socketAddress);
			// step2:发送请求，使用GBK编码
			channel.write(charset.encode("GET " + "/ HTTP/1.1" + "\r\n\r\n"));
			// step3:读取数据
			ByteBuffer buffer = ByteBuffer.allocate(1024);// 创建1024字节的缓冲
			while (channel.read(buffer) != -1) {
				buffer.flip();// flip方法在读缓冲区字节操作之前调用。
				System.out.println(charset.decode(buffer));
				// 使用Charset.decode方法将字节转换为字符串
				buffer.clear();// 清空缓冲
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