package com.test.jhh;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyListener;

public class TestMonitorDirContent {

	public static void main(String[] args) throws Exception {
		String dirname = "D:/temp/data/";
		// monitorDir(dirname);
		TestMonitorDirContent t = new TestMonitorDirContent();
		t.recursiveDir(dirname);
		Thread.sleep(3600 * 1000);
	}

	private void monitorDir(String dirname) throws Exception {
		File rootDir = new File(dirname);
		if (rootDir.isDirectory()) {

			Path rootPath = rootDir.toPath();
			WatchService watcher = FileSystems.getDefault().newWatchService();
			WatchKey key = rootPath.register(watcher,
					StandardWatchEventKinds.ENTRY_MODIFY,
					StandardWatchEventKinds.ENTRY_CREATE,
					StandardWatchEventKinds.ENTRY_DELETE);

			while (true) {
				key = watcher.take();
				for (WatchEvent<?> event : key.pollEvents()) {
					WatchEvent.Kind<?> kind = event.kind();

					if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
						WatchEvent<Path> ev = (WatchEvent<Path>) event;
						Path filename = ev.context();
						System.out.println(filename.toString() + " create");
					}
					if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
						WatchEvent<Path> ev = (WatchEvent<Path>) event;
						Path filename = ev.context();
						System.out.println(filename.toString() + " modify");
					}
					if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
						WatchEvent<Path> ev = (WatchEvent<Path>) event;
						Path filename = ev.context();
						System.out.println(filename.toString() + " delete");
					}
				}
				if (!key.reset()) {
					System.out.println("key.reset() return false");
					break;
				}
			}
		} else {
			System.out.println(dirname + " is not a dir");
		}
	}

	public void recursiveDir(String baseDirStr) throws Exception {
		File baseDir = new File(baseDirStr);
		if (baseDir.isDirectory()) {

			File[] fs = baseDir.listFiles();
			int dir_cnt = 0;
			for (int i = 0; i < fs.length; ++i) {
				File file = fs[i];
				if (file.isDirectory()) {
					dir_cnt = dir_cnt + 1;
				}
			}
			if (dir_cnt == 0) {
				tail(baseDir.getAbsolutePath());
			} else {
				for (int i = 0; i < fs.length; ++i) {
					File file = fs[i];
					if (file.isDirectory()) {
						recursiveDir(file.getAbsolutePath());
					}
				}

			}
		}
	}

	// Thread.sleep(3600 * 1000);
	private void tail(String dirname) throws Exception {
		// to add a watch :
		// String dirname = "/home/omry/tmp";
		// int mask = JNotify.FILE_CREATED | JNotify.FILE_DELETED
		// | JNotify.FILE_MODIFIED | JNotify.FILE_RENAMED;
		int mask = JNotify.FILE_CREATED;
		boolean watchSubtree = false;
		Listener jnl = new Listener(5, "UTF-8");
		int watchID = JNotify.addWatch(dirname, mask, watchSubtree, jnl);
		System.out.println(watchID + "\t" + dirname);

		// to remove watch:
		// boolean res = JNotify.removeWatch(watchID);
		// if (!res) {
		// // invalid watch ID specified.
		// }
	}

	// @SuppressWarnings({ "rawtypes", "unchecked" })
	class Listener implements JNotifyListener {

		String active_filename = null;
		File active_fileobj = null;
		BufferedInputStream active_bis = null;
		int batch_num = 0;
		int read_byte_num = 0;
		String charset = "UTF-8";
		int sleep_second_num = 1 * 1000;
		Pattern p = null;
		boolean read_finished = true;
		int op_type = 0;
		List<String> results = new ArrayList<String>();
		String str = "";

		public Listener(int batch_num, String charset) {
			if (batch_num < 1) {
				this.batch_num = 1;
			}
			if (batch_num > 1000) {
				this.batch_num = 1000;
			}
			this.batch_num = batch_num;
			this.read_byte_num = batch_num * 1024;
			String regEx = "(.*\r?\n){" + batch_num + "," + batch_num + "}";
			this.p = Pattern.compile(regEx);

			this.charset = charset;
		}

		public void fileCreated(int wd, String rootPath, String name) {
			System.out.println("JNotifyTest.fileCreated() : wd #" + wd
					+ " root = " + rootPath + ", " + name);
			try {

				op_type = 2;
				while (read_finished == false) {
					Thread.sleep(sleep_second_num);
				}

				if (!(rootPath.endsWith("/") || rootPath.endsWith("\\"))) {
					rootPath = rootPath + "/";
				}
				active_filename = rootPath + name;
				active_fileobj = new File(active_filename);
				active_bis = new BufferedInputStream(new FileInputStream(
						active_fileobj));
				op_type = 1;
				read_finished = false;

				List<String> rets = doRead();
				while (rets.size() >= batch_num) {
					System.out.println(rets.toString());
					rets = doRead();
				}
				System.out.println(rets.toString());

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void fileDeleted(int wd, String rootPath, String name) {
			// TODO Auto-generated method stub

		}

		@Override
		public void fileModified(int wd, String rootPath, String name) {
			// TODO Auto-generated method stub

		}

		@Override
		public void fileRenamed(int wd, String rootPath, String oldName,
				String newName) {
			// TODO Auto-generated method stub

		}

		public List<String> doRead() throws Exception {

			if (op_type == 1) { // begin


				byte[] available_bytes = new byte[0];

				Matcher m = null;
				while (true) {
					byte[] bytes = new byte[read_byte_num];
					while (active_bis.available() <= read_byte_num) {
						Thread.sleep(sleep_second_num);
					}
					active_bis.read(bytes);
					int pos = available_bytes.length;
					available_bytes = Arrays.copyOf(available_bytes, pos
							+ read_byte_num);
					System.arraycopy(bytes, 0, available_bytes, pos,
							read_byte_num);

					str = str + new String(available_bytes, charset);
					m = p.matcher(str);
					if (m.find()) {
						String[] str_arr = str.split("\r?\n");
						int len = 0;
						List<String> rets = new ArrayList<String>(batch_num);
						for (int i = 0; i < batch_num; i++) {
							rets.add(str_arr[i]);
							len = len + str_arr[i].length();
						}
						str = str.substring(len);
						return rets;
					}
				}
			}
			if (op_type == 2) { // final

				byte[] available_bytes = new byte[0];
				while (true) {
					byte[] bytes = new byte[read_byte_num];
					while (active_bis.available() > 0) {

						int read_byte_num = active_bis.read(bytes);

						available_bytes = Arrays.copyOf(available_bytes,
								available_bytes.length + read_byte_num);
						System.arraycopy(bytes, 0, available_bytes,
								available_bytes.length, read_byte_num);
					}
					read_finished = true;
					str = str + new String(available_bytes, charset);
					String[] str_arr = str.split("\r?\n");
					int len = 0;
					List<String> rets = new ArrayList<String>(batch_num);
					for (int i = 0; i < batch_num; i++) {
						rets.add(str_arr[i]);
						len = len + str_arr[i].length();
					}
					str = str.substring(len);
					return rets;
				}
			}
			return null;
		}

		class Reader implements Callable {

			int op_type = 0;

			public Reader(int op_type) {
				this.op_type = op_type;
			}

			public void set_op_type(int op_type) {
				this.op_type = op_type;
			}

			public List<String> call() throws Exception {
				List<String> rets = new ArrayList<String>();
				if (op_type == 1) { // begin

					byte[] available_bytes = new byte[0];
					String str = null;
					Matcher m = null;
					while (true) {
						byte[] bytes = new byte[read_byte_num];
						while (active_bis.available() <= read_byte_num) {
							Thread.sleep(sleep_second_num);
						}
						active_bis.read(bytes);
						available_bytes = Arrays.copyOf(available_bytes,
								available_bytes.length + read_byte_num);
						System.arraycopy(bytes, 0, available_bytes,
								available_bytes.length, read_byte_num);

						str = new String(available_bytes, charset);
						m = p.matcher(str);
						if (m.find()) {
							String[] str_arr = str.split("\r?\n");
							for (int i = 0; i < str_arr.length; i++) {
								rets.add(str_arr[i]);
							}
							return rets;
						}
					}
				}
				if (op_type == 2) { // final

					byte[] available_bytes = new byte[0];
					String str = null;
					while (true) {
						byte[] bytes = new byte[read_byte_num];
						while (active_bis.available() > 0) {

							int read_byte_num = active_bis.read(bytes);

							available_bytes = Arrays.copyOf(available_bytes,
									available_bytes.length + read_byte_num);
							System.arraycopy(bytes, 0, available_bytes,
									available_bytes.length, read_byte_num);
						}
						str = new String(available_bytes, charset);
						String[] str_arr = str.split("\r?\n");
						for (int i = 0; i < str_arr.length; i++) {
							rets.add(str_arr[i]);
						}
						return rets;

					}
				}
				return rets;
			}
		}
	}

}
