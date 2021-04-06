package com.test.jhh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyListener;

public class TestMonitorDirEvent {

	public static void main(String[] args) throws Exception {
		String dirname = "D:/temp/data/";
		// monitorDir(dirname);
		TestMonitorDirEvent t = new TestMonitorDirEvent();
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
		int mask = JNotify.FILE_CREATED | JNotify.FILE_DELETED
				| JNotify.FILE_MODIFIED | JNotify.FILE_RENAMED;
		boolean watchSubtree = false;
		Listener jnl = new Listener();
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

		Map<String, BufferedReader> file_reader_map = null;
		Map<String, File> file_obj_map = null;
		Map<String, Long> file_last_modified_map = null;
		Map<String, Long> file_size_map = null;

		public Listener() {
			file_reader_map = new HashMap<String, BufferedReader>();
			file_obj_map = new HashMap<String, File>();
			file_last_modified_map = new HashMap<String, Long>();
			file_size_map = new HashMap<String, Long>();
		}

		public void fileRenamed(int wd, String rootPath, String oldName,
				String newName) {
			System.out
					.println("JNotifyTest.fileRenamed() : wd #" + wd
							+ " root = " + rootPath + ", " + oldName + " -> "
							+ newName);
		}

		public void fileModified(int wd, String rootPath, String name) {
			System.out.println("JNotifyTest.fileModified() : wd #" + wd
					+ " root = " + rootPath + ", " + name);

			if (!(rootPath.endsWith("/") || rootPath.endsWith("\\"))) {
				rootPath = rootPath + "/";
			}
			String filename = rootPath + name;

			try {

				long before_last_modified = file_last_modified_map
						.get(filename);
				long before_file_size = file_size_map.get(filename);

				File file = file_obj_map.get(filename);
				BufferedReader br = file_reader_map.get(filename);

				long last_modified = file.lastModified();
				long file_size = file.length();

				// 

				boolean should_read = (file_size - before_file_size) >= 64 && (last_modified - before_last_modified) >= 1 * 1000;
				if (should_read) {
					doRead(br);
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private void doRead(BufferedReader br) throws IOException {
			String line_str = null;
			String ret = "";
			while ((line_str = br.readLine()) != null) {
				ret = ret + line_str + "\n";
			}
			System.out.println(ret);
		}

		public void fileDeleted(int wd, String rootPath, String name) {
			System.out.println("JNotifyTest.fileDeleted() : wd #" + wd
					+ " root = " + rootPath + ", " + name);
		}

		public void fileCreated(int wd, String rootPath, String name) {
			System.out.println("JNotifyTest.fileCreated() : wd #" + wd
					+ " root = " + rootPath + ", " + name);

			try {

				Iterator<String> it = file_reader_map.keySet().iterator();
				while (it.hasNext()) {
					String filename = (String) it.next();
					BufferedReader br = file_reader_map.get(filename);
					doRead(br);
					br.close();
					file_obj_map.remove(filename);
					file_reader_map.remove(filename);
					file_last_modified_map.remove(filename);
					file_size_map.remove(filename);
				}

				if (!(rootPath.endsWith("/") || rootPath.endsWith("\\"))) {
					rootPath = rootPath + "/";
				}
				String filename = rootPath + name;
				File file = new File(filename);
				Long last_modified = file.lastModified();
				Long file_size = file.length();
				BufferedReader br = new BufferedReader(new FileReader(file));

				file_obj_map.put(filename, file);
				file_reader_map.put(filename, br);
				file_last_modified_map.put(filename, last_modified);
				file_size_map.put(filename, file_size);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

}
