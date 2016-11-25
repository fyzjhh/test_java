package com.test.jhh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestGetNewIp {

	public static void main(String[] args) throws Exception {
		deal("E:\\temp\\oldip.txt", "E:\\temp\\newip.txt");

	}

	public static void deal(String fn1, String fn2) throws Exception {
		List<Entry> userentries = new ArrayList<Entry>();
		List<Entry> ipentries = new ArrayList<Entry>();

		File file1 = new File(fn1);
		File file2 = new File(fn2);
		BufferedReader reader1 = null;
		BufferedReader reader2 = null;
		try {
			reader1 = new BufferedReader(new FileReader(file1));
			reader2 = new BufferedReader(new FileReader(file2));
			String tempString = null;
			String user = "";
			String old_client_ip = "";
			String new_client_ip = "";

			while ((tempString = reader1.readLine()) != null) {

				if (tempString.split("\t").length == 4) {
					user = tempString.split("\t")[0];
					old_client_ip = tempString.split("\t")[2];
					Entry e = new Entry();
					e.setUser(user);
					e.setOldip(old_client_ip);
					userentries.add(e);
				}
			}

			while ((tempString = reader2.readLine()) != null) {

				if (tempString.split("\t").length == 5) {
					old_client_ip = tempString.split("\t")[2];
					new_client_ip = tempString.split("\t")[4];
					Entry e = new Entry();
					e.setOldip(old_client_ip);
					e.setNewip(new_client_ip);
					ipentries.add(e);
				}
			}

			reader1.close();
			reader2.close();

			for (Iterator iterator = userentries.iterator(); iterator.hasNext();) {
				Entry entry = (Entry) iterator.next();
				String euser = entry.getUser();
				String eoldip = entry.getOldip();
				for (Iterator it = ipentries.iterator(); it.hasNext();) {
					Entry ipe = (Entry) it.next();
					String inoldip = ipe.getOldip();
					String innewip = ipe.getNewip();

					if (inoldip.equals(eoldip)) {
						System.out.println(euser + "\t" + inoldip + "\t"
								+ innewip);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
