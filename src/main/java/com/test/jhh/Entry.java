package com.test.jhh;

import java.util.ArrayList;
import java.util.List;

public class Entry {
	String user;
	String oldip;
	String newip;
	public List<String> newipList = new ArrayList<String>();

	public Entry(String user, String oldip, String newip) {
		super();
		this.user = user;
		this.oldip = oldip;
		this.newip = newip;
	}

	public Entry() {
		super();
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getOldip() {
		return oldip;
	}

	public void setOldip(String oldip) {
		this.oldip = oldip;
	}

	public String getNewip() {
		return newip;
	}

	public void setNewip(String newip) {
		this.newip = newip;
	}
}
