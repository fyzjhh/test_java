package com.test.dfs;

public class DocIDEntity {
	private long docid;
	private long size;

	public DocIDEntity(long docid, long size) {
		this.docid = docid;
		this.size = size;
	}

	public long getDocid() {
		return docid;
	}

	@Override
	public String toString() {
		return "DocIDWrapEntity [docid=" + docid + ", size=" + size + "]";
	}

	public void setDocid(long docid) {
		this.docid = docid;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

}
