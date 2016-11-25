package com.test.addr;



public class GoogleMapJSONBean {
	
	public GoogleMapJSONBean(){
		
	}
	
	public String status;
	public Results[] results;
	
	
	
	
	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public Results[] getResults() {
		return results;
	}
	public void setResults(Results[] results) {
		this.results = results;
	}
}
