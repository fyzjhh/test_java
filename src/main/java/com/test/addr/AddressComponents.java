package com.test.addr;
import java.util.List;


public class AddressComponents {
	public AddressComponents(){
		
	}
	public String long_name;
	public String short_name;
	public List types;
	
	public String getLong_name() {
		return long_name;
	}
	public void setLong_name(String long_name) {
		this.long_name = long_name;
	}
	public String getShort_name() {
		return short_name;
	}
	public void setShort_name(String short_name) {
		this.short_name = short_name;
	}
	public List getTypes() {
		return types;
	}
	public void setTypes(List types) {
		this.types = types;
	}
}