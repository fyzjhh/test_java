package com.test.addr;
import java.util.List;

public class Results {
	public Results(){
	}
	
	public List types;
	public String formatted_address;
	public Geometry geometry;
	public AddressComponents[] address_components;

	

	public String getFormatted_address() {
		return formatted_address;
	}

	public void setFormatted_address(String formatted_address) {
		this.formatted_address = formatted_address;
	}

	public Geometry getGeometry() {
		return geometry;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public List getTypes() {
		return types;
	}

	public void setTypes(List types) {
		this.types = types;
	}

	public AddressComponents[] getAddress_components() {
		return address_components;
	}

	public void setAddress_components(AddressComponents[] address_components) {
		this.address_components = address_components;
	}
}