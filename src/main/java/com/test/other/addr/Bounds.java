package com.test.addr;

public class Bounds {
	public SouthWest southwest;
	public NorthEast northeast;
	
	public Bounds(){
		
	}

	public NorthEast getNortheast() {
		return northeast;
	}

	public void setNortheast(NorthEast northeast) {
		this.northeast = northeast;
	}

	public SouthWest getSouthwest() {
		return southwest;
	}

	public void setSouthwest(SouthWest southwest) {
		this.southwest = southwest;
	}
}
