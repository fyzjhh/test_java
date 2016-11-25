package com.test.addr;
public class ViewPoint{
	
	public SouthWest southwest;
	public NorthEast northeast;
	
	public ViewPoint(){
		
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