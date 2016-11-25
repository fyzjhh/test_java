package com.test.addr;

public class Geometry {
	public Geometry(){
		
	}
	
	public String location_type;
	public Location location;
	public ViewPoint viewport;
	public Bounds bounds;


	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public String getLocation_type() {
		return location_type;
	}

	public void setLocation_type(String location_type) {
		this.location_type = location_type;
	}

	public ViewPoint getViewport() {
		return viewport;
	}

	public void setViewport(ViewPoint viewport) {
		this.viewport = viewport;
	}

	public Bounds getBounds() {
		return bounds;
	}

	public void setBounds(Bounds bounds) {
		this.bounds = bounds;
	}
}