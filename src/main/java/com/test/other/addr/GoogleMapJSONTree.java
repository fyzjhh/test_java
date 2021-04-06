package com.test.addr;
import java.util.List;


public class GoogleMapJSONTree {
	
	public String status;
	public Result[] results;
	
	public GoogleMapJSONTree(){
		
	}
	
	
	
	public class Result{
		
		public List types;
		public String formatted_address;
		public Geometry geometry;
		public AddressComponents[] address_components;
		
		public Result(){
		}
		
		public class AddressComponents{
			
			public String long_name;
			public String short_name;
			public List types;
			
			public AddressComponents(){
				
			}
			
			
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
		
		public class Geometry{
			
			public String location_type;
			public Location location;
			public ViewPoint viewport;
			public Bounds bounds;
			
			public Geometry(){
				
			}
			
			public class Location{
				
				public String lat;
				public String lng;
				
				public Location(){
					
				}
				
				
				public String getLat() {
					return lat;
				}
				public void setLat(String lat) {
					this.lat = lat;
				}
				public String getLng() {
					return lng;
				}
				public void setLng(String lng) {
					this.lng = lng;
				}
			}
			
			public class ViewPoint{
				
				public SouthWest southwest;
				public NorthEast northeast;
				
				public ViewPoint(){
					
				}
				
				public class SouthWest{
					
					public String lat;
					public String lng;
					
					public SouthWest(){
						
					}
					
					public String getLat() {
						return lat;
					}
					public void setLat(String lat) {
						this.lat = lat;
					}
					public String getLng() {
						return lng;
					}
					public void setLng(String lng) {
						this.lng = lng;
					}
					
				}
				
				public class NorthEast{
					
					public String lat;
					public String lng;
					
					public NorthEast(){
						
					}
					
					public String getLat() {
						return lat;
					}
					public void setLat(String lat) {
						this.lat = lat;
					}
					public String getLng() {
						return lng;
					}
					public void setLng(String lng) {
						this.lng = lng;
					}
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
			
			
			public class Bounds{
				
				public SouthWest southwest;
				public NorthEast northeast;
				
				public Bounds(){
					
				}
				
				public class SouthWest{
					
					public String lat;
					public String lng;
					
					public SouthWest(){
						
					}
					
					public String getLat() {
						return lat;
					}
					public void setLat(String lat) {
						this.lat = lat;
					}
					public String getLng() {
						return lng;
					}
					public void setLng(String lng) {
						this.lng = lng;
					}
					
				}
				
				public class NorthEast{
					
					public String lat;
					public String lng;
					
					public NorthEast(){
						
					}
					
					public String getLat() {
						return lat;
					}
					public void setLat(String lat) {
						this.lat = lat;
					}
					public String getLng() {
						return lng;
					}
					public void setLng(String lng) {
						this.lng = lng;
					}
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

	public Result[] getResults() {
		return results;
	}

	public void setResults(Result[] results) {
		this.results = results;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
