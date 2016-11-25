package com.test.addr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import net.sf.json.JSONObject;

public class GoogleMapGeocode {
	public void GoogleMapGeocode(){
		
	}
	
	/**
	 * ��ַ����
	 * @param address ��ַ
	 * @return ��γ�ȣ�������磺lat,lng
	 */
	public static String getLatLngByAddress(String address){
		String latLng = "";
		BufferedReader in= null;
		try {
			URL url = new URL("http://maps.google.com/maps/api/geocode/json?address="+URLEncoder.encode(address,"UTF-8")+"&language=zh-CN&sensor=true");
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();   
			httpConn.setDoInput(true);   
			in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));   
		    String line;
		    String result="";
		    while ((line = in.readLine()) != null) {   
		        result += line;   
		    }   
		    in.close();
		    JSONObject jsonObject = JSONObject.fromObject( result );
		    GoogleMapJSONBean bean = (GoogleMapJSONBean) JSONObject.toBean( jsonObject, GoogleMapJSONBean.class );
		    latLng = bean.getResults()[0].getGeometry().getLocation().lat+","+bean.getResults()[0].getGeometry().getLocation().lng;
		    System.out.println(latLng);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(in != null){
				try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return latLng;
	}
	
	/**
	 * �����ַ����
	 * @param latLng ��γ�ȣ���ʽ���磺lat,lng
	 * @return ��ַ
	 */
	public static String getAddressByLatLng(String latLng){
		String address = "";
		BufferedReader in= null;
		try {
			URL url = new URL("http://maps.google.com/maps/api/geocode/json?latlng="+latLng+"&language=zh-CN&sensor=true");
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			httpConn.setDoInput(true);   
			in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));   
		    String line;
		    String result="";
		    while ((line = in.readLine()) != null) {   
		        result += line;   
		    }   
		    in.close();
		    JSONObject jsonObject = JSONObject.fromObject( result );
		    GoogleMapJSONBean bean = (GoogleMapJSONBean) JSONObject.toBean( jsonObject, GoogleMapJSONBean.class );
		    address = bean.getResults()[0].formatted_address;
		    System.out.println("address="+new String(address.getBytes("GBK"),"UTF-8"));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return address;
	}
	
	public static void main(String[] args){
		System.out.println(getLatLngByAddress("浙江省杭州市"));
		System.out.println(getLatLngByAddress("浙江杭州"));
//		getAddressByLatLng("25.1,114.5");
	}
	
}
