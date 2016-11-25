package com.test.dfs.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class VerifyUtil {
	private static Logger logger = Logger.getLogger(VerifyUtil.class);

//	public static boolean compare(InputStream ois, InputStream nis) throws IOException{
//		BufferedReader obr = new BufferedReader(new InputStreamReader(ois));
//		BufferedReader ibr = new BufferedReader(new InputStreamReader(nis));
//		
//		String line1 = obr.readLine();
//		String line2 = ibr.readLine();
//		
//		int i=0;
//		while(line1!=null && line2!=null){
//			i++;
////			logger.debug("line " + i + " line1.len = " + line1.length() + " line2.len = "+ line2.length());
//			if(!(line1.equals(line2))){
//				logger.error("Unequal Content!");
//				logger.error("line1.len = " + line1.length() + " line2.len = "+ line2.length());
//				logger.error("line1 = " + line1);
//				logger.error("line2 = " + line2);
//				return false;
//			}
//			line1 = obr.readLine();
//			line2 = ibr.readLine();
//		}
//		logger.debug("total line = " + i);
//		obr.close();
//		ibr.close();
//		
//		if(line1 != null || line2 != null){
//			logger.error("Unequal Content!");
//			logger.error("line1 = " + line1);
//			logger.error("line2 = " + line2);
//			return false;
//		}else{
//			
//			logger.debug("total Line = " + i);
//			return true;			
//		}
//	}
	
	
	public static boolean compare(InputStream ois, InputStream nis) throws IOException{
		boolean ret = false;
		try {
			String newMD5 = CommonFunction.getMD5(nis);
			String oldMD5 = CommonFunction.getMD5(ois);
			if(newMD5.equals(oldMD5)){
				return true;
			}else{
				logger.error("newMD5 = " + newMD5 + " oldMD5 = "  + oldMD5);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Failed to compare files!");
		}
		return ret;
	}
	
	public static boolean compare(InputStream ois, InputStream nis, int times) throws IOException{
		BufferedReader obr = new BufferedReader(new InputStreamReader(ois));
		BufferedReader ibr = new BufferedReader(new InputStreamReader(nis));
		
		String firstLine = null;
		String preLine1 = null;
		String preLine2 = null;
		String postLine1 = null;
		String postLine2 = null;
		preLine1 = obr.readLine();
		preLine2 = ibr.readLine();
		firstLine = preLine1;
		int j=1;
		if(preLine1 != null){
			postLine1 = obr.readLine();
		}
		if(preLine2 != null){
			postLine2 = ibr.readLine();
		}
		
		boolean result = false;
		int i = 0;
//		for(int i=0; i<times; i++){
//			logger.debug("times = " + i);
			while(preLine1!=null && preLine2!=null){
				logger.debug("line = " + j);
				
				if(postLine1 == null && i!=(times-1)){
					logger.debug((preLine1+firstLine).length() +" "+ preLine2.length());
					if(!(preLine1+firstLine).equals(preLine2)){
						preLine1 = preLine1+firstLine;
						logger.error("Unequal Content!");
						logger.error("Combined line1.len = " + preLine1.length() + " line1 = " + preLine1);
						logger.error("line2.len = " + preLine2.length() + " line2 = " + preLine2);
						return false;
					}else{
						ois.reset();
						obr = new BufferedReader(new InputStreamReader(ois));
						postLine1 = obr.readLine(); 
						postLine1 = obr.readLine(); //跳过第一行
						i++;
					}
				}else{
					logger.debug(preLine1.length() +" "+ preLine2.length());
					if(!(preLine1.equals(preLine2))){
							logger.debug("j = " + j);
							logger.error("Unequal Content!");
							logger.error("line1.len = " + preLine1.length() + " line1 = " + preLine1);
							logger.error("line2.len = " + preLine2.length() + " line2 = " + preLine2);
							return false;
					}
				}
				preLine1 = postLine1;
				preLine2 = postLine2;
				postLine1 = obr.readLine();
				postLine2 = ibr.readLine();
				j++;
			}
			if(preLine1 != null){
				logger.error("preLine1 != null");
				return false;
			}
			if(preLine2 != null){
				logger.error("preLine2 != null");
				return false;
			}
			

//				preLine1 = postLine1;
//				preLine2 = postLine2;
//				if(preLine1 == null){
//					obr = new BufferedReader(new InputStreamReader(ois));
//					preLine1 = obr.readLine();
//				}
//				postLine1 = obr.readLine();
//				postLine2 = ibr.readLine();
//				j++;
//			}
//		}
		obr.close();
		ibr.close();
		return true;
	}
	
	public static boolean compare(String fileName, InputStream nis, int times) throws IOException{
		BufferedReader obr = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		BufferedReader ibr = new BufferedReader(new InputStreamReader(nis));
		
		String firstLine = null;
		String preLine1 = null;
		String preLine2 = null;
		String postLine1 = null;
		String postLine2 = null;
		preLine1 = obr.readLine();
		preLine2 = ibr.readLine();
		firstLine = preLine1;
		int j=1;
		if(preLine1 != null){
			postLine1 = obr.readLine();
		}
		if(preLine2 != null){
			postLine2 = ibr.readLine();
		}
		
		boolean result = false;
		int i = 0;
		while(preLine1!=null && preLine2!=null){
			logger.debug("line = " + j);
			if(postLine1 == null && i!=(times-1)){
				if(!(preLine1+firstLine).equals(preLine2)){
					logger.error("preLine1.length() = " + preLine1.length() + "firstLine = " + firstLine.length());
					preLine1 = preLine1+firstLine;
					logger.error("Unequal Content!Line = " + j);
					logger.error("Combined line1.len = " + preLine1.length() + " line1 = " + preLine1);
					logger.error("line2.len = " + preLine2.length() + " line2 = " + preLine2);
					return false;
				} else{
					obr.close();
					obr = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
					postLine1 = obr.readLine(); 
					postLine1 = obr.readLine(); //跳过第一行
					logger.debug(" time = " + i + "...............................");
					i++;
				}
			}else{
//				logger.debug(preLine1.length() +" "+ preLine2.length());
				if(!(preLine1.equals(preLine2))){
					logger.error("Unequal Content!Line = " + j);
					logger.error("line1.len = " + preLine1.length() + " line1 = " + preLine1);
					logger.error("line2.len = " + preLine2.length() + " line2 = " + preLine2);
					return false;
				}
			}
			preLine1 = postLine1;
			preLine2 = postLine2;
			postLine1 = obr.readLine();
			postLine2 = ibr.readLine();
			j++;
		}
		if(preLine1 != null){
			logger.error("preLine1 != null");
			return false;
		}
		if(preLine2 != null){
			logger.error("preLine2 != null");
			return false;
		}
		obr.close();
		ibr.close();
		return true;
	}

	
	public static boolean compareShorterFile(InputStream ois, InputStream nis, long smallerLen){
		BufferedReader obr = new BufferedReader(new InputStreamReader(ois));
		BufferedReader ibr = new BufferedReader(new InputStreamReader(nis));
		
		String line1;
		try {
			line1 = obr.readLine();
			String line2 = ibr.readLine();
			int count1 = 0;
			int count2 = 0;
			int  totalLen = 0;
			boolean result = false;
			
			while(line1 != null && line2 != null && (line1.length() + totalLen) < smallerLen ){
				count1++;
				count2++;
				
				if(!(line1.equals(line2))){
					logger.error("Unequal Content!");
					logger.error("line1 = " + line1);
					logger.error("line2 = " + line2);
					return false;
				}
				
				totalLen += line1.length();
				logger.debug("line1.length =" + line1.length());
				logger.debug("totalLen = " + totalLen);
				line1 = obr.readLine();
				line2 = ibr.readLine();
			}
			
			if(line1 != null && line2 !=null){
				//
				logger.debug("Last line1.length =" + line1.length());
				logger.debug("Last totalLen = " + totalLen);
				logger.debug("smallerLen = " + smallerLen);
				logger.debug("smallerLen - totalLen = " + (smallerLen - totalLen));

				//
				line1 = line1.substring(0, (int)(smallerLen-totalLen)); 	
				if(line1.equals(line2)){
					result = true;
				}else{
					logger.error("Unequal Content!");
					logger.error("line1.len = " + line1.length() + " line1 = " + line1);
					logger.error("line2.len = " + line2.length() +  " line2 = " + line2);
					result = false;
				}
			}else{
				//
				if(line2 == null){
					logger.debug("totalLen = " + totalLen);
					logger.debug("totalLen = " + smallerLen);
					result = (line1.length() + totalLen) == smallerLen;
				}
			}
			line2 = ibr.readLine();
			logger.debug("line2.len = " + line2.length());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			try {
				obr.close();
				ibr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return false;
	}
	
	public static boolean verifyError(String expectedError, String actualError){
		boolean result = false;
		if(!expectedError.equals(actualError)){
			logger.error("expectedError = " + expectedError + " actualError = " + actualError);
		}else{
			result = true;
		}
		return result;
	}
	
	public static boolean verifyErrorCode(int expectedErrorCode, int actualErrorCode){
		boolean result = false;
		if(expectedErrorCode != actualErrorCode){
			logger.error("expectedErrorCode = " + expectedErrorCode + " actualError = " + actualErrorCode);
		}else{
			result = true;
		}
		return result;
	}

}
