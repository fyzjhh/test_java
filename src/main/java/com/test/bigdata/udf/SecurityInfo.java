package com.test.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UserLevel.
 * 
 */
@Description(name = "SecurityInfo", value = "_FUNC_(str) - returns security info", extended = "Example:\n select id, SecurityInfo(sec_info) sec_info from t limit 10;")
public class SecurityInfo extends UDF {

	public String evaluate(final long secinfo) {
		//二进制各位上对应分数，从0开始计数
		Map<String,String> socreMap = new HashMap<String,String>();
		socreMap.put("0", "0");//弱密码
		socreMap.put("1", "5");//一般密码
		socreMap.put("2", "10");//复杂密码
		socreMap.put("3", "10");//身份证信息
		socreMap.put("4", "10");//密保问题
		socreMap.put("5", "15");//安全邮箱
		socreMap.put("6", "20");//手机绑定
		socreMap.put("7", "60");//密保卡
		socreMap.put("8", "80");//实物统军令
		socreMap.put("9", "80");//手机统军令
		socreMap.put("10", "0");//登录验证
		
		//将传入的安全信息转为二进制，并按个位、十位、百位的顺序排列
		String binStr = Long.toBinaryString(secinfo);
		String secFlag="";//存储结果
		StringBuffer sectmp = new StringBuffer();
		int socre = 0;
		int strLen = binStr.length();
		int mbFlag = 0;//绑定手机标识位
		int tjlFlag = 0;//绑定统军令标识位，含统军令实物、手机统军令
		int jcFlag = 1;//基础资料是否最大分数
		int mmFlag = 0;//密码项是否最大分数
		//解析二进制
		for(int i=0;i<strLen;i++){
			int ps = strLen-1-i;
			int pe = strLen-i;
			String valStr = binStr.substring(ps, pe);
			int valInt = Integer.parseInt(valStr);
			if(valInt == 1){
				socre = socre + Integer.parseInt(socreMap.get(Integer.toString(i)));
			}
//			System.out.println(i+"-"+valStr+"--当前分数"+socre);
			if(i==2){
				//前三项为密码相关，如果为复杂密码则得分最高
				mmFlag = valInt;
			}else if(i>=3 && i<=6 && valInt==0){
				//3~6这四项为基础资料，需要均为1时认为基础资料得分最高
				jcFlag = 0;
			}else if(i==7){
//				System.out.println("绑定密保标识位："+valInt);
				mbFlag = valInt;
			}else if(tjlFlag == 0 && (i==8 || i==9)){
//				System.out.println("绑定统军令标识位："+valInt);
				tjlFlag = valInt;
			}
			//从个位开始取数
			sectmp.append(valStr);
			if(i<(strLen-1)){
				sectmp.append(",");
			}
		}
		if(tjlFlag == 0 && mbFlag == 1 && socre>75){
			//如果只绑定了密保没有绑定统军令则最高评分75
			socre = 75;
		}else if(socre>95 && tjlFlag == 1 && (jcFlag ==0 || mmFlag ==0)){
			//如果绑定了统军令，密码和基本资料不是最高的则最高95分
			socre = 95;
		}else if(socre>100){
			//最高分为100
			socre = 100;
		}
		if(strLen<11){
			for(int i=0;i<(11-strLen);i++){
				sectmp.append(",0");
			}
		}
		//输出结果：评分,个位,十位,百位,千位...
		secFlag = Integer.toString(socre)+","+sectmp.toString();
		return secFlag;
	}
}
