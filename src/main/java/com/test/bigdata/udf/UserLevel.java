package com.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UserLevel.
 * 
 */
@Description(name = "userlevel", value = "_FUNC_(str) - returns userlevel", extended = "Example:\n select id, userlevel(score) userlevel from t limit 10;")
public class UserLevel extends UDF {

	public String evaluate(final long score, final int mode) {
		String ret = "unkown";
		if (mode == 0) {
			if (score < 0) {
				ret = "unkown";
			} else if (score >= 0 && score <= 999) {
				ret = "01";
			} else if (score >= 1000 && score <= 4999) {
				ret = "02";
			} else if (score >= 5000 && score <= 9999) {
				ret = "03";
			} else if (score >= 10000 && score <= 15999) {
				ret = "04";
			} else if (score >= 16000 && score <= 23499) {
				ret = "05";
			} else if (score >= 23500 && score <= 33999) {
				ret = "06";
			} else if (score >= 34000 && score <= 47499) {
				ret = "07";
			} else if (score >= 47500 && score <= 63999) {
				ret = "08";
			} else if (score >= 64000 && score <= 83499) {
				ret = "09";
			} else if (score >= 83500 && score <= 113499) {
				ret = "10";
			} else if (score >= 113500 && score <= 155499) {
				ret = "11";
			} else if (score >= 155500 && score <= 209499) {
				ret = "12";
			} else if (score >= 209500 && score <= 275499) {
				ret = "13";
			} else if (score >= 275500 && score <= 353499) {
				ret = "14";
			} else if (score >= 353500 && score <= 533499) {
				ret = "15";
			} else if (score >= 533500 && score <= 785499) {
				ret = "16";
			} else if (score >= 785500 && score <= 1109499) {
				ret = "17";
			} else if (score >= 1109500 && score <= 1505499) {
				ret = "18";
			} else if (score >= 1505500 && score <= 1973499) {
				ret = "19";
			} else if (score >= 1973500 && score <= 2333499) {
				ret = "20";
			} else if (score >= 2333500 && score <= 2837499) {
				ret = "21";
			} else if (score >= 2837500 && score <= 3485499) {
				ret = "22";
			} else if (score >= 3485500 && score <= 4277499) {
				ret = "23";
			} else if (score >= 4277500 && score <= 5213499) {
				ret = "24";
			} else if (score >= 5213500 && score <= 25447499) {
				ret = "25";
			} else if (score >= 25447500 && score <= 50894999) {
				ret = "26";
			} else if (score >= 50895000 && score <= 101789999) {
				ret = "27";
			} else if (score >= 101790000 && score <= 203579999) {
				ret = "28";
			} else if (score >= 203580000 && score <= 407159999) {
				ret = "29";
			} else if (score >= 407160000) {
				ret = "30";
			}
		}
		if (mode == 1) {
			if (score < 0) {
				ret = "unkown";
			} else if (score >= 0 && score <= 999) {
				ret = "白身";
			} else if (score >= 1000 && score <= 4999) {
				ret = "平民";
			} else if (score >= 5000 && score <= 9999) {
				ret = "壮士";
			} else if (score >= 10000 && score <= 15999) {
				ret = "乡豪";
			} else if (score >= 16000 && score <= 23499) {
				ret = "豪杰";
			} else if (score >= 23500 && score <= 33999) {
				ret = "门将";
			} else if (score >= 34000 && score <= 47499) {
				ret = "武将";
			} else if (score >= 47500 && score <= 63999) {
				ret = "副将";
			} else if (score >= 64000 && score <= 83499) {
				ret = "主将";
			} else if (score >= 83500 && score <= 113499) {
				ret = "大将";
			} else if (score >= 113500 && score <= 155499) {
				ret = "名将";
			} else if (score >= 155500 && score <= 209499) {
				ret = "都尉";
			} else if (score >= 209500 && score <= 275499) {
				ret = "校尉";
			} else if (score >= 275500 && score <= 353499) {
				ret = "五官中郎将";
			} else if (score >= 353500 && score <= 533499) {
				ret = "前将军";
			} else if (score >= 533500 && score <= 785499) {
				ret = "五虎上将";
			} else if (score >= 785500 && score <= 1109499) {
				ret = "镇国将军";
			} else if (score >= 1109500 && score <= 1505499) {
				ret = "大都督";
			} else if (score >= 1505500 && score <= 1973499) {
				ret = "骠骑大将军";
			} else if (score >= 1973500 && score <= 2333499) {
				ret = "三军大元帅";
			} else if (score >= 2333500 && score <= 2837499) {
				ret = "太守";
			} else if (score >= 2837500 && score <= 3485499) {
				ret = "刺史";
			} else if (score >= 3485500 && score <= 4277499) {
				ret = "州牧";
			} else if (score >= 4277500 && score <= 5213499) {
				ret = "诸侯";
			} else if (score >= 5213500 && score <= 25447499) {
				ret = "世子";
			} else if (score >= 25447500 && score <= 50894999) {
				ret = "王";
			} else if (score >= 50895000 && score <= 101789999) {
				ret = "皇帝";
			} else if (score >= 101790000 && score <= 203579999) {
				ret = "仙人";
			} else if (score >= 203580000 && score <= 407159999) {
				ret = "魔神";
			} else if (score >= 407160000) {
				ret = "游戏开发者";
			}
		}

		if (mode == 2) {
			if (score < 0) {
				ret = "unkown";
			} else if (score >= 0 && score <= 999) {
				ret = "01_白身";
			} else if (score >= 1000 && score <= 4999) {
				ret = "02_平民";
			} else if (score >= 5000 && score <= 9999) {
				ret = "03_壮士";
			} else if (score >= 10000 && score <= 15999) {
				ret = "04_乡豪";
			} else if (score >= 16000 && score <= 23499) {
				ret = "05_豪杰";
			} else if (score >= 23500 && score <= 33999) {
				ret = "06_门将";
			} else if (score >= 34000 && score <= 47499) {
				ret = "07_武将";
			} else if (score >= 47500 && score <= 63999) {
				ret = "08_副将";
			} else if (score >= 64000 && score <= 83499) {
				ret = "09_主将";
			} else if (score >= 83500 && score <= 113499) {
				ret = "10_大将";
			} else if (score >= 113500 && score <= 155499) {
				ret = "11_名将";
			} else if (score >= 155500 && score <= 209499) {
				ret = "12_都尉";
			} else if (score >= 209500 && score <= 275499) {
				ret = "13_校尉";
			} else if (score >= 275500 && score <= 353499) {
				ret = "14_五官中郎将";
			} else if (score >= 353500 && score <= 533499) {
				ret = "15_前将军";
			} else if (score >= 533500 && score <= 785499) {
				ret = "16_五虎上将";
			} else if (score >= 785500 && score <= 1109499) {
				ret = "17_镇国将军";
			} else if (score >= 1109500 && score <= 1505499) {
				ret = "18_大都督";
			} else if (score >= 1505500 && score <= 1973499) {
				ret = "19_骠骑大将军";
			} else if (score >= 1973500 && score <= 2333499) {
				ret = "20_三军大元帅";
			} else if (score >= 2333500 && score <= 2837499) {
				ret = "21_太守";
			} else if (score >= 2837500 && score <= 3485499) {
				ret = "22_刺史";
			} else if (score >= 3485500 && score <= 4277499) {
				ret = "23_州牧";
			} else if (score >= 4277500 && score <= 5213499) {
				ret = "24_诸侯";
			} else if (score >= 5213500 && score <= 25447499) {
				ret = "25_世子";
			} else if (score >= 25447500 && score <= 50894999) {
				ret = "26_王";
			} else if (score >= 50895000 && score <= 101789999) {
				ret = "27_皇帝";
			} else if (score >= 101790000 && score <= 203579999) {
				ret = "28_仙人";
			} else if (score >= 203580000 && score <= 407159999) {
				ret = "29_魔神";
			} else if (score >= 407160000) {
				ret = "30_游戏开发者";
			}
		}
		return ret;
	}

	public String evaluate(final long score) {
		String ret = "unkown";
		if (score < 0) {
			ret = "unkown";
		} else if (score >= 0 && score <= 999) {
			ret = "01";
		} else if (score >= 1000 && score <= 4999) {
			ret = "02";
		} else if (score >= 5000 && score <= 9999) {
			ret = "03";
		} else if (score >= 10000 && score <= 15999) {
			ret = "04";
		} else if (score >= 16000 && score <= 23499) {
			ret = "05";
		} else if (score >= 23500 && score <= 33999) {
			ret = "06";
		} else if (score >= 34000 && score <= 47499) {
			ret = "07";
		} else if (score >= 47500 && score <= 63999) {
			ret = "08";
		} else if (score >= 64000 && score <= 83499) {
			ret = "09";
		} else if (score >= 83500 && score <= 113499) {
			ret = "10";
		} else if (score >= 113500 && score <= 155499) {
			ret = "11";
		} else if (score >= 155500 && score <= 209499) {
			ret = "12";
		} else if (score >= 209500 && score <= 275499) {
			ret = "13";
		} else if (score >= 275500 && score <= 353499) {
			ret = "14";
		} else if (score >= 353500 && score <= 533499) {
			ret = "15";
		} else if (score >= 533500 && score <= 785499) {
			ret = "16";
		} else if (score >= 785500 && score <= 1109499) {
			ret = "17";
		} else if (score >= 1109500 && score <= 1505499) {
			ret = "18";
		} else if (score >= 1505500 && score <= 1973499) {
			ret = "19";
		} else if (score >= 1973500 && score <= 2333499) {
			ret = "20";
		} else if (score >= 2333500 && score <= 2837499) {
			ret = "21";
		} else if (score >= 2837500 && score <= 3485499) {
			ret = "22";
		} else if (score >= 3485500 && score <= 4277499) {
			ret = "23";
		} else if (score >= 4277500 && score <= 5213499) {
			ret = "24";
		} else if (score >= 5213500 && score <= 25447499) {
			ret = "25";
		} else if (score >= 25447500 && score <= 50894999) {
			ret = "26";
		} else if (score >= 50895000 && score <= 101789999) {
			ret = "27";
		} else if (score >= 101790000 && score <= 203579999) {
			ret = "28";
		} else if (score >= 203580000 && score <= 407159999) {
			ret = "29";
		} else if (score >= 407160000) {
			ret = "30";
		}

		return ret;
	}
}
