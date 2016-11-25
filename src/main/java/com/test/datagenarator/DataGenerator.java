package com.test.datagenarator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataGenerator {
	static String a = "0|1|11,1|2015-01-01|2015-02-01,11|2015-01-01|2015-02-01,2,3|8,4|1.0|100.0";
	static int cnt = 1000;

	static DateFormat spacedatetimeformat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	static DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

	static String xing_strs = "Smith Jones Williams Taylor Brown Davies Evans Wilson Thomas Johnson Roberts Robinson Thompson Wright Walker White Edwards Hughes Green Hall Lewis Harris Clarke Patel Jackson";
	static String ming_strs = "Jacob Michael Matthew Joshua Christopher Nicholas Andrew Joseph Daniel Daniel Tyler Brandon Ryan Austin William John David Zachary Anthony James Justin Alexander Jonathan Dylan Christian Noah Robert Samuel Kyle Benjamin Jose Jordan Kevin Thomas Nathan Cameron Hunter Ethan Aaron Eric Jason Caleb Logan Brian Luis Adam Juan Steven Jordan Cody Gabriel Connor Timothy Charles Isaiah Jack Carlos Jared Sean Alex Evan Elijah Richard Patrick Nathaniel Isaac Seth Trevor Angel Luke Devin Bryan Jesus Mark Ian Mason Cole Adrian Chase Jeremy Dakota Garrett Antonio Jackson Jesse Blake Dalton Tanner Stephen Alejandro Kenneth Miguel Victor Lucas Spencer Bryce Paul Brendan Jake Tristan Emily Hannah Alexis Samantha Sarah Ashley Madison Taylor Jessica Elizabeth Alyssa Lauren Kayla Brianna Megan Victoria Emma Abigail Rachel Olivia Jennifer Amanda Nicole Sydney Morgan Jasmine Grace Anna Destiny Julia Alexandra Haley Natalie Kaitlyn Katherine Stephanie Brittany Rebecca Maria Allison Amber Savannah Danielle Courtney Mary Gabrielle Brooke Sierra Sara Kimberly Sophia Mackenzie Andrea Michelle Hailey Vanessa Katelyn Katelyn Erin Isabella Shelby Jenna Chloe Melissa Bailey Makayla Paige Mariah Kaylee Kaylee Madeline Caroline Kelsey Kelsey Marissa Breanna Kiara Christina Faith Autumn Laura Tiffany Jacqueline Briana Alexandria Cheyenne Mikayla Cassandra Claire Alexa Sabrina Angela Kathryn Katie Caitlin Isabel Miranda Lindsey Kelly Catherine";
	static int len_xing = xing_strs.split(" +").length;
	static int len_ming = ming_strs.split(" +").length;

	static String char_strs = "0 1 2 3 4 5 6 7 8 9 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z a b c d e f g h i j k l m n o p q r s t u v w x y z";
	static int char_len = char_strs.split(" +").length;

	/*
	 * 0 rand_int 00 递增的数字，increment 1 rand_datetime 11 rand_date 2
	 * rand_xingming 3 rand_password 4 rand_double 5 rand_string
	 */
	public static void main(String[] args) throws Exception {

		gendata();

		System.out.println("====success====");
	}

	private static void gendata() throws Exception {
		String line_sep = System.getProperty("line.separator");
		String field_sep = ",";

		String[] field_schema = a.split(",+");
		for (int i = 0; i < cnt; i++) {
			String tmp = "";

			for (int j = 0; j < field_schema.length; j++) {
				String[] arr = field_schema[j].split("\\|");
				String field_type = arr[0];

				if (field_type.equals("0")) {
					String par1 = arr[1];
					String par2 = arr[2];
					int len = Integer.parseInt(par2) - Integer.parseInt(par1);
					int rand = (int) (Math.random() * len);
					int ret = rand + Integer.parseInt(par1);
					tmp += ret + field_sep;
				}
				if (field_type.equals("00")) {
					tmp += j + field_sep;
				}
				if (field_type.equals("1")) {
					String par1 = arr[1];
					String par2 = arr[2];
					Date de = dateformat.parse(par1);
					Date ds = dateformat.parse(par2);

					int len = (int) ((de.getTime() - ds.getTime()) / 1000L);
					int rand = (int) (Math.random() * len);
					int ret = rand + (int) (ds.getTime() / 1000L);
					Date d = new Date(ret * 1000L);
					tmp += spacedatetimeformat.format(d) + field_sep;
				}
				if (field_type.equals("11")) {
					String par1 = arr[1];
					String par2 = arr[2];
					Date start = dateformat.parse(par1);
					Date stop = dateformat.parse(par2);

					int len = (int) ((stop.getTime() - start.getTime()) / 86400000L);
					int rand = (int) (Math.random() * len);
					int ret = rand + (int) (start.getTime() / 86400000L);
					Date d = new Date(ret * 86400000L);
					tmp += dateformat.format(d) + field_sep;
				}

				if (field_type.equals("2")) {

					int rand_xing = (int) (Math.random() * len_xing);
					int rand_ming = (int) (Math.random() * len_ming);

					tmp += xing_strs.split(" +")[rand_xing] + " "
							+ ming_strs.split(" +")[rand_ming] + field_sep;
				}

				if (field_type.equals("3")) {
					String par1 = arr[1];
					int len = Integer.parseInt(par1);
					String ret = "";

					for (int k = 0; k < len; k++) {
						int rand = (int) (Math.random() * char_len);
						ret += char_strs.split(" +")[rand];
					}
					tmp += ret + field_sep;
				}
				if (field_type.equals("4")) {
					String par1 = arr[1];
					String par2 = arr[2];
					double start = Double.parseDouble(par1);
					double stop = Double.parseDouble(par2);

					double len = (double) (stop - start);
					double rand = (double) (Math.random() * len);
					double ret = rand + start;
					tmp += ret + field_sep;
				}
			}

			tmp =tmp.substring(0, tmp.length()-1)+ line_sep;
			System.out.print(tmp);

		}
	}

}
