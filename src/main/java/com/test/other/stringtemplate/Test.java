package com.test.stringtemplate;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;
import org.stringtemplate.v4.STGroupString;

public class Test {

	public static void main(String[] args) throws Exception {

		test_3(args);
	}

	private static void test_1(String[] args) throws Exception {
		ST hello = new ST("Hello, <name>");
		hello.add("name", "World");
		System.out.println(hello.render());
	}

	private static void test_2(String[] args) throws Exception {

		String g =
			    "a(x) ::= <<foo>>\n"+
			    "b() ::= <<bar>>\n";
		STGroup group = new STGroupString(g);
		ST st = group.getInstanceOf("a");
		String result = st.render();

		System.out.println(result);
	}
	
	private static void test_3(String[] args) throws Exception {

		STGroup group = new STGroupFile("./src/com/test/stringtemplate/a.stg");
		ST st = group.getInstanceOf("a");
		String result = st.render();

		System.out.println(result);
	}
}
