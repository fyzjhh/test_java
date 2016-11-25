package com.test.antlr.a3;

import java.io.FileInputStream;
import java.io.InputStream;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;

public class TestAntlr3 {

	public static void main(String[] args) throws Exception {
		TestAntlr3.test2();
	}

	private static void test2() throws Exception {
		InputStream is = new FileInputStream("E:/Study/antlr/cal.txt");
		ANTLRInputStream input = new ANTLRInputStream(is);
		CalLexer lexer = new CalLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		CalParser parser = new CalParser(tokens);

		System.out.println(parser.expr());

		System.out.println("success=================");
	}
}