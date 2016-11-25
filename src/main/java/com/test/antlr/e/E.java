package com.test.antlr.e;

import java.io.FileInputStream;
import java.io.InputStream;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.BaseTree;

public class E {
	public static void main(String[] args) throws Exception {
		InputStream is = new FileInputStream("D:/temp/antlr/cal.txt");
		ANTLRInputStream input = new ANTLRInputStream(is);
		ELexer lexer = new ELexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		EParser parser = new EParser(tokens);
		EParser.program_return r = parser.program();
		System.out.println(((BaseTree) r.getTree()).toStringTree());
	}
}
