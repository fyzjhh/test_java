package com.test.antlr.a3tree;

import java.io.FileInputStream;
import java.io.InputStream;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;

public class TestAntlr3 {

	public static void main(String[] args) throws Exception {
		TestAntlr3.test2();
	}

	private static void test2() throws Exception {
		InputStream is = new FileInputStream("E:/Study/antlr/cal.txt");
		ANTLRInputStream input = new ANTLRInputStream(is);
		CalculatorLexer lexer = new CalculatorLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		CalculatorParser parser = new CalculatorParser(tokens);

		CommonTree t = (CommonTree) parser.plusExpr().getTree();
		CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
		CalculatorTreeParser walker = new CalculatorTreeParser(nodes);
		System.out.println(walker.expr());
		System.out.println("success=================");
//		InputStream is1 = new FileInputStream("E:/Study/antlr/cal1.txt");
//		ANTLRInputStream input1 = new ANTLRInputStream(is1);
//		CalculatorLexer lexer1 = new CalculatorLexer(input1);
//		CommonTokenStream tokens1 = new CommonTokenStream(lexer1);
//		CalculatorParser parser1 = new CalculatorParser(tokens1);
//		
//		CommonTree t1 = (CommonTree) parser1.minusExpr().getTree();
//		CommonTreeNodeStream nodes1 = new CommonTreeNodeStream(t1);
//		CalculatorTreeParser walker1 = new CalculatorTreeParser(nodes1);
//		System.out.println(walker1.expr1());

	}
}