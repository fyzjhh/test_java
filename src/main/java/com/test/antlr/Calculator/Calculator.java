package com.test.antlr.Calculator;

// $ANTLR 3.5.1 D:\\temp\\Calculator.g 2014-08-19 15:14:14

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;

@SuppressWarnings("all")
public class Calculator {

	public static void main(String[] args) throws Exception {
		InputStream is = new FileInputStream("D:/temp/antlr/cal.txt");
		ANTLRInputStream input = new ANTLRInputStream(is);
		CalculatorLexer lexer = new CalculatorLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		CalculatorParser parser = new CalculatorParser(tokens);

		CommonTree t = (CommonTree) parser.minusExpr().getTree();
		CommonTreeNodeStream nodes = new CommonTreeNodeStream(t);
		CalculatorTreeParser walker = new CalculatorTreeParser(nodes);
		System.out.println(walker.expr_minus());

	}

}
