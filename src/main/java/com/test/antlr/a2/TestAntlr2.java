package com.test.antlr.a2;

import java.io.DataInputStream;

public class TestAntlr2 {

	public static void main(String[] args) {
		TestAntlr2.test2();
	}

	private static void test2() {
		try {

			L lexer = new L(new DataInputStream(System.in));

			P parser = new P(lexer);

			parser.startRule();

		} catch (Exception e) {

			System.err.println("exception£º " + e);
		}
	}
}