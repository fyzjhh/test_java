package com.test.antlr.a277;

import java.io.*;

import antlr.*;

class Test {
	public static void main(String[] args) {
		try {
			
			InputStream is = new FileInputStream("E:\\Study\\antlr\\cal.txt");

			TLexer lexer = new TLexer(new DataInputStream(is));
			boolean done = false;
			while ( !done ) {
				Token t = lexer.nextToken();
				System.out.println("Token: "+t);
				if ( t.getType()==Token.EOF_TYPE ) {
					done = true;
				}
			}
			System.out.println("done lexing...");
		} catch(Exception e) {
			System.err.println("exception: "+e);
		}
	}
}

