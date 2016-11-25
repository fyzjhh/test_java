package com.test.antlr.java;

import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.debug.BlankDebugEventListener;

import java.io.File;

/** Parse a java file or directory of java files using the generated parser
 *  ANTLR builds from java.g
 */
class Main {
	public static long lexerTime = 0;
	public static boolean profile = true;

	static JavaLexer lexer;

    

	/** Given a String that has a run-length-encoding of some unsigned shorts
	 *  like "\1\2\3\9", convert to short[] {2,9,9,9}.  We do this to avoid
	 *  static short[] which generates so much init code that the class won't
	 *  compile. :(
	 */
	public static short[] unpackEncodedString(String encodedString) {
		// walk first to find how big it is.
		int size = 0;
		for (int i=0; i<encodedString.length(); i+=2) {
			size += encodedString.charAt(i);
		}
		short[] data = new short[size];
		int di = 0;
		for (int i=0; i<encodedString.length(); i+=2) {
			char n = encodedString.charAt(i);
			char v = encodedString.charAt(i+1);
			// add v n times to data
			for (int j=1; j<=n; j++) {
				data[di++] = (short)v;
			}
		}
		return data;
	}

	static String s = "\1\u0030\1\uffff\1\u007f\1\u7fff\1\u8000";
	public static void main(String[] args) {
		test();
		
	}


	private static void test() {
		String[] files={"E:\\Study\\antlr\\examples-v3\\java\\java\\Test.java"};
		try {
			long start = System.currentTimeMillis();
			if (files.length > 0 ) {
				// for each directory/file specified on the command line
				for(int i=0; i< files.length;i++) {
					doFile(new File(files[i])); // parse it
				}
			}
			else {
				System.err.println("Usage: java Main <directory or file name>");
			}
			long stop = System.currentTimeMillis();

			System.out.println("finished parsing OK");
			if ( profile ) {
				System.out.println("num decisions "+profiler.numDecisions);
			}
		}
		catch(Exception e) {
			System.err.println("exception: "+e);
			e.printStackTrace(System.err);   // so we can get stack trace
		}
	}


	// This method decides what action to take based on the type of
	//   file we are looking at
	public static void doFile(File f)
							  throws Exception {
		// If this is a directory, walk each file/dir in that directory
		if (f.isDirectory()) {
			String files[] = f.list();
			for(int i=0; i < files.length; i++)
				doFile(new File(f, files[i]));
		}

		// otherwise, if this is a java file, parse it!
		else if ( ((f.getName().length()>5) &&
				f.getName().substring(f.getName().length()-5).equals(".java"))
			|| f.getName().equals("input") )
		{
		    System.err.println("parsing "+f.getAbsolutePath());
			parseFile(f.getAbsolutePath());
		}
	}

	static class CountDecisions extends BlankDebugEventListener {
		public int numDecisions = 0;
		public void enterDecision(int decisionNumber) {
			numDecisions++;
		}
	}
	static CountDecisions profiler = new CountDecisions();

	// Here's where we do the real work...
	public static void parseFile(String f)
								 throws Exception {
		try {
			// Create a scanner that reads from the input stream passed to us
			if ( lexer==null ) {
				lexer = new JavaLexer();
			}
			lexer.setCharStream(new ANTLRFileStream(f));
			CommonTokenStream tokens = new CommonTokenStream();
//			tokens.discardOffChannelTokens(true);
			tokens.setTokenSource(lexer);
			long start = System.currentTimeMillis();
			tokens.LT(1); // force load
			long stop = System.currentTimeMillis();
			lexerTime += stop-start;

			/*
			long t1 = System.currentTimeMillis();
			tokens.LT(1);
			long t2 = System.currentTimeMillis();
			System.out.println("lexing time: "+(t2-t1)+"ms");
			*/
			System.out.println(tokens);

			// Create a parser that reads from the scanner
			JavaParser parser = null;
			parser = new JavaParser(tokens);

			// start parsing at the compilationUnit rule
			parser.compilationUnit();
			System.err.println("finished "+f);
		}
		catch (Exception e) {
			System.err.println("parser exception: "+e);
			e.printStackTrace();   // so we can get stack trace		
		}
	}
	
}

