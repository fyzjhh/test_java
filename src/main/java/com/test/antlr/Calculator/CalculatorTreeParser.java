package com.test.antlr.Calculator;

// $ANTLR 3.5.1 D:\\temp\\CalculatorTreeParser.g 2014-08-19 15:15:43

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CalculatorTreeParser extends TreeParser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "INT", "MINUS", "PLUS"
	};
	public static final int EOF=-1;
	public static final int INT=4;
	public static final int MINUS=5;
	public static final int PLUS=6;

	// delegates
	public TreeParser[] getDelegates() {
		return new TreeParser[] {};
	}

	// delegators


	public CalculatorTreeParser(TreeNodeStream input) {
		this(input, new RecognizerSharedState());
	}
	public CalculatorTreeParser(TreeNodeStream input, RecognizerSharedState state) {
		super(input, state);
	}

	@Override public String[] getTokenNames() { return CalculatorTreeParser.tokenNames; }
	@Override public String getGrammarFileName() { return "D:\\temp\\CalculatorTreeParser.g"; }



	// $ANTLR start "expr_plus"
	// D:\\temp\\CalculatorTreeParser.g:8:1: expr_plus returns [int value] : ^( PLUS a= INT b= INT ) ;
	public final int expr_plus() throws RecognitionException {
		int value = 0;


		CommonTree a=null;
		CommonTree b=null;

		try {
			// D:\\temp\\CalculatorTreeParser.g:9:5: ( ^( PLUS a= INT b= INT ) )
			// D:\\temp\\CalculatorTreeParser.g:9:7: ^( PLUS a= INT b= INT )
			{
			match(input,PLUS,FOLLOW_PLUS_in_expr_plus42); 
			match(input, Token.DOWN, null); 
			a=(CommonTree)match(input,INT,FOLLOW_INT_in_expr_plus46); 
			b=(CommonTree)match(input,INT,FOLLOW_INT_in_expr_plus50); 
			match(input, Token.UP, null); 


			          int aValue = Integer.parseInt((a!=null?a.getText():null));
			          int bValue = Integer.parseInt((b!=null?b.getText():null));
			          value = aValue + bValue;
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return value;
	}
	// $ANTLR end "expr_plus"



	// $ANTLR start "expr_minus"
	// D:\\temp\\CalculatorTreeParser.g:17:1: expr_minus returns [int value] : ^( MINUS a= INT b= INT ) ;
	public final int expr_minus() throws RecognitionException {
		int value = 0;


		CommonTree a=null;
		CommonTree b=null;

		try {
			// D:\\temp\\CalculatorTreeParser.g:18:5: ( ^( MINUS a= INT b= INT ) )
			// D:\\temp\\CalculatorTreeParser.g:18:7: ^( MINUS a= INT b= INT )
			{
			match(input,MINUS,FOLLOW_MINUS_in_expr_minus84); 
			match(input, Token.DOWN, null); 
			a=(CommonTree)match(input,INT,FOLLOW_INT_in_expr_minus88); 
			b=(CommonTree)match(input,INT,FOLLOW_INT_in_expr_minus92); 
			match(input, Token.UP, null); 


			     int aValue = Integer.parseInt((a!=null?a.getText():null));
			     int bValue = Integer.parseInt((b!=null?b.getText():null));
			     value = aValue - bValue; 
			    
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return value;
	}
	// $ANTLR end "expr_minus"

	// Delegated rules



	public static final BitSet FOLLOW_PLUS_in_expr_plus42 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_INT_in_expr_plus46 = new BitSet(new long[]{0x0000000000000010L});
	public static final BitSet FOLLOW_INT_in_expr_plus50 = new BitSet(new long[]{0x0000000000000008L});
	public static final BitSet FOLLOW_MINUS_in_expr_minus84 = new BitSet(new long[]{0x0000000000000004L});
	public static final BitSet FOLLOW_INT_in_expr_minus88 = new BitSet(new long[]{0x0000000000000010L});
	public static final BitSet FOLLOW_INT_in_expr_minus92 = new BitSet(new long[]{0x0000000000000008L});
}
