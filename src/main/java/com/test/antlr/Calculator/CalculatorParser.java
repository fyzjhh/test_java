package com.test.antlr.Calculator;

// $ANTLR 3.5.1 D:\\temp\\Calculator.g 2014-08-19 15:14:14

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.tree.*;


@SuppressWarnings("all")
public class CalculatorParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "INT", "MINUS", "PLUS"
	};
	public static final int EOF=-1;
	public static final int INT=4;
	public static final int MINUS=5;
	public static final int PLUS=6;

	// delegates
	public Parser[] getDelegates() {
		return new Parser[] {};
	}

	// delegators


	public CalculatorParser(TokenStream input) {
		this(input, new RecognizerSharedState());
	}
	public CalculatorParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(TreeAdaptor adaptor) {
		this.adaptor = adaptor;
	}
	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}
	@Override public String[] getTokenNames() { return CalculatorParser.tokenNames; }
	@Override public String getGrammarFileName() { return "D:\\temp\\Calculator.g"; }


	public static class plusExpr_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "plusExpr"
	// D:\\temp\\Calculator.g:8:1: plusExpr : INT PLUS ^ INT ;
	public final CalculatorParser.plusExpr_return plusExpr() throws RecognitionException {
		CalculatorParser.plusExpr_return retval = new CalculatorParser.plusExpr_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token INT1=null;
		Token PLUS2=null;
		Token INT3=null;

		CommonTree INT1_tree=null;
		CommonTree PLUS2_tree=null;
		CommonTree INT3_tree=null;

		try {
			// D:\\temp\\Calculator.g:8:10: ( INT PLUS ^ INT )
			// D:\\temp\\Calculator.g:8:12: INT PLUS ^ INT
			{
			root_0 = (CommonTree)adaptor.nil();


			INT1=(Token)match(input,INT,FOLLOW_INT_in_plusExpr35); 
			INT1_tree = (CommonTree)adaptor.create(INT1);
			adaptor.addChild(root_0, INT1_tree);

			PLUS2=(Token)match(input,PLUS,FOLLOW_PLUS_in_plusExpr37); 
			PLUS2_tree = (CommonTree)adaptor.create(PLUS2);
			root_0 = (CommonTree)adaptor.becomeRoot(PLUS2_tree, root_0);

			INT3=(Token)match(input,INT,FOLLOW_INT_in_plusExpr40); 
			INT3_tree = (CommonTree)adaptor.create(INT3);
			adaptor.addChild(root_0, INT3_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "plusExpr"


	public static class minusExpr_return extends ParserRuleReturnScope {
		CommonTree tree;
		@Override
		public CommonTree getTree() { return tree; }
	};


	// $ANTLR start "minusExpr"
	// D:\\temp\\Calculator.g:9:1: minusExpr : INT MINUS ^ INT ;
	public final CalculatorParser.minusExpr_return minusExpr() throws RecognitionException {
		CalculatorParser.minusExpr_return retval = new CalculatorParser.minusExpr_return();
		retval.start = input.LT(1);

		CommonTree root_0 = null;

		Token INT4=null;
		Token MINUS5=null;
		Token INT6=null;

		CommonTree INT4_tree=null;
		CommonTree MINUS5_tree=null;
		CommonTree INT6_tree=null;

		try {
			// D:\\temp\\Calculator.g:9:11: ( INT MINUS ^ INT )
			// D:\\temp\\Calculator.g:9:13: INT MINUS ^ INT
			{
			root_0 = (CommonTree)adaptor.nil();


			INT4=(Token)match(input,INT,FOLLOW_INT_in_minusExpr48); 
			INT4_tree = (CommonTree)adaptor.create(INT4);
			adaptor.addChild(root_0, INT4_tree);

			MINUS5=(Token)match(input,MINUS,FOLLOW_MINUS_in_minusExpr50); 
			MINUS5_tree = (CommonTree)adaptor.create(MINUS5);
			root_0 = (CommonTree)adaptor.becomeRoot(MINUS5_tree, root_0);

			INT6=(Token)match(input,INT,FOLLOW_INT_in_minusExpr53); 
			INT6_tree = (CommonTree)adaptor.create(INT6);
			adaptor.addChild(root_0, INT6_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "minusExpr"

	// Delegated rules



	public static final BitSet FOLLOW_INT_in_plusExpr35 = new BitSet(new long[]{0x0000000000000040L});
	public static final BitSet FOLLOW_PLUS_in_plusExpr37 = new BitSet(new long[]{0x0000000000000010L});
	public static final BitSet FOLLOW_INT_in_plusExpr40 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_INT_in_minusExpr48 = new BitSet(new long[]{0x0000000000000020L});
	public static final BitSet FOLLOW_MINUS_in_minusExpr50 = new BitSet(new long[]{0x0000000000000010L});
	public static final BitSet FOLLOW_INT_in_minusExpr53 = new BitSet(new long[]{0x0000000000000002L});
}
