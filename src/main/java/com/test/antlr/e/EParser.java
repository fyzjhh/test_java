package com.test.antlr.e;

// $ANTLR 3.5.1 D:\\temp\\E.g 2014-08-19 14:58:49

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.tree.*;


@SuppressWarnings("all")
public class EParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "INT", "STRING", "VAR", "WS", 
		"'('", "')'", "'*'", "'+'", "'-'", "';'", "'='"
	};
	public static final int EOF=-1;
	public static final int T__8=8;
	public static final int T__9=9;
	public static final int T__10=10;
	public static final int T__11=11;
	public static final int T__12=12;
	public static final int T__13=13;
	public static final int T__14=14;
	public static final int INT=4;
	public static final int STRING=5;
	public static final int VAR=6;
	public static final int WS=7;

	// delegates
	public Parser[] getDelegates() {
		return new Parser[] {};
	}

	// delegators


	public EParser(TokenStream input) {
		this(input, new RecognizerSharedState());
	}
	public EParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(TreeAdaptor adaptor) {
		this.adaptor = adaptor;
	}
	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}
	@Override public String[] getTokenNames() { return EParser.tokenNames; }
	@Override public String getGrammarFileName() { return "D:\\temp\\E.g"; }


	public static class program_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "program"
	// D:\\temp\\E.g:3:1: program : ( statement )+ ;
	public final EParser.program_return program() throws RecognitionException {
		EParser.program_return retval = new EParser.program_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		ParserRuleReturnScope statement1 =null;


		try {
			// D:\\temp\\E.g:3:9: ( ( statement )+ )
			// D:\\temp\\E.g:3:11: ( statement )+
			{
			root_0 = (Object)adaptor.nil();


			// D:\\temp\\E.g:3:11: ( statement )+
			int cnt1=0;
			loop1:
			while (true) {
				int alt1=2;
				int LA1_0 = input.LA(1);
				if ( ((LA1_0 >= INT && LA1_0 <= VAR)||LA1_0==8) ) {
					alt1=1;
				}

				switch (alt1) {
				case 1 :
					// D:\\temp\\E.g:3:11: statement
					{
					pushFollow(FOLLOW_statement_in_program17);
					statement1=statement();
					state._fsp--;

					adaptor.addChild(root_0, statement1.getTree());

					}
					break;

				default :
					if ( cnt1 >= 1 ) break loop1;
					EarlyExitException eee = new EarlyExitException(1, input);
					throw eee;
				}
				cnt1++;
			}

			}

			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "program"


	public static class statement_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "statement"
	// D:\\temp\\E.g:4:1: statement : ( expression | VAR '=' expression ) ';' ;
	public final EParser.statement_return statement() throws RecognitionException {
		EParser.statement_return retval = new EParser.statement_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		Token VAR3=null;
		Token char_literal4=null;
		Token char_literal6=null;
		ParserRuleReturnScope expression2 =null;
		ParserRuleReturnScope expression5 =null;

		Object VAR3_tree=null;
		Object char_literal4_tree=null;
		Object char_literal6_tree=null;

		try {
			// D:\\temp\\E.g:4:10: ( ( expression | VAR '=' expression ) ';' )
			// D:\\temp\\E.g:4:12: ( expression | VAR '=' expression ) ';'
			{
			root_0 = (Object)adaptor.nil();


			// D:\\temp\\E.g:4:12: ( expression | VAR '=' expression )
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( ((LA2_0 >= INT && LA2_0 <= STRING)||LA2_0==8) ) {
				alt2=1;
			}
			else if ( (LA2_0==VAR) ) {
				alt2=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}

			switch (alt2) {
				case 1 :
					// D:\\temp\\E.g:4:13: expression
					{
					pushFollow(FOLLOW_expression_in_statement27);
					expression2=expression();
					state._fsp--;

					adaptor.addChild(root_0, expression2.getTree());

					}
					break;
				case 2 :
					// D:\\temp\\E.g:4:26: VAR '=' expression
					{
					VAR3=(Token)match(input,VAR,FOLLOW_VAR_in_statement31); 
					VAR3_tree = (Object)adaptor.create(VAR3);
					adaptor.addChild(root_0, VAR3_tree);

					char_literal4=(Token)match(input,14,FOLLOW_14_in_statement33); 
					char_literal4_tree = (Object)adaptor.create(char_literal4);
					adaptor.addChild(root_0, char_literal4_tree);

					pushFollow(FOLLOW_expression_in_statement35);
					expression5=expression();
					state._fsp--;

					adaptor.addChild(root_0, expression5.getTree());

					}
					break;

			}

			char_literal6=(Token)match(input,13,FOLLOW_13_in_statement38); 
			char_literal6_tree = (Object)adaptor.create(char_literal6);
			adaptor.addChild(root_0, char_literal6_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "statement"


	public static class expression_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "expression"
	// D:\\temp\\E.g:5:1: expression : ( ( multExpr ( ( '+' | '-' ) multExpr )* ) | STRING );
	public final EParser.expression_return expression() throws RecognitionException {
		EParser.expression_return retval = new EParser.expression_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		Token set8=null;
		Token STRING10=null;
		ParserRuleReturnScope multExpr7 =null;
		ParserRuleReturnScope multExpr9 =null;

		Object set8_tree=null;
		Object STRING10_tree=null;

		try {
			// D:\\temp\\E.g:5:12: ( ( multExpr ( ( '+' | '-' ) multExpr )* ) | STRING )
			int alt4=2;
			int LA4_0 = input.LA(1);
			if ( (LA4_0==INT||LA4_0==8) ) {
				alt4=1;
			}
			else if ( (LA4_0==STRING) ) {
				alt4=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 4, 0, input);
				throw nvae;
			}

			switch (alt4) {
				case 1 :
					// D:\\temp\\E.g:5:14: ( multExpr ( ( '+' | '-' ) multExpr )* )
					{
					root_0 = (Object)adaptor.nil();


					// D:\\temp\\E.g:5:14: ( multExpr ( ( '+' | '-' ) multExpr )* )
					// D:\\temp\\E.g:5:15: multExpr ( ( '+' | '-' ) multExpr )*
					{
					pushFollow(FOLLOW_multExpr_in_expression47);
					multExpr7=multExpr();
					state._fsp--;

					adaptor.addChild(root_0, multExpr7.getTree());

					// D:\\temp\\E.g:5:24: ( ( '+' | '-' ) multExpr )*
					loop3:
					while (true) {
						int alt3=2;
						int LA3_0 = input.LA(1);
						if ( ((LA3_0 >= 11 && LA3_0 <= 12)) ) {
							alt3=1;
						}

						switch (alt3) {
						case 1 :
							// D:\\temp\\E.g:5:25: ( '+' | '-' ) multExpr
							{
							set8=input.LT(1);
							if ( (input.LA(1) >= 11 && input.LA(1) <= 12) ) {
								input.consume();
								adaptor.addChild(root_0, (Object)adaptor.create(set8));
								state.errorRecovery=false;
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								throw mse;
							}
							pushFollow(FOLLOW_multExpr_in_expression58);
							multExpr9=multExpr();
							state._fsp--;

							adaptor.addChild(root_0, multExpr9.getTree());

							}
							break;

						default :
							break loop3;
						}
					}

					}

					}
					break;
				case 2 :
					// D:\\temp\\E.g:5:51: STRING
					{
					root_0 = (Object)adaptor.nil();


					STRING10=(Token)match(input,STRING,FOLLOW_STRING_in_expression65); 
					STRING10_tree = (Object)adaptor.create(STRING10);
					adaptor.addChild(root_0, STRING10_tree);

					}
					break;

			}
			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "expression"


	public static class multExpr_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "multExpr"
	// D:\\temp\\E.g:6:1: multExpr : atom ( '*' atom )* ;
	public final EParser.multExpr_return multExpr() throws RecognitionException {
		EParser.multExpr_return retval = new EParser.multExpr_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		Token char_literal12=null;
		ParserRuleReturnScope atom11 =null;
		ParserRuleReturnScope atom13 =null;

		Object char_literal12_tree=null;

		try {
			// D:\\temp\\E.g:6:10: ( atom ( '*' atom )* )
			// D:\\temp\\E.g:6:12: atom ( '*' atom )*
			{
			root_0 = (Object)adaptor.nil();


			pushFollow(FOLLOW_atom_in_multExpr72);
			atom11=atom();
			state._fsp--;

			adaptor.addChild(root_0, atom11.getTree());

			// D:\\temp\\E.g:6:17: ( '*' atom )*
			loop5:
			while (true) {
				int alt5=2;
				int LA5_0 = input.LA(1);
				if ( (LA5_0==10) ) {
					alt5=1;
				}

				switch (alt5) {
				case 1 :
					// D:\\temp\\E.g:6:18: '*' atom
					{
					char_literal12=(Token)match(input,10,FOLLOW_10_in_multExpr75); 
					char_literal12_tree = (Object)adaptor.create(char_literal12);
					adaptor.addChild(root_0, char_literal12_tree);

					pushFollow(FOLLOW_atom_in_multExpr77);
					atom13=atom();
					state._fsp--;

					adaptor.addChild(root_0, atom13.getTree());

					}
					break;

				default :
					break loop5;
				}
			}

			}

			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "multExpr"


	public static class atom_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "atom"
	// D:\\temp\\E.g:7:1: atom : ( INT | '(' expression ')' );
	public final EParser.atom_return atom() throws RecognitionException {
		EParser.atom_return retval = new EParser.atom_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		Token INT14=null;
		Token char_literal15=null;
		Token char_literal17=null;
		ParserRuleReturnScope expression16 =null;

		Object INT14_tree=null;
		Object char_literal15_tree=null;
		Object char_literal17_tree=null;

		try {
			// D:\\temp\\E.g:7:6: ( INT | '(' expression ')' )
			int alt6=2;
			int LA6_0 = input.LA(1);
			if ( (LA6_0==INT) ) {
				alt6=1;
			}
			else if ( (LA6_0==8) ) {
				alt6=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 6, 0, input);
				throw nvae;
			}

			switch (alt6) {
				case 1 :
					// D:\\temp\\E.g:7:8: INT
					{
					root_0 = (Object)adaptor.nil();


					INT14=(Token)match(input,INT,FOLLOW_INT_in_atom86); 
					INT14_tree = (Object)adaptor.create(INT14);
					adaptor.addChild(root_0, INT14_tree);

					}
					break;
				case 2 :
					// D:\\temp\\E.g:7:14: '(' expression ')'
					{
					root_0 = (Object)adaptor.nil();


					char_literal15=(Token)match(input,8,FOLLOW_8_in_atom90); 
					char_literal15_tree = (Object)adaptor.create(char_literal15);
					adaptor.addChild(root_0, char_literal15_tree);

					pushFollow(FOLLOW_expression_in_atom92);
					expression16=expression();
					state._fsp--;

					adaptor.addChild(root_0, expression16.getTree());

					char_literal17=(Token)match(input,9,FOLLOW_9_in_atom94); 
					char_literal17_tree = (Object)adaptor.create(char_literal17);
					adaptor.addChild(root_0, char_literal17_tree);

					}
					break;

			}
			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
			retval.tree = (Object)adaptor.errorNode(input, retval.start, input.LT(-1), re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "atom"

	// Delegated rules



	public static final BitSet FOLLOW_statement_in_program17 = new BitSet(new long[]{0x0000000000000172L});
	public static final BitSet FOLLOW_expression_in_statement27 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_VAR_in_statement31 = new BitSet(new long[]{0x0000000000004000L});
	public static final BitSet FOLLOW_14_in_statement33 = new BitSet(new long[]{0x0000000000000130L});
	public static final BitSet FOLLOW_expression_in_statement35 = new BitSet(new long[]{0x0000000000002000L});
	public static final BitSet FOLLOW_13_in_statement38 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_multExpr_in_expression47 = new BitSet(new long[]{0x0000000000001802L});
	public static final BitSet FOLLOW_set_in_expression50 = new BitSet(new long[]{0x0000000000000110L});
	public static final BitSet FOLLOW_multExpr_in_expression58 = new BitSet(new long[]{0x0000000000001802L});
	public static final BitSet FOLLOW_STRING_in_expression65 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_atom_in_multExpr72 = new BitSet(new long[]{0x0000000000000402L});
	public static final BitSet FOLLOW_10_in_multExpr75 = new BitSet(new long[]{0x0000000000000110L});
	public static final BitSet FOLLOW_atom_in_multExpr77 = new BitSet(new long[]{0x0000000000000402L});
	public static final BitSet FOLLOW_INT_in_atom86 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_8_in_atom90 = new BitSet(new long[]{0x0000000000000130L});
	public static final BitSet FOLLOW_expression_in_atom92 = new BitSet(new long[]{0x0000000000000200L});
	public static final BitSet FOLLOW_9_in_atom94 = new BitSet(new long[]{0x0000000000000002L});
}
