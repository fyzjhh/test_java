package com.test.antlr.a277;

// $ANTLR 2.7.7 (2006-11-01): "calc.g" -> "CalcTreeWalker.java"$

import antlr.TreeParser;
import antlr.Token;
import antlr.collections.AST;
import antlr.RecognitionException;
import antlr.ANTLRException;
import antlr.NoViableAltException;
import antlr.MismatchedTokenException;
import antlr.SemanticException;
import antlr.collections.impl.BitSet;
import antlr.ASTPair;
import antlr.collections.impl.ASTArray;


public class CalcTreeWalker extends antlr.TreeParser       implements CalcParserTokenTypes
 {
public CalcTreeWalker() {
	tokenNames = _tokenNames;
}

	public final float  expr(AST _t) throws RecognitionException {
		float r;
		
		AST expr_AST_in = (_t == ASTNULL) ? null : (AST)_t;
		AST i = null;
		
			float a,b;
			r=0;
		
		
		try {      // for error handling
			if (_t==null) _t=ASTNULL;
			switch ( _t.getType()) {
			case PLUS:
			{
				AST __t20 = _t;
				AST tmp1_AST_in = (AST)_t;
				match(_t,PLUS);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t20;
				_t = _t.getNextSibling();
				r = a+b;
				break;
			}
			case STAR:
			{
				AST __t21 = _t;
				AST tmp2_AST_in = (AST)_t;
				match(_t,STAR);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t21;
				_t = _t.getNextSibling();
				r = a*b;
				break;
			}
			case INT:
			{
				i = (AST)_t;
				match(_t,INT);
				_t = _t.getNextSibling();
				r = (float)Integer.parseInt(i.getText());
				break;
			}
			default:
			{
				throw new NoViableAltException(_t);
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			if (_t!=null) {_t = _t.getNextSibling();}
		}
		_retTree = _t;
		return r;
	}
	
	
	public static final String[] _tokenNames = {
		"<0>",
		"EOF",
		"<2>",
		"NULL_TREE_LOOKAHEAD",
		"PLUS",
		"SEMI",
		"STAR",
		"INT",
		"WS",
		"LPAREN",
		"RPAREN",
		"DIGIT"
	};
	
	}
	
