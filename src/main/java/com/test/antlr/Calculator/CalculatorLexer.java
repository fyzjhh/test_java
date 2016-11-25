package com.test.antlr.Calculator;

// $ANTLR 3.5.1 D:\\temp\\Calculator.g 2014-08-19 15:14:14

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CalculatorLexer extends Lexer {
	public static final int EOF=-1;
	public static final int INT=4;
	public static final int MINUS=5;
	public static final int PLUS=6;

	// delegates
	// delegators
	public Lexer[] getDelegates() {
		return new Lexer[] {};
	}

	public CalculatorLexer() {} 
	public CalculatorLexer(CharStream input) {
		this(input, new RecognizerSharedState());
	}
	public CalculatorLexer(CharStream input, RecognizerSharedState state) {
		super(input,state);
	}
	@Override public String getGrammarFileName() { return "D:\\temp\\Calculator.g"; }

	// $ANTLR start "MINUS"
	public final void mMINUS() throws RecognitionException {
		try {
			int _type = MINUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\temp\\Calculator.g:11:7: ( '-' )
			// D:\\temp\\Calculator.g:11:9: '-'
			{
			match('-'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "MINUS"

	// $ANTLR start "PLUS"
	public final void mPLUS() throws RecognitionException {
		try {
			int _type = PLUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\temp\\Calculator.g:12:7: ( '+' )
			// D:\\temp\\Calculator.g:12:9: '+'
			{
			match('+'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "PLUS"

	// $ANTLR start "INT"
	public final void mINT() throws RecognitionException {
		try {
			int _type = INT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// D:\\temp\\Calculator.g:13:7: ( ( '0' .. '9' )+ )
			// D:\\temp\\Calculator.g:13:9: ( '0' .. '9' )+
			{
			// D:\\temp\\Calculator.g:13:9: ( '0' .. '9' )+
			int cnt1=0;
			loop1:
			while (true) {
				int alt1=2;
				int LA1_0 = input.LA(1);
				if ( ((LA1_0 >= '0' && LA1_0 <= '9')) ) {
					alt1=1;
				}

				switch (alt1) {
				case 1 :
					// D:\\temp\\Calculator.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
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

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "INT"

	@Override
	public void mTokens() throws RecognitionException {
		// D:\\temp\\Calculator.g:1:8: ( MINUS | PLUS | INT )
		int alt2=3;
		switch ( input.LA(1) ) {
		case '-':
			{
			alt2=1;
			}
			break;
		case '+':
			{
			alt2=2;
			}
			break;
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			{
			alt2=3;
			}
			break;
		default:
			NoViableAltException nvae =
				new NoViableAltException("", 2, 0, input);
			throw nvae;
		}
		switch (alt2) {
			case 1 :
				// D:\\temp\\Calculator.g:1:10: MINUS
				{
				mMINUS(); 

				}
				break;
			case 2 :
				// D:\\temp\\Calculator.g:1:16: PLUS
				{
				mPLUS(); 

				}
				break;
			case 3 :
				// D:\\temp\\Calculator.g:1:21: INT
				{
				mINT(); 

				}
				break;

		}
	}



}
