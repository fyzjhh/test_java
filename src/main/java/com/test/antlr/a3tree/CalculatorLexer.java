package com.test.antlr.a3tree;

// $ANTLR 3.1.2 Calculator.g 2012-06-15 10:41:11

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CalculatorLexer extends Lexer {

    public static final int PLUS=5;
    public static final int INT=4;
    public static final int MINUS=7;
    public static final int EOF=-1;

    // delegates
    // delegators

    public CalculatorLexer() {;} 
    public CalculatorLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public CalculatorLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "Calculator.g"; }

    // $ANTLR start "MINUS"
    public final void mMINUS() throws RecognitionException {
        try {
            int _type = MINUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Calculator.g:11:7: ( '-' )
            // Calculator.g:11:9: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "MINUS"

    // $ANTLR start "PLUS"
    public final void mPLUS() throws RecognitionException {
        try {
            int _type = PLUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Calculator.g:12:7: ( '+' )
            // Calculator.g:12:9: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "PLUS"

    // $ANTLR start "INT"
    public final void mINT() throws RecognitionException {
        try {
            int _type = INT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Calculator.g:13:7: ( ( '0' .. '9' )+ )
            // Calculator.g:13:9: ( '0' .. '9' )+
            {
            // Calculator.g:13:9: ( '0' .. '9' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // Calculator.g:13:10: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "INT"

    public void mTokens() throws RecognitionException {
        // Calculator.g:1:8: ( MINUS | PLUS | INT | NEWLINE )
        int alt2=4;
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
                // Calculator.g:1:10: MINUS
                {
                mMINUS(); 

                }
                break;
            case 2 :
                // Calculator.g:1:16: PLUS
                {
                mPLUS(); 

                }
                break;
            case 3 :
                // Calculator.g:1:21: INT
                {
                mINT(); 

                }
                break;

        }

    }


 

}