package com.test.antlr.a3;

// $ANTLR 3.1.2 Cal.g 2012-06-14 10:30:49

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CalLexer extends Lexer {
    public static final int PLUS=5;
    public static final int INT=4;
    public static final int NEWLINE=6;
    public static final int EOF=-1;

    // delegates
    // delegators

    public CalLexer() {;} 
    public CalLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public CalLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "Cal.g"; }

    // $ANTLR start "PLUS"
    public final void mPLUS() throws RecognitionException {
        try {
            int _type = PLUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Cal.g:12:7: ( '+' )
            // Cal.g:12:9: '+'
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
            // Cal.g:13:7: ( ( '0' .. '9' )+ )
            // Cal.g:13:9: ( '0' .. '9' )+
            {
            // Cal.g:13:9: ( '0' .. '9' )+
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
            	    // Cal.g:13:10: '0' .. '9'
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

    // $ANTLR start "NEWLINE"
    public final void mNEWLINE() throws RecognitionException {
        try {
            int _type = NEWLINE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Cal.g:14:9: ( '\\r' '\\n' | '\\n' )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='\r') ) {
                alt2=1;
            }
            else if ( (LA2_0=='\n') ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // Cal.g:15:1: '\\r' '\\n'
                    {
                    match('\r'); 
                    match('\n'); 

                    }
                    break;
                case 2 :
                    // Cal.g:16:3: '\\n'
                    {
                    match('\n'); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "NEWLINE"

    public void mTokens() throws RecognitionException {
        // Cal.g:1:8: ( PLUS | INT | NEWLINE )
        int alt3=3;
        switch ( input.LA(1) ) {
        case '+':
            {
            alt3=1;
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
            alt3=2;
            }
            break;
        case '\n':
        case '\r':
            {
            alt3=3;
            }
            break;
        default:
            NoViableAltException nvae =
                new NoViableAltException("", 3, 0, input);

            throw nvae;
        }

        switch (alt3) {
            case 1 :
                // Cal.g:1:10: PLUS
                {
                mPLUS(); 

                }
                break;
            case 2 :
                // Cal.g:1:15: INT
                {
                mINT(); 

                }
                break;
            case 3 :
                // Cal.g:1:19: NEWLINE
                {
                mNEWLINE(); 

                }
                break;

        }

    }


 

}