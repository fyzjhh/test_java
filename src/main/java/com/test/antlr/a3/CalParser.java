package com.test.antlr.a3;

// $ANTLR 3.1.2 Cal.g 2012-06-14 10:30:49

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CalParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "INT", "PLUS", "NEWLINE"
    };
    public static final int PLUS=5;
    public static final int INT=4;
    public static final int NEWLINE=6;
    public static final int EOF=-1;

    // delegates
    // delegators


        public CalParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public CalParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return CalParser.tokenNames; }
    public String getGrammarFileName() { return "Cal.g"; }



    // $ANTLR start "expr"
    // Cal.g:3:1: expr returns [int value=0] : a= INT PLUS b= INT ;
    public final int expr() throws RecognitionException {
        int value = 0;

        Token a=null;
        Token b=null;

        try {
            // Cal.g:4:9: (a= INT PLUS b= INT )
            // Cal.g:4:11: a= INT PLUS b= INT
            {
            a=(Token)match(input,INT,FOLLOW_INT_in_expr26); 
            match(input,PLUS,FOLLOW_PLUS_in_expr28); 
            b=(Token)match(input,INT,FOLLOW_INT_in_expr34); 

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
        }
        return value;
    }
    // $ANTLR end "expr"

    // Delegated rules


 

    public static final BitSet FOLLOW_INT_in_expr26 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_PLUS_in_expr28 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_INT_in_expr34 = new BitSet(new long[]{0x0000000000000002L});

}