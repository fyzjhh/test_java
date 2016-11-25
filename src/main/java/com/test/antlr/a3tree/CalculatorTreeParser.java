package com.test.antlr.a3tree;

// $ANTLR 3.0.1 .\\CalculatorTreeParser.g 2012-06-17 14:48:28

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CalculatorTreeParser extends TreeParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "INT", "PLUS", "MINUS"
    };
    public static final int PLUS=5;
    public static final int INT=4;
    public static final int MINUS=6;
    public static final int EOF=-1;

        public CalculatorTreeParser(TreeNodeStream input) {
            super(input);
        }
        

    public String[] getTokenNames() { return tokenNames; }
    public String getGrammarFileName() { return ".\\CalculatorTreeParser.g"; }



    // $ANTLR start expr
    // .\\CalculatorTreeParser.g:8:1: expr returns [int value] : ^( PLUS a= INT b= INT ) ;
    public final int expr() throws RecognitionException {
        int value = 0;

        CommonTree a=null;
        CommonTree b=null;

        try {
            // .\\CalculatorTreeParser.g:9:5: ( ^( PLUS a= INT b= INT ) )
            // .\\CalculatorTreeParser.g:9:7: ^( PLUS a= INT b= INT )
            {
            match(input,PLUS,FOLLOW_PLUS_in_expr42); 

            match(input, Token.DOWN, null); 
            a=(CommonTree)input.LT(1);
            match(input,INT,FOLLOW_INT_in_expr46); 
            b=(CommonTree)input.LT(1);
            match(input,INT,FOLLOW_INT_in_expr50); 

            match(input, Token.UP, null); 
            
                      int aValue = Integer.parseInt(a.getText());
                      int bValue = Integer.parseInt(b.getText());
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
    // $ANTLR end expr


    // $ANTLR start expr1
    // .\\CalculatorTreeParser.g:17:1: expr1 returns [int value] : ^( MINUS a= INT b= INT ) ;
    public final int expr1() throws RecognitionException {
        int value = 0;

        CommonTree a=null;
        CommonTree b=null;

        try {
            // .\\CalculatorTreeParser.g:18:5: ( ^( MINUS a= INT b= INT ) )
            // .\\CalculatorTreeParser.g:18:7: ^( MINUS a= INT b= INT )
            {
            match(input,MINUS,FOLLOW_MINUS_in_expr184); 

            match(input, Token.DOWN, null); 
            a=(CommonTree)input.LT(1);
            match(input,INT,FOLLOW_INT_in_expr188); 
            b=(CommonTree)input.LT(1);
            match(input,INT,FOLLOW_INT_in_expr192); 

            match(input, Token.UP, null); 
            
                 int aValue = Integer.parseInt(a.getText());
                 int bValue = Integer.parseInt(b.getText());
                 value = aValue - bValue; 
                

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
    // $ANTLR end expr1


 

    public static final BitSet FOLLOW_PLUS_in_expr42 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_INT_in_expr46 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_INT_in_expr50 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_MINUS_in_expr184 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_INT_in_expr188 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_INT_in_expr192 = new BitSet(new long[]{0x0000000000000008L});

}