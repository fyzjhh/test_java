class P extends Parser;

startRule
:   n:NAME

{System.out.println("Hi,"+n.getText());}

;

class L extends Lexer;

NAME:( 'a'..'z'|'A'..'Z' )+ NEWLINE
;

NEWLINE
:   '\r' '\n'  
|   '\n'     
;


// java -cp antlr-2.7.5.jar antlr.Tool hello.g