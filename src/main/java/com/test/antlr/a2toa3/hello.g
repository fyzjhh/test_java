class P extends Parser;
startRule
:   n:NAME

{System.out.println("Hithere,"+n.getText());}

;

class L extends Lexer;

// one-or-more letters followed by a newline

NAME:( 'a'..'z'|'A'..'Z' )+ NEWLINE

;

NEWLINE
:   '\r' '\n'   // DOS
|   '\n'        // UNIX
;


// java -cp antlr-2.7.5.jar antlr.Tool hello.g