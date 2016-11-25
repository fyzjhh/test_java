parser grammar P;
startRule
:   n=NAME

'System.out.println("Hi there,'

;

lexer grammar L;


NAME:( 'a'..'z'|'A'..'Z' )+ NEWLINE

;

NEWLINE
:   '\r' '\n'   // DOS
|   '\n'        // UNIX
;


