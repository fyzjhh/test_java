ID : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')* ;
WS  : (' '|'\r'|'\t'|'\u000C'|'\n') {$channel=HIDDEN;}
    ;
COMMENT
    : '/*' .* '*/' {$channel=HIDDEN;}
    ;
LINE_COMMENT
    : '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    ;
    
#The $channel=HIDDEN; action places those tokens on a hidden channel. 
#They are still sent to the parser, but the parser does not see them. Actions, however, can ask for the hidden channel tokens. 
#If you want to literally throw out tokens then use action skip()

Sometimes you will need some help or rules to make your lexer grammar more readable. Use the fragment modifier in front of the rule:

HexLiteral : '0' ('x'|'X') HexDigit+ ;
fragment HexDigit : ('0'..'9'|'a'..'f'|'A'..'F') ;

