grammar Calculator;
 
options {
    output=AST;
    ASTLabelType=CommonTree;
}
 
plusExpr : INT PLUS^ INT ;
minusExpr : INT MINUS^ INT ;

MINUS : '-';
PLUS  : '+';
INT   : ('0'..'9')+;
