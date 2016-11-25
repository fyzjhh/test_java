tree grammar CalculatorTreeParser;
 
options {
  tokenVocab=Calculator;
  ASTLabelType=CommonTree;
}
 
expr_plus returns [int value]
    : ^(PLUS a=INT b=INT)  
      {
          int aValue = Integer.parseInt($a.text);
          int bValue = Integer.parseInt($b.text);
          value = aValue + bValue;
      }
    ; 

expr_minus returns [int value]
    : ^(MINUS a=INT b=INT)
    {
     int aValue = Integer.parseInt($a.text);
     int bValue = Integer.parseInt($b.text);
     value = aValue - bValue; 
    };