grammar Cal;

expr returns [int value=0]
        : a = INT PLUS b = INT
          {
            int aValue = Integer.parseInt($a.text);
            int bValue = Integer.parseInt($b.text);
            value = aValue + bValue;
          }
        ;
PLUS  : '+' ;
INT   : ('0'..'9')+ NEWLINE ; 
NEWLINE:'\r' '\n' | '\n'
//java -cp antlr-3.1.2.jar org.antlr.Tool Cal.g