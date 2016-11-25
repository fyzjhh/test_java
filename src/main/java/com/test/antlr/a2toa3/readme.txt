java -cp .\v2v3\;.\antlr-2.7.7.jar;.\antlr-3.0.1.jar;.\antlr-runtime-3.0.1.jar;.\stringtemplate-3.1b1.jar v3me hello.g > hello3.g

在转换v2到v3的过程成要选对版本， 之前我选了antlr-2009-02-19.11这个， 转换的时候就报错，
Exception in thread "main" java.lang.NoSuchFieldError: type
        at ANTLRLexer.mT48(ANTLRLexer.java:140)
        at ANTLRLexer.mTokens(ANTLRLexer.java:2822)
        at org.antlr.runtime.Lexer.nextToken(Lexer.java:84)
        
换成3.0.1版本的就正常了。