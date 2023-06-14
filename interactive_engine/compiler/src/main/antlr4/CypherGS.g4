/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar CypherGS;

oC_Cypher
      :  SP? oC_Statement ( SP? ';' )? SP? EOF ;

oC_Statement
      :  oC_Query ;

oC_Query
     :  oC_RegularQuery ;

oC_RegularQuery
     :  oC_Match ( SP? oC_With )* ( SP oC_Return ) ;

oC_Match
     :  MATCH SP? oC_Pattern ( SP? oC_Where )? ;

MATCH : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

// multiple sentences
oC_Pattern
       :  oC_PatternPart ( SP? ',' SP? oC_PatternPart )* ;

// single sentence pattern
oC_PatternPart
       : oC_AnonymousPatternPart
       ;

oC_AnonymousPatternPart
                    :  oC_PatternElement ;

oC_PatternElement
              :  ( oC_NodePattern ( SP? oC_PatternElementChain )* )
                  | ( '(' oC_PatternElement ')' )
                  ;

// (n)
// (n:Person)
// (n:Person {name : 'marko'})
// (n:Person {name : 'marko', age : 27})
oC_NodePattern
           :  '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( oC_Properties SP? )? ')' ;

oC_PatternElementChain
                   :  oC_RelationshipPattern SP? oC_NodePattern ;

// -[...]-
// <-[...]->
// -[...]->
// <[...]--
oC_RelationshipPattern
                   :  ( oC_LeftArrowHead SP? oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash SP? oC_RightArrowHead )
                       | ( oC_LeftArrowHead SP? oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )
                       | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash SP? oC_RightArrowHead )
                       | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )
                       ;
// [x]
// [x:Knows]
// [x:Knows *1..2]
// [x:Knows *1..2 {name:'XX'}]
oC_RelationshipDetail
                  :  '[' SP? ( oC_Variable SP? )? ( oC_RelationshipTypes SP? )? oC_RangeLiteral? ( oC_Properties SP? )? ']' ;

oC_Properties
          :  oC_MapLiteral
          ;

oC_RelationshipTypes
          :  ':' SP? oC_RelTypeName ( SP? '|' ':'? SP? oC_RelTypeName )* ;

oC_NodeLabels
          :  ':' SP? oC_LabelName ( SP? '|' ':'? SP? oC_LabelName )* ;

oC_RangeLiteral
            :  '*' SP? ( oC_IntegerLiteral SP? ) '..' SP? ( oC_IntegerLiteral SP? ) ;

oC_LabelName
         :  oC_SchemaName ;

oC_RelTypeName
           :  oC_SchemaName ;

oC_With
    :  WITH oC_ProjectionBody ( SP? oC_Where )? ;

WITH : ( 'W' | 'w' ) ( 'I' | 'i' ) ( 'T' | 't' ) ( 'H' | 'h' ) ;

oC_Return
    :  RETURN oC_ProjectionBody ;

RETURN : ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'U' | 'u' ) ( 'R' | 'r' ) ( 'N' | 'n' ) ;

oC_ProjectionBody
    :  ( SP? DISTINCT )? SP oC_ProjectionItems ( SP oC_Order )? ( SP oC_Limit )? ;

oC_ProjectionItems
    :  ( oC_ProjectionItem ( SP? ',' SP? oC_ProjectionItem )* )
    ;

oC_ProjectionItem
    :  ( oC_Expression SP AS SP oC_Variable )
    |  oC_Expression
    ;

AS : ( 'A' | 'a' ) ( 'S' | 's' );

oC_Where
    :  WHERE SP oC_Expression ;

WHERE : ( 'W' | 'w' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'E' | 'e' ) ;

oC_Order
    :  ORDER SP BY SP oC_SortItem ( ',' SP? oC_SortItem )* ;

oC_SortItem
    :  oC_Expression ( SP? ( ASCENDING | ASC | DESCENDING | DESC ) )? ;

ASCENDING : ( 'A' | 'a' ) ( 'S' | 's' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

ASC : ( 'A' | 'a' ) ( 'S' | 's' ) ( 'C' | 'c' ) ;

DESCENDING : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

DESC : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'C' | 'c' ) ;

ORDER : ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ;

BY : ( 'B' | 'b' ) ( 'Y' | 'y' ) ;

oC_Limit
    :  LIMIT SP oC_IntegerLiteral ;

LIMIT : ( 'L' | 'l' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'T' | 't' ) ;

oC_FunctionInvocation
    :  oC_FunctionName SP? '(' SP? ( DISTINCT SP? )? ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? ')' ;

oC_FunctionName
    :  COUNT
    |  AVG
    |  COLLECT
    |  MAX
    |  MIN
    |  SUM
    ;

DISTINCT : ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'C' | 'c' ) ( 'T' | 't' ) ;

COUNT : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'U' | 'u' ) ( 'N' | 'n' ) ( 'T' | 't' ) ;

AVG : ( 'A' | 'a' ) ( 'V' | 'v' ) ( 'G' | 'g' ) ;

COLLECT : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ( 'C' | 'c' ) ( 'T' | 't' ) ;

MAX : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'X' | 'x' ) ;

MIN : ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ;

SUM : ( 'S' | 's' ) ( 'U' | 'u' ) ( 'M' | 'm' ) ;

// literal: 'a'
// variable: b.name
// logical: a=b, a>b, a<2 or c>=3
// arithmetic: a+b, b+1
// complex: (a+1=b or b+2=c) and (c+4=d or d+2=e)
oC_Expression
          :  oC_OrExpression ;

oC_OrExpression
            :  oC_AndExpression ( SP OR SP oC_AndExpression )* ;

OR : ( 'O' | 'o' ) ( 'R' | 'r' ) ;

XOR : ( 'X' | 'x' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ;

oC_AndExpression
             :  oC_ComparisonExpression ( SP AND SP oC_ComparisonExpression )* ;

AND : ( 'A' | 'a' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ;

NOT : ( 'N' | 'n' ) ( 'O' | 'o' ) ( 'T' | 't' ) ;

oC_ComparisonExpression
                    :  oC_StringListNullPredicateExpression ( SP? oC_PartialComparisonExpression )* ;

oC_PartialComparisonExpression
                           :  ( '=' SP? oC_StringListNullPredicateExpression )
                               | ( '<>' SP? oC_StringListNullPredicateExpression )
                               | ( '<' SP? oC_StringListNullPredicateExpression )
                               | ( '>' SP? oC_StringListNullPredicateExpression )
                               | ( '<=' SP? oC_StringListNullPredicateExpression )
                               | ( '>=' SP? oC_StringListNullPredicateExpression )
                               ;

oC_StringListNullPredicateExpression
                                 :  oC_AddOrSubtractExpression ;

IN : ( 'I' | 'i' ) ( 'N' | 'n' ) ;

IS : ( 'I' | 'i' ) ( 'S' | 's' ) ;

NULL : ( 'N' | 'n' ) ( 'U' | 'u' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

oC_AddOrSubtractExpression
                       :  oC_MultiplyDivideModuloExpression ( ( SP? '+' SP? oC_MultiplyDivideModuloExpression ) | ( SP? '-' SP? oC_MultiplyDivideModuloExpression ) )* ;

oC_MultiplyDivideModuloExpression
                              :  oC_PowerOfExpression ( ( SP? '*' SP? oC_PowerOfExpression ) | ( SP? '/' SP? oC_PowerOfExpression ) | ( SP? '%' SP? oC_PowerOfExpression ) )* ;

oC_PowerOfExpression
                 :  oC_UnaryAddOrSubtractExpression ( SP? '^' SP? oC_UnaryAddOrSubtractExpression )* ;

oC_UnaryAddOrSubtractExpression
                            :  oC_ListOperatorExpression
                                | ( ( '+' | '-' ) SP? oC_ListOperatorExpression )
                                ;

oC_ListOperatorExpression
                      :  oC_PropertyOrLabelsExpression ;

oC_PropertyOrLabelsExpression
                          :  oC_Atom ( SP? oC_PropertyLookup )? ;

oC_PropertyLookup
              :  '.' SP? ( oC_PropertyKeyName ) ;

oC_Atom
    :  oC_Literal
        | oC_ParenthesizedExpression
        | oC_Variable
        | oC_FunctionInvocation
        | oC_CountAny
        | oC_Parameter
        ;

oC_Parameter
    : '$' ( oC_SymbolicName ) ;

oC_CountAny
    : ( COUNT SP? '(' SP? '*' SP? ')' )
    ;

oC_ParenthesizedExpression
    :  '(' SP? oC_Expression SP? ')' ;

oC_Variable
        :  oC_SymbolicName ;

oC_Literal
       :  oC_BooleanLiteral
           | NULL
           | oC_NumberLiteral
           | StringLiteral
           | oC_ListLiteral
           | oC_MapLiteral
           ;

oC_BooleanLiteral
              :  TRUE
                  | FALSE
                  ;

TRUE : ( 'T' | 't' ) ( 'R' | 'r' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ;

FALSE : ( 'F' | 'f' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

oC_NumberLiteral
             :  oC_DoubleLiteral
                 | oC_IntegerLiteral
                 ;

oC_IntegerLiteral
              :  HexInteger
                  | OctalInteger
                  | DecimalInteger
                  ;

HexInteger
          :  '0x' ( HexDigit )+ ;

DecimalInteger
              :  ZeroDigit
                  | ( NonZeroDigit ( Digit )* )
                  ;

OctalInteger
            :  '0o' ( OctDigit )+ ;

HexLetter
         :  ( ( 'A' | 'a' ) )
             | ( ( 'B' | 'b' ) )
             | ( ( 'C' | 'c' ) )
             | ( ( 'D' | 'd' ) )
             | ( ( 'E' | 'e' ) )
             | ( ( 'F' | 'f' ) )
             ;

HexDigit
        :  Digit
            | HexLetter
            ;

Digit
     :  ZeroDigit
         | NonZeroDigit
         ;

NonZeroDigit
            :  NonZeroOctDigit
                | '8'
                | '9'
                ;

NonZeroOctDigit
               :  '1'
                   | '2'
                   | '3'
                   | '4'
                   | '5'
                   | '6'
                   | '7'
                   ;

OctDigit
        :  ZeroDigit
            | NonZeroOctDigit
            ;

ZeroDigit
         :  '0' ;

oC_DoubleLiteral
             :  ExponentDecimalReal
                 | RegularDecimalReal
                 ;

ExponentDecimalReal
                   :  ( ( Digit )+ | ( ( Digit )+ '.' ( Digit )+ ) | ( '.' ( Digit )+ ) ) ( ( 'E' | 'e' ) ) '-'? ( Digit )+ ;

RegularDecimalReal
                  :  ( Digit )* '.' ( Digit )+ ;

StringLiteral
             :  ( '"' ( StringLiteral_0 | EscapedChar )* '"' )
                 | ( '\'' ( StringLiteral_1 | EscapedChar )* '\'' )
                 ;

EscapedChar
           :  '\\' ( '\\' | '\'' | '"' | ( ( 'B' | 'b' ) ) | ( ( 'F' | 'f' ) ) | ( ( 'N' | 'n' ) ) | ( ( 'R' | 'r' ) ) | ( ( 'T' | 't' ) ) | ( ( ( 'U' | 'u' ) ) ( HexDigit HexDigit HexDigit HexDigit ) ) | ( ( ( 'U' | 'u' ) ) ( HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit ) ) ) ;

oC_ListLiteral
           :  '[' SP? ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? ']' ;

oC_MapLiteral
          :  '{' SP? ( oC_PropertyKeyName SP? ':' SP? oC_Expression SP? ( ',' SP? oC_PropertyKeyName SP? ':' SP? oC_Expression SP? )* )? '}' ;

oC_PropertyKeyName
               :  oC_SchemaName ;

oC_SchemaName
         :  oC_SymbolicName ;

oC_SymbolicName
            :  UnescapedSymbolicName
                | EscapedSymbolicName
                | HexLetter
                ;

UnescapedSymbolicName
                     :  IdentifierStart ( IdentifierPart )* ;

/**
 * Based on the unicode identifier and pattern syntax
 *   (http://www.unicode.org/reports/tr31/)
 * And extended with a few characters.
 */
IdentifierStart
               :  ID_Start
                   | Pc
                   ;

/**
 * Based on the unicode identifier and pattern syntax
 *   (http://www.unicode.org/reports/tr31/)
 * And extended with a few characters.
 */
IdentifierPart
              :  ID_Continue
                  | Sc
                  ;

/**
 * Any character except "`", enclosed within `backticks`. Backticks are escaped with double backticks.
 */
EscapedSymbolicName
                   :  ( '`' ( EscapedSymbolicName_0 )* '`' )+ ;

SP
  :  ( WHITESPACE )+ ;

WHITESPACE
          :  SPACE
              | TAB
              | LF
              | VT
              | FF
              | CR
              | FS
              | GS
              | RS
              | US
              | '\u1680'
              | '\u180e'
              | '\u2000'
              | '\u2001'
              | '\u2002'
              | '\u2003'
              | '\u2004'
              | '\u2005'
              | '\u2006'
              | '\u2008'
              | '\u2009'
              | '\u200a'
              | '\u2028'
              | '\u2029'
              | '\u205f'
              | '\u3000'
              | '\u00a0'
              | '\u2007'
              | '\u202f'
              | Comment
              ;

Comment
       :  ( '/*' ( Comment_1 | ( '*' Comment_2 ) )* '*/' )
           | ( '//' ( Comment_3 )* CR? ( LF | EOF ) )
           ;

oC_LeftArrowHead
             :  '<'
                 | '\u27e8'
                 | '\u3008'
                 | '\ufe64'
                 | '\uff1c'
                 ;

oC_RightArrowHead
              :  '>'
                  | '\u27e9'
                  | '\u3009'
                  | '\ufe65'
                  | '\uff1e'
                  ;

oC_Dash
    :  '-'
        | '\u00ad'
        | '\u2010'
        | '\u2011'
        | '\u2012'
        | '\u2013'
        | '\u2014'
        | '\u2015'
        | '\u2212'
        | '\ufe58'
        | '\ufe63'
        | '\uff0d'
        ;

fragment FF : [\f] ;

fragment EscapedSymbolicName_0 : ~[`] ;

fragment RS : [\u001E] ;

fragment ID_Continue : [\p{ID_Continue}] ;

fragment Comment_1 : ~[*] ;

fragment StringLiteral_1 : ~['\\] ;

fragment Comment_3 : ~[\n\r] ;

fragment Comment_2 : ~[/] ;

fragment GS : [\u001D] ;

fragment FS : [\u001C] ;

fragment CR : [\r] ;

fragment Sc : [\p{Sc}] ;

fragment SPACE : [ ] ;

fragment Pc : [\p{Pc}] ;

fragment TAB : [\t] ;

fragment StringLiteral_0 : ~["\\] ;

fragment LF : [\n] ;

fragment VT : [\u000B] ;

fragment US : [\u001F] ;

fragment ID_Start : [\p{ID_Start}] ;
