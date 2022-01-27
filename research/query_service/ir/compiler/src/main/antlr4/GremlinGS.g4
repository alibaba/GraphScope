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

grammar GremlinGS;

// g or g.rootTraversal()
query
    : rootTraversal
    ;

// g
traversalSource
    : TRAVERSAL_ROOT
    ;

// g.rootTraversal()
rootTraversal
    : traversalSource DOT traversalSourceSpawnMethod
    | traversalSource DOT traversalSourceSpawnMethod DOT chainedTraversal
    ;

// A recursive definition of chained traversal, where
// it is either a traversal method itself, e.g. V(), has(), out()
// or it is <chainedTraversal>.<traversalMethod>
chainedTraversal
    : traversalMethod
    | chainedTraversal DOT traversalMethod
    ;

traversalSourceSpawnMethod
    : traversalSourceSpawnMethod_V  // V()
    | traversalSourceSpawnMethod_E  // E()
    ;

// Defining supported traversal methods
traversalMethod
	: traversalMethod_as  // as()
    | traversalMethod_hasLabel  // hasLabel()
    | traversalMethod_hasId  // hasId()
    | traversalMethod_has   // has()
    | traversalMethod_out   // out()
    | traversalMethod_in  // in()
    | traversalMethod_both  // in()
    | traversalMethod_outE  // outE()[.inV()]
    | traversalMethod_inE  // inE()[.outV()]
    | traversalMethod_bothE  // bothE()[.otherV()]
    | traversalMethod_limit    // limit()
    | traversalMethod_valueMap  // valueMap()
    | traversalMethod_order  // order()
    | traversalMethod_select  // select()
    | traversalMethod_dedup   // dedup()
    | traversalMethod_group   // group()
    | traversalMethod_groupCount // groupCount()
    | traversalMethod_values    // values()
    | traversalMethod_count // count()
    | traversalMethod_is    // is()
    | traversalMethod_where // where()
    | traversalMethod_inV   // inV()
    | traversalMethod_outV  // outV()
    | traversalMethod_otherV  // otherV()
    | traversalMethod_not  // not()
    ;

traversalSourceSpawnMethod_V
	: 'V' LPAREN genericLiteralList RPAREN
	;

traversalSourceSpawnMethod_E
    : 'E' LPAREN genericLiteralList RPAREN
    ;

traversalMethod_as
    : 'as' LPAREN stringLiteral RPAREN
    ;

// hasLabel('')
traversalMethod_hasLabel
    : 'hasLabel' LPAREN stringLiteral (COMMA stringLiteralList)?  RPAREN
    ;

// hasId(1, 2, 3), or hasId("1", "2", "3")
traversalMethod_hasId
    : 'hasId' LPAREN genericLiteral (COMMA genericLiteralList)? RPAREN
    ;

// has("str", y), has("str", eq/neq/gt/gte/lt/lte(y))
// has("person", "name", "marko")
// has("person", "name", P.eq("marko"))
// has("name")
traversalMethod_has
    : 'has' LPAREN stringLiteral COMMA genericLiteral RPAREN  // indicate eq
    | 'has' LPAREN stringLiteral COMMA traversalPredicate RPAREN
    | 'has' LPAREN stringLiteral COMMA stringLiteral COMMA genericLiteral RPAREN
    | 'has' LPAREN stringLiteral COMMA stringLiteral COMMA traversalPredicate RPAREN
    | 'has' LPAREN stringLiteral RPAREN
    ;

// out('str1', ...)
// out('1..5', 'str1')
traversalMethod_out
	: 'out' LPAREN stringLiteralList RPAREN
	;

// in('str1', ...)
// in('1..5', 'str1')
traversalMethod_in
	: 'in' LPAREN stringLiteralList RPAREN
	;

// both('str1', ...)
// both('1..5', 'str1', ...)
traversalMethod_both
	: 'both' LPAREN stringLiteralList RPAREN
	;

// outE('str1', ...), outE().inV()
traversalMethod_outE
	: 'outE' LPAREN stringLiteralList RPAREN (DOT traversalMethod_inV)?
	;

// inE('str1', ...), inE().outV()
traversalMethod_inE
	: 'inE' LPAREN stringLiteralList RPAREN (DOT traversalMethod_outV)?
	;

// bothE('str1', ...), bothE().otherV()
traversalMethod_bothE
	: 'bothE' LPAREN stringLiteralList RPAREN (DOT traversalMethod_otherV)?
	;

// outV()
traversalMethod_outV
	: 'outV' LPAREN RPAREN
	;

// inV()
traversalMethod_inV
	: 'inV' LPAREN RPAREN
	;

// otherV()
traversalMethod_otherV
	: 'otherV' LPAREN RPAREN
	;

// limit(n)
traversalMethod_limit
	: 'limit' LPAREN integerLiteral RPAREN
	;

// valueMap('s1', ...)
// valueMap() is unsupported
traversalMethod_valueMap
    : 'valueMap' LPAREN stringLiteralExpr RPAREN
    ;

// order()
// order().by
traversalMethod_order
    : 'order' LPAREN RPAREN (DOT traversalMethod_orderby_list)?
    ;

// by()
// by('asc' | 'desc')
// by('a', 'asc' | 'desc')
// by(values(..), 'asc' | 'desc')
// by(select("a"), 'asc' | 'desc')
// by(select("a").by("name"), 'asc' | 'desc')
// by(select("a").by(valueMap("name")), 'asc' | 'desc')
traversalMethod_orderby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN traversalOrder RPAREN
    | 'by' LPAREN stringLiteral (COMMA traversalOrder)? RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_values (COMMA traversalOrder)? RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_select (COMMA traversalOrder)? RPAREN
    ;

traversalMethod_orderby_list
    : traversalMethod_orderby (DOT traversalMethod_orderby)*
    ;

// select('s', ...)
// select('s', ...).by(...).by(...)
traversalMethod_select
    : 'select' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN (DOT traversalMethod_selectby_list)?
    ;

// by()
// by("name")
// by(valueMap())
traversalMethod_selectby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN stringLiteral RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_valueMap RPAREN
    ;

traversalMethod_selectby_list
    : traversalMethod_selectby (DOT traversalMethod_selectby)*
    ;

// dedup in global scope
traversalMethod_dedup
	: 'dedup' LPAREN RPAREN
	;

traversalMethod_group
	: 'group' LPAREN RPAREN (DOT traversalMethod_group_keyby)?
	| 'group' LPAREN RPAREN DOT traversalMethod_group_keyby DOT traversalMethod_group_valueby
	;

traversalMethod_groupCount
	: 'groupCount' LPAREN RPAREN (DOT traversalMethod_group_keyby)?
	;

// group().by()
// group().by('name')
// group().by(values('name'))
// group().by(values('name').as('key'))
traversalMethod_group_keyby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN stringLiteral RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_values (DOT traversalMethod_as)? RPAREN
    ;

// group().by(...).by()
// group().by(...).by(fold().as("value"))
// group().by(...).by(count())
// group().by(...).by(count().as("value"))
traversalMethod_group_valueby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_aggregate_func (DOT traversalMethod_as)? RPAREN
    ;

traversalMethod_aggregate_func
    : traversalMethod_count
    | traversalMethod_fold
    ;

// count in global scope
traversalMethod_count
	: 'count' LPAREN RPAREN
	;

// only one argument is permitted
// values("name")
traversalMethod_values
    : 'values' LPAREN stringLiteral RPAREN
    ;

// fold()
traversalMethod_fold
	: 'fold' LPAREN RPAREN
	;

// is(27)
// is(P.eq(27))
traversalMethod_is
	: 'is' LPAREN genericLiteral RPAREN
	| 'is' LPAREN traversalPredicate RPAREN
	;

// where(P.eq("a"))
// where("c", P.eq("a"))
// where(P.eq("a")).by("age")
// where("c", P.eq("a")).by("id").by("age")
// where(out().out()...)
// where(__.as("a")...as("b"))
// where(__.not(__.out)) equal to not(__.out)
traversalMethod_where
	: 'where' LPAREN traversalPredicate RPAREN (DOT traversalMethod_whereby_list)?
	| 'where' LPAREN stringLiteral COMMA traversalPredicate RPAREN (DOT traversalMethod_whereby_list)?
    | 'where' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_not RPAREN // match not(__.out) as traversalMethod_not instead of nestedTraversal
	| 'where' LPAREN nestedTraversal RPAREN
	;

// where().by()
// where().by('name')
// where().by(values('name'))
traversalMethod_whereby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN stringLiteral RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_values RPAREN
    ;

traversalMethod_whereby_list
    : traversalMethod_whereby (DOT traversalMethod_whereby)*
    ;

traversalMethod_not
    : 'not' LPAREN nestedTraversal RPAREN
    ;

// only permit non empty, \'\' or \"\" or \'null\' is meaningless as a parameter
stringLiteral
    : NonEmptyStringLiteral
    ;

stringLiteralList
    : stringLiteralExpr?
    | LBRACK stringLiteralExpr? RBRACK
    ;

stringLiteralExpr
    : stringLiteral (COMMA stringLiteral)*
    ;

genericLiteral
	: integerLiteral
	| floatLiteral
	| booleanLiteral
	| stringLiteral
	;

genericLiteralList
    : genericLiteralExpr?
    | LBRACK genericLiteralExpr? RBRACK
    ;

genericLiteralExpr
    : genericLiteral (COMMA genericLiteral)*
    ;

integerLiteral
    : IntegerLiteral
    ;

floatLiteral
    : FloatingPointLiteral
    ;

booleanLiteral
    : BooleanLiteral
    ;

nullLiteral
    : NullLiteral
    ;

// Traversal predicate
traversalPredicate
    : traversalPredicate_eq
    | traversalPredicate_neq
    | traversalPredicate_lt
    | traversalPredicate_lte
    | traversalPredicate_gt
    | traversalPredicate_gte
    | traversalPredicate_within
    | traversalPredicate_without
    | traversalPredicate DOT 'and' LPAREN traversalPredicate RPAREN
    | traversalPredicate DOT 'or' LPAREN traversalPredicate RPAREN
    ;

nestedTraversal
    : chainedTraversal
    | ANON_TRAVERSAL_ROOT DOT chainedTraversal
    ;

traversalPredicate_eq
    : ('P.eq' | 'eq') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_neq
    : ('P.neq' | 'neq') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_lt
    : ('P.lt' | 'lt') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_lte
    : ('P.lte' | 'lte') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_gt
    : ('P.gt' | 'gt') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_gte
    : ('P.gte' | 'gte') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_within
    : ('P.within' | 'within') LPAREN genericLiteralList RPAREN
    ;

traversalPredicate_without
    : ('P.without' | 'without') LPAREN genericLiteralList RPAREN
    ;

// incr and decr is unsupported in 3.5.1
traversalOrder
    : 'asc'  | 'Order.asc'
    | 'desc' | 'Order.desc'
    | 'shuffle' | 'Order.shuffle'
    ;

// Integer Literals

IntegerLiteral
	:	Sign? DecimalIntegerLiteral
	;

fragment
DecimalIntegerLiteral
	:	DecimalNumeral IntegerTypeSuffix?
	;

fragment
IntegerTypeSuffix
	:	[lL]
	;

fragment
DecimalNumeral
	:	'0'
	|	NonZeroDigit (Digits? | Underscores Digits)
	;

fragment
Digits
	:	Digit (DigitsAndUnderscores? Digit)?
	;

fragment
Digit
	:	'0'
	|	NonZeroDigit
	;

fragment
NonZeroDigit
	:	[1-9]
	;

fragment
DigitsAndUnderscores
	:	DigitOrUnderscore+
	;

fragment
DigitOrUnderscore
	:	Digit
	|	'_'
	;

fragment
Underscores
	:	'_'+
	;

// Floating-Point Literals

FloatingPointLiteral
	:	Sign? DecimalFloatingPointLiteral
	;

fragment
DecimalFloatingPointLiteral
    :   Digits ('.' Digits ExponentPart? | ExponentPart) FloatTypeSuffix?
	|	Digits FloatTypeSuffix
	;

fragment
ExponentPart
	:	ExponentIndicator SignedInteger
	;

fragment
ExponentIndicator
	:	[eE]
	;

fragment
SignedInteger
	:	Sign? Digits
	;

fragment
Sign
	:	[+-]
	;

fragment
FloatTypeSuffix
	:	[fFdD]
	;

// Boolean Literals

BooleanLiteral
	:	'true'
	|	'false'
	;

// Null Literal

NullLiteral
	:	'null'
	;


fragment
DoubleQuotedStringCharacters
	:	DoubleQuotedStringCharacter+
	;

EmptyStringLiteral
	:   '""'
	|   '\'\''
	;

NonEmptyStringLiteral
	:   '"' DoubleQuotedStringCharacters '"'
	|   '\'' SingleQuotedStringCharacters '\''
	;

fragment
DoubleQuotedStringCharacter
	:	~('"' | '\\')
	|   JoinLineEscape
	|	EscapeSequence
	;

fragment
SingleQuotedStringCharacters
	:	SingleQuotedStringCharacter+
	;

fragment
SingleQuotedStringCharacter
	:	~('\'' | '\\')
	|   JoinLineEscape
	|	EscapeSequence
	;

// Escape Sequences for Character and String Literals
fragment JoinLineEscape
    : '\\' '\r'? '\n'
    ;

fragment
EscapeSequence
	:	'\\' [btnfr"'\\]
	;

// Separators

LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
LBRACK : '[';
RBRACK : ']';
SEMI : ';';
COMMA : ',';
DOT : '.';
COLON : ':';

TRAVERSAL_ROOT:     'g';
ANON_TRAVERSAL_ROOT:     '__';

// Trim whitespace and comments if present

WS  :  [ \t\r\n\u000C]+ -> skip
    ;

LINE_COMMENT
    :   '//' ~[\r\n]* -> skip
    ;
