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
    | traversalMethod_is    // is()
    | traversalMethod_where // where()
    | traversalMethod_inV   // inV()
    | traversalMethod_outV  // outV()
    | traversalMethod_endV  // endV()
    | traversalMethod_otherV  // otherV()
    | traversalMethod_not  // not()
    | traversalMethod_union // union()
    | traversalMethod_match // match()
    | traversalMethod_subgraph // subgraph()
    | traversalMethod_bothV // bothV()
    | traversalMethod_aggregate_func
    | traversalMethod_hasNot // hasNot()
    | traversalMethod_coin  // coin()
    | traversalMethod_with  // with()
    ;

traversalSourceSpawnMethod_V
	: 'V' LPAREN integerLiteralList RPAREN
	;

traversalSourceSpawnMethod_E
    : 'E' LPAREN integerLiteralList RPAREN
    ;

traversalMethod_as
    : 'as' LPAREN stringLiteral RPAREN
    ;

// hasLabel('')
traversalMethod_hasLabel
    : 'hasLabel' LPAREN stringLiteral (COMMA stringLiteralList)?  RPAREN
    ;

// hasId(1, 2, 3)
traversalMethod_hasId
    : 'hasId' LPAREN integerLiteral (COMMA integerLiteralList)? RPAREN
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

// hasNot("age")
traversalMethod_hasNot
    : 'hasNot' LPAREN stringLiteral RPAREN
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

// case-insensitive
// with('PATH_OPT', 'SIMPLE' | 'ARBITRARY')
// with('RESULT_OPT', 'ALL_V' | 'END_V')
traversalMethod_with
    : 'with' LPAREN stringLiteral COMMA stringLiteral RPAREN
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

// endV()
traversalMethod_endV
	: 'endV' LPAREN RPAREN
	;

// limit(n)
traversalMethod_limit
	: 'limit' LPAREN integerLiteral RPAREN
	;

// valueMap()
// valueMap('s1', ...)
traversalMethod_valueMap
    : 'valueMap' LPAREN stringLiteralList RPAREN
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
// by(select("a").by(values("name")), 'asc' | 'desc')
// by(out().count()), by(out().count(), desc)
traversalMethod_orderby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN traversalOrder RPAREN
    | 'by' LPAREN stringLiteral (COMMA traversalOrder)? RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_values (COMMA traversalOrder)? RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_select (COMMA traversalOrder)? RPAREN
    | 'by' LPAREN nestedTraversal (COMMA traversalOrder)? RPAREN
    ;

traversalMethod_orderby_list
    : traversalMethod_orderby (DOT traversalMethod_orderby)*
    ;

// select('s', ...)
// select('s', ...).by(...).by(...)
// select(expr('@.age'))
traversalMethod_select
    : 'select' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN (DOT traversalMethod_selectby_list)?
    | 'select' LPAREN traversalColumn RPAREN
    | 'select' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_expr RPAREN
    ;

// by()
// by("name")
// by(valueMap())
// by(out().count())
// by(T.label/T.id)
traversalMethod_selectby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN stringLiteral RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_valueMap RPAREN
    | 'by' LPAREN nestedTraversal RPAREN
    | 'by' LPAREN traversalToken RPAREN
    ;

traversalMethod_selectby_list
    : traversalMethod_selectby (DOT traversalMethod_selectby)*
    ;

// dedup in global scope
// dedup()
// dedup().by('name')
// dedup().by(T.label/T.id)
// dedup('a')
// dedup('a').by('name')
// dedup('a', 'b')
// dedup('a', 'b').by('name')
// multiple by traversals is unsupported in standard gremlin, i.e. dedup().by(..).by(..)
traversalMethod_dedup
	: 'dedup' LPAREN stringLiteralList RPAREN (DOT traversalMethod_dedupby)?
	;

// by('name')
// by(values('name')), by(out().count())
// by(T.label/T.id)
traversalMethod_dedupby
    : 'by' LPAREN stringLiteral RPAREN
    | 'by' LPAREN nestedTraversal RPAREN
    | 'by' LPAREN traversalToken RPAREN
    ;

traversalToken
    : 'id' | 'T.id'
    | 'label' | 'T.label'
    ;

traversalMethod_group
	: 'group' LPAREN RPAREN (DOT traversalMethod_group_keyby)?
	| 'group' LPAREN RPAREN DOT traversalMethod_group_keyby DOT traversalMethod_group_valueby
	;

traversalMethod_groupCount
	: 'groupCount' LPAREN RPAREN (DOT traversalMethod_group_keyby)?
	;

traversalMethod_group_keyby
    : 'by' LPAREN RPAREN                   // group().by()
    | 'by' LPAREN stringLiteral RPAREN     // group().by('name')
    | 'by' LPAREN nonStringKeyByList RPAREN
    ;

// group().by(values('name'))
// group().by(values('name').as('key'))
// group().by(out().count())
// group().by(out().count().as('key'))
nonStringKeyBy
    : nestedTraversal
    ;

// group().by(values('name').as('k1'), values('age').as('k2'))
// group().by(out().count().as('k1'), in().count().as('k2'))
nonStringKeyByList
    : nonStringKeyBy (COMMA nonStringKeyBy)*
    ;

traversalMethod_group_valueby
    : 'by' LPAREN RPAREN                   // group().by(...).by()
    | 'by' LPAREN stringLiteral RPAREN     // group().by(...).by("name") = group().by(...).by(values("name").fold())
    | 'by' LPAREN nonStringValueByList RPAREN
    ;

// group().by(...).by(count()/sum()/min()/max()/mean()/fold())

// group().by(...).by(select("a").count()/sum()/min()/max()/mean()/fold())
// group().by(...).by(select("a").by("name").count()/sum()/min()/max()/mean()/fold())
// group().by(...).by(select("a").values("name").count()/sum()/min()/max()/mean()/fold())

// group().by(...).by(dedup().count()) = countDistinct('@')
// group().by(...).by(dedup('a').count()) = countDistinct('@a')
// group().by(...).by(dedup('a').by('name').count()) = countDistinct('@a.name')

// group().by(...).by(dedup().fold()) = toSet('@')
// group().by(...).by(dedup('a').fold()) = toSet('@a')
// group().by(...).by(dedup('a').by('name').fold()) = toSet('@a.name')
nonStringValueBy
    : (ANON_TRAVERSAL_ROOT DOT)? (traversalMethod_select DOT)? (traversalMethod_values DOT)? traversalMethod_aggregate_func (DOT traversalMethod_as)?
    | (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_dedup DOT traversalMethod_count (DOT traversalMethod_as)?
    | (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_dedup DOT traversalMethod_fold (DOT traversalMethod_as)?
    ;

// i.e. group().by(...).by(count().as('a'), sum().as('b'))
nonStringValueByList
    : nonStringValueBy (COMMA nonStringValueBy)*
    ;

traversalMethod_aggregate_func
    : traversalMethod_count
    | traversalMethod_fold
    | traversalMethod_sum
    | traversalMethod_min
    | traversalMethod_max
    | traversalMethod_mean
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

// sum in global scope
traversalMethod_sum
	: 'sum' LPAREN RPAREN
	;

// min in global scope
traversalMethod_min
	: 'min' LPAREN RPAREN
	;

// max in global scope
traversalMethod_max
	: 'max' LPAREN RPAREN
	;

// mean in global scope
traversalMethod_mean
	: 'mean' LPAREN RPAREN
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
// where(expr("@.age && @.age > 20"))
traversalMethod_where
	: 'where' LPAREN traversalPredicate RPAREN (DOT traversalMethod_whereby_list)?
	| 'where' LPAREN stringLiteral COMMA traversalPredicate RPAREN (DOT traversalMethod_whereby_list)?
    | 'where' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_not RPAREN // match not(__.out) as traversalMethod_not instead of nestedTraversal
    | 'where' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_expr RPAREN
	| 'where' LPAREN nestedTraversal RPAREN
	;

// where().by()
// where().by('name')
// where().by(values('name'))
traversalMethod_whereby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN stringLiteral RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_values RPAREN
    | 'by' LPAREN nestedTraversal RPAREN
    ;

traversalMethod_whereby_list
    : traversalMethod_whereby (DOT traversalMethod_whereby)*
    ;

traversalMethod_not
    : 'not' LPAREN nestedTraversal RPAREN
    ;

// union(__.out(), __.out().out())
traversalMethod_union
    : 'union' LPAREN nestedTraversalExpr RPAREN
    ;

traversalMethod_coin
	: 'coin' LPAREN floatLiteral RPAREN
	;

nestedTraversalExpr
    : nestedTraversal (COMMA nestedTraversal)*
    ;

traversalMethod_match
	: 'match' LPAREN nestedTraversalExpr RPAREN
	;

traversalMethod_expr
    : 'expr' LPAREN stringLiteral RPAREN
    ;

// i.e. g.E().subgraph("graph_name")
traversalMethod_subgraph
	: 'subgraph' LPAREN stringLiteral RPAREN
	;

traversalMethod_bothV
	: 'bothV' LPAREN RPAREN
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

integerLiteralList
    : integerLiteralExpr?
    | LBRACK integerLiteralExpr? RBRACK
    ;

integerLiteralExpr
    : integerLiteral (COMMA integerLiteral)*
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
    | traversalPredicate_containing // TextP.containing
    | traversalPredicate_notContaining //  TextP.notContaining
    | traversalPredicate_not
    | traversalPredicate_inside
    | traversalPredicate_outside
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

traversalPredicate_containing
    : ('TextP.containing' | 'containing') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_notContaining
    : ('TextP.notContaining' | 'notContaining') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_not
    : ('P.not' | 'not') LPAREN traversalPredicate RPAREN
    ;

traversalPredicate_inside
    : ('P.inside' | 'inside') LPAREN genericLiteral COMMA genericLiteral RPAREN
    ;

traversalPredicate_outside
    : ('P.outside' | 'outside') LPAREN genericLiteral COMMA genericLiteral RPAREN
    ;

// incr and decr is unsupported in 3.5.1
traversalOrder
    : 'asc'  | 'Order.asc'
    | 'desc' | 'Order.desc'
    | 'shuffle' | 'Order.shuffle'
    ;

traversalColumn
    : 'keys' | 'Column.keys'
    | 'values' | 'Column.values'
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
