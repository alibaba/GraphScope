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

import ExprGS;

// g or g.rootTraversal()
query
    : rootTraversal
    ;

// g
// g.with(ARGS_EVAL_TIMEOUT, 2000L)
// g.with(Tokens.ARGS_EVAL_TIMEOUT, 2000L)
// g.with('evaluationTimeout', 2000L)
traversalSource
    : TRAVERSAL_ROOT (DOT traversalMethod_with) *
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
    | traversalMethod_valueMap // valueMap()
    | traversalMethod_elementMap // elementMap()
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
    | traversalMethod_identity // identity()
    | traversalMethod_match // match()
    | traversalMethod_subgraph // subgraph()
    | traversalMethod_bothV // bothV()
    | traversalMethod_unfold // unfold()
    | traversalMethod_hasNot // hasNot()
    | traversalMethod_coin  // coin()
    | traversalMethod_sample    // sample()
    | traversalMethod_with  // with()
    | traversalMethod_id    // id()
    | traversalMethod_label // label()
    | traversalMethod_constant  //constant
    | oC_AggregateFunctionInvocation // aggregate function, i.e. count/sum/min/max/mean/fold
    ;

traversalSourceSpawnMethod_V
    : 'V' LPAREN oC_ListLiteral RPAREN
    | 'V' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
	;

traversalSourceSpawnMethod_E
    : 'E' LPAREN oC_ListLiteral RPAREN
    | 'E' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
    ;

traversalMethod_as
    : 'as' LPAREN StringLiteral RPAREN
    ;

// hasLabel('')
traversalMethod_hasLabel
    : 'hasLabel' LPAREN oC_ListLiteral RPAREN
    | 'hasLabel' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
    ;

// hasId(1, 2, 3)
traversalMethod_hasId
    : 'hasId' LPAREN oC_ListLiteral RPAREN
    | 'hasId' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
    ;

// has("str", y), has("str", eq/neq/gt/gte/lt/lte(y))
// has("person", "name", "marko")
// has("person", "name", P.eq("marko"))
// has("name")
traversalMethod_has
    : 'has' LPAREN StringLiteral COMMA oC_Literal RPAREN  // indicate eq
    | 'has' LPAREN StringLiteral COMMA traversalPredicate RPAREN
    | 'has' LPAREN StringLiteral COMMA StringLiteral COMMA oC_Literal RPAREN
    | 'has' LPAREN StringLiteral COMMA StringLiteral COMMA traversalPredicate RPAREN
    | 'has' LPAREN StringLiteral RPAREN
    ;

// hasNot("age")
traversalMethod_hasNot
    : 'hasNot' LPAREN StringLiteral RPAREN
    ;

// out('str1', ...)
// out('1..5', 'str1')
traversalMethod_out
	: 'out' LPAREN oC_ListLiteral RPAREN
	| 'out' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
	;

// in('str1', ...)
// in('1..5', 'str1')
traversalMethod_in
	: 'in' LPAREN oC_ListLiteral RPAREN
    | 'in' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
	;

// both('str1', ...)
// both('1..5', 'str1', ...)
traversalMethod_both
	: 'both' LPAREN oC_ListLiteral RPAREN
    | 'both' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
	;

// outE('str1', ...), outE().inV()
traversalMethod_outE
	: 'outE' LPAREN oC_ListLiteral RPAREN (DOT traversalMethod_inV)?
	| 'outE' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN (DOT traversalMethod_inV)?
	;

// inE('str1', ...), inE().outV()
traversalMethod_inE
	: 'inE' LPAREN oC_ListLiteral RPAREN (DOT traversalMethod_outV)?
	| 'inE' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN (DOT traversalMethod_outV)?
	;

// bothE('str1', ...), bothE().otherV()
traversalMethod_bothE
	: 'bothE' LPAREN oC_ListLiteral RPAREN (DOT traversalMethod_otherV)?
	| 'bothE' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN (DOT traversalMethod_otherV)?
	;

// case-insensitive
// with('PATH_OPT', 'SIMPLE' | 'ARBITRARY')
// with('RESULT_OPT', 'ALL_V' | 'END_V')
// with('UNTIL', expression)
// with('ARGS_EVAL_TIMEOUT', 2000L) // set evaluation timeout to 2 seconds
// with('Tokens.ARGS_EVAL_TIMEOUT', 2000L) // set evaluation timeout to 2 seconds
// with('evaluationTimeout', 2000L) // set evaluation timeout to 2 seconds
traversalMethod_with
    : 'with' LPAREN StringLiteral COMMA oC_Literal RPAREN
    | 'with' LPAREN evaluationTimeoutKey COMMA evaluationTimeoutValue RPAREN
    | 'with' LPAREN StringLiteral COMMA (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_expr RPAREN // to support path until condition in gremlin-calcite, i.e. with('UNTIL', expr(_.age > 20))
    ;

evaluationTimeoutKey
    : 'ARGS_EVAL_TIMEOUT' | 'Tokens.ARGS_EVAL_TIMEOUT'
    ;

evaluationTimeoutValue
    : oC_IntegerLiteral
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
	: 'limit' LPAREN oC_IntegerLiteral RPAREN
	;

// valueMap()
// valueMap('s1', ...)
traversalMethod_valueMap
    : 'valueMap' LPAREN oC_ListLiteral RPAREN
    | 'valueMap' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
    ;

// elementMap()
// elementMap('s1', ...)
traversalMethod_elementMap
    : 'elementMap' LPAREN oC_ListLiteral RPAREN
    | 'elementMap' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
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
    | 'by' LPAREN StringLiteral (COMMA traversalOrder)? RPAREN
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
    : 'select' LPAREN oC_ListLiteral RPAREN (DOT traversalMethod_selectby_list)?
    | 'select' LPAREN (oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN (DOT traversalMethod_selectby_list)?
    | 'select' LPAREN traversalColumn RPAREN
    | 'select' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_expr RPAREN
    ;

// by()
// by("name")
// by(valueMap())
// by(elementMap())
// by(out().count())
// by(T.label/T.id)
traversalMethod_selectby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN StringLiteral RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_valueMap RPAREN
    | 'by' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_elementMap RPAREN
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
	: 'dedup' LPAREN oC_ListLiteral RPAREN (DOT traversalMethod_dedupby)?
	| 'dedup' LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN (DOT traversalMethod_dedupby)?
	;

// by('name')
// by(values('name')), by(out().count())
// by(T.label/T.id)
traversalMethod_dedupby
    : 'by' LPAREN StringLiteral RPAREN
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
    | 'by' LPAREN StringLiteral RPAREN     // group().by('name')
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
    | 'by' LPAREN StringLiteral RPAREN     // group().by(...).by("name") = group().by(...).by(values("name").fold())
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
    : (ANON_TRAVERSAL_ROOT DOT)? (traversalMethod_select DOT)? (traversalMethod_values DOT)? oC_AggregateFunctionInvocation (DOT traversalMethod_as)?
    | (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_dedup DOT oC_AggregateFunctionInvocation (DOT traversalMethod_as)?
    ;

// i.e. group().by(...).by(count().as('a'), sum().as('b'))
nonStringValueByList
    : nonStringValueBy (COMMA nonStringValueBy)*
    ;

// only one argument is permitted
// values("name")
traversalMethod_values
    : 'values' LPAREN StringLiteral RPAREN
    ;

// is(27)
// is(P.eq(27))
traversalMethod_is
	: 'is' LPAREN oC_Literal RPAREN
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
	| 'where' LPAREN StringLiteral COMMA traversalPredicate RPAREN (DOT traversalMethod_whereby_list)?
    | 'where' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_not RPAREN // match not(__.out) as traversalMethod_not instead of nestedTraversal
    | 'where' LPAREN (ANON_TRAVERSAL_ROOT DOT)? traversalMethod_expr RPAREN
	| 'where' LPAREN nestedTraversal RPAREN
	;

// where().by()
// where().by('name')
// where().by(values('name'))
traversalMethod_whereby
    : 'by' LPAREN RPAREN
    | 'by' LPAREN StringLiteral RPAREN
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

traversalMethod_identity
    : 'identity' LPAREN RPAREN
    ;

// coin(0.5)
traversalMethod_coin
	: 'coin' LPAREN oC_DoubleLiteral RPAREN
	;

// sample(100)
// sample(100).by(T.id)
// sample(100).by('name')
// sample(100).by(select('a').by('name'))
// sample(100).by(out().count())
traversalMethod_sample
    : 'sample' LPAREN oC_IntegerLiteral RPAREN (DOT traversalMethod_sampleby) ?
    ;

traversalMethod_sampleby
    : 'by' LPAREN traversalToken RPAREN
    | 'by' LPAREN StringLiteral RPAREN
    | 'by' LPAREN nestedTraversal RPAREN
    ;

nestedTraversalExpr
    : nestedTraversal (COMMA nestedTraversal)*
    ;

traversalMethod_match
	: 'match' LPAREN nestedTraversalExpr RPAREN
	;

traversalMethod_expr
    : 'expr' LPAREN StringLiteral RPAREN
    | 'expr' LPAREN oC_Expression RPAREN
    ;

// i.e. g.E().subgraph("graph_name")
traversalMethod_subgraph
	: 'subgraph' LPAREN StringLiteral RPAREN
	;

traversalMethod_bothV
	: 'bothV' LPAREN RPAREN
	;

traversalMethod_unfold
    : 'unfold' LPAREN RPAREN
    ;

traversalMethod_id
	: 'id' LPAREN RPAREN
	;

traversalMethod_label
	: 'label' LPAREN RPAREN
	;

traversalMethod_constant
	: 'constant' LPAREN oC_Literal RPAREN
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
    | traversalPredicate DOT AND LPAREN traversalPredicate RPAREN
    | traversalPredicate DOT OR LPAREN traversalPredicate RPAREN
    | traversalPredicate_containing         // TextP.containing
    | traversalPredicate_notContaining      // TextP.notContaining
    | traversalPredicate_startingWith       // TextP.startingWith
    | traversalPredicate_notStartingWith    // TextP.notStartingWith
    | traversalPredicate_endingWith         // TextP.endingWith
    | traversalPredicate_notEndingWith      // TextP.notEndingWith
    | traversalPredicate_not                // P.not
    | traversalPredicate_inside             // P.inside
    | traversalPredicate_outside            // P.outside
    ;

nestedTraversal
    : chainedTraversal
    | ANON_TRAVERSAL_ROOT DOT chainedTraversal
    ;

traversalPredicate_eq
    : ('P.eq' | 'eq') LPAREN oC_Literal RPAREN
    ;

traversalPredicate_neq
    : ('P.neq' | 'neq') LPAREN oC_Literal RPAREN
    ;

traversalPredicate_lt
    : ('P.lt' | 'lt') LPAREN oC_Literal RPAREN
    ;

traversalPredicate_lte
    : ('P.lte' | 'lte') LPAREN oC_Literal RPAREN
    ;

traversalPredicate_gt
    : ('P.gt' | 'gt') LPAREN oC_Literal RPAREN
    ;

traversalPredicate_gte
    : ('P.gte' | 'gte') LPAREN oC_Literal RPAREN
    ;

traversalPredicate_within
    : ('P.within' | 'within') LPAREN oC_ListLiteral RPAREN
    | ('P.within' | 'within') LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
    ;

traversalPredicate_without
    : ('P.without' | 'without') LPAREN oC_ListLiteral RPAREN
    | ('P.without' | 'without') LPAREN ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? RPAREN
    ;

traversalPredicate_containing
    : ('TextP.containing' | 'containing') LPAREN StringLiteral RPAREN
    ;

traversalPredicate_notContaining
    : ('TextP.notContaining' | 'notContaining') LPAREN StringLiteral RPAREN
    ;

traversalPredicate_not
    : ('P.not' | 'not') LPAREN traversalPredicate RPAREN
    ;

traversalPredicate_inside
    : ('P.inside' | 'inside') LPAREN oC_Literal COMMA oC_Literal RPAREN
    ;

traversalPredicate_outside
    : ('P.outside' | 'outside') LPAREN oC_Literal COMMA oC_Literal RPAREN
    ;

traversalPredicate_startingWith
    : ('TextP.startingWith' | 'startingWith') LPAREN StringLiteral RPAREN
    ;

traversalPredicate_notStartingWith
    : ('TextP.notStartingWith' | 'notStartingWith') LPAREN StringLiteral RPAREN
    ;

traversalPredicate_endingWith
    : ('TextP.endingWith' | 'endingWith') LPAREN StringLiteral RPAREN
    ;

traversalPredicate_notEndingWith
    : ('TextP.notEndingWith' | 'notEndingWith') LPAREN StringLiteral RPAREN
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
