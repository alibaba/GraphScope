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

import ExprGS;

oC_Cypher
      :  SP? oC_Statement ( SP? ';' )? SP? EOF ;

oC_Statement
      :  oC_Query ;

oC_Query
     :  oC_RegularQuery
     |  oC_StandaloneCall
     ;

oC_StandaloneCall
     :  CALL SP ( oC_ExplicitProcedureInvocation | oC_ImplicitProcedureInvocation ) ( SP? YIELD SP ( '*' ) )? ;

oC_ExplicitProcedureInvocation
     :  oC_ProcedureName SP? '(' SP? ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? ')' ;

oC_ImplicitProcedureInvocation
     :  oC_ProcedureName ;

oC_ProcedureName
     :  oC_Namespace oC_SymbolicName ;

oC_ProcedureResultField
     :  oC_SymbolicName ;

oC_UpdatingClause
      : oC_Create
      | oC_Delete
      | oC_Set
      | oC_LoadCSV
      ;

CREATE : ( 'C' | 'c' ) ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

oC_Create
      :  CREATE SP? oC_Pattern ;

oC_Set
   :  SET SP? oC_SetItem ( SP? ',' SP? oC_SetItem )* ;

SET : ( 'S' | 's' ) ( 'E' | 'e' ) ( 'T' | 't' ) ;

oC_SetItem
       :  ( oC_PropertyOrLabelsExpression SP? '=' SP? oC_Expression )
           | ( oC_Variable SP? '=' SP? oC_Expression )
           ;

oC_Delete
      :  ( DETACH SP )? DELETE SP? oC_Expression ( SP? ',' SP? oC_Expression )* ;

DETACH : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

DELETE : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

oC_LoadCSV
    : LOAD SP CSV ( SP oC_WithHeaders )? SP FROM SP oC_Expression SP AS SP oC_Variable ( SP oC_FieldTerminator )?;

LOAD: ( 'L' | 'l' ) ( 'O' | 'o' ) ( 'A' | 'a' ) ( 'D' | 'd' );

CSV: ( 'C' | 'c' ) ( 'S' | 's' ) ( 'V' | 'v' );

oC_WithHeaders
    : WITH SP HEADERS
    ;

HEADERS: ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'A' | 'a' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'S' | 's' );

oC_FieldTerminator
    : FIELDTERMINATOR SP oC_Expression ;

FIELDTERMINATOR: ( 'F' | 'f' ) ( 'I' | 'i' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'D' | 'd' ) ( 'T' | 't' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'O' | 'o' ) ( 'R' | 'r' );

FROM: ( 'F' | 'f' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'M' | 'm' );

CALL : ( 'C' | 'c' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

YIELD : ( 'Y' | 'y' ) ( 'I' | 'i' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'D' | 'd' ) ;

oC_RegularQuery
     : ( ( oC_ReadQueryPart | oC_UpdatingClause ) SP? )* SP? oC_ReadQueryPart ( SP oC_Return )
     | ( ( oC_ReadQueryPart | oC_UpdatingClause ) SP? )* SP? oC_UpdatingClause ( SP oC_Return )?
     ;

oC_ReadQueryPart
    : oC_Match ( SP? ( oC_Match | oC_With ) )*
    ;

oC_Match
     :  ( OPTIONAL SP )? MATCH SP? oC_Pattern ( SP? oC_Where )? ;

MATCH : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

OPTIONAL : ( 'O' | 'o' ) ( 'P' | 'p' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ;

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
