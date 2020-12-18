#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

""" Patch for Cython writer
"""

import functools

from Cython.CodeWriter import CodeWriter


def patch_cython_codewriter(writer):  # noqa: C901
    """Patch for Cython CodeWriter.

       See also: https://github.com/cython/cython/pull/3907

    1. Patch for `return` expression.

        Examples:
        ---------

        >>> if True:
        >>>     pass
        >>> else:
        >>>     return


    2. Add for `del` expression.

        Examples:
        --------

        >>> del a
        >>> del a[0]

    3. Patch for `NameNode` node for AnnAssign op.

        Examples:
        ---------

        >>> a: int = 10
        >>> b: str = 'graphscope'


    4. Add for `RaiseStatNode` node.

        Examples:
        ---------

        >>> raise RuntimeError

    5. Patch for `visit_TryExceptStatNode` node.

        Examples:
        ---------

        >>> try:
        >>>     pass
        >>> except:
        >>>     pass
        >>> else:
        >>>     pass

    6. Patch for 'visit_ExceptClauseNode' node.

        Examples:
        ---------

        >>> try:
        >>>     pass
        >>> except IOError as e:
        >>>    pass

    7. Patch for 'visit_TryFinallyStatNode' node.

        Examples:
        ---------

        >>> try:
        >>>     pass
        >>> except IOError as e:
        >>>     pass
        >>> finally:
        >>>     pass

    8. Add for `AssertStatNode` node.

        Examples:
        ---------

        >>> assert a == 10
        >>> assert a == 10, "error msg here"

    9. Patch for `GlobalNode` and 'NonlocalNode' node.

        Examples:
        ---------

        >>> global a, b
        >>> nonlocal a, b

    10. Add for `YieldExprNode` and `YieldFromExprNode` node.

        Examples:
        ---------

        >>> yield (1, 2)
        >>> yield from [1, 2, 3, 4]

    11. Fix bug of `remove` method, which use a undefined method named `endswith`.

    12. Patch for `visit_LambdaNode` node.

        Examples:
        ---------

        >>> lambda x : x + 2

    13. Add for `visit_CascadedCmpNode` node.
        Patch for `visit_PrimaryCmpNode` node.

        Examaples:
        ----------

        >>> 5 < 6 <= 7 > 4 >= 3 > 2 != 1
        >>> True
    """

    # 1
    def visit_ReturnStatNode(self, node):
        self.startline(u"return")
        if node.value is not None:
            self.put(u" ")
            self.visit(node.value)
        self.endline()

    setattr(
        writer, "visit_ReturnStatNode", functools.partial(visit_ReturnStatNode, writer)
    )

    # 2
    def visit_DelStatNode(self, node):
        self.startline(u"del ")
        for arg in node.args:
            self.visit(arg)
        self.endline()

    setattr(writer, "visit_DelStatNode", functools.partial(visit_DelStatNode, writer))

    # 3
    def visit_NameNode(self, node):
        self.put(node.name)
        if node.annotation:
            self.put(u" : ")
            self.visit(node.annotation)

    setattr(writer, "visit_NameNode", functools.partial(visit_NameNode, writer))

    # 4
    def visit_RaiseStatNode(self, node):
        self.startline(u"raise ")
        self.visit(node.exc_type)
        self.endline()

    setattr(
        writer, "visit_RaiseStatNode", functools.partial(visit_RaiseStatNode, writer)
    )

    # 5
    def visit_TryExceptStatNode(self, node):
        self.line(u"try:")
        self.indent()
        self.visit(node.body)
        self.dedent()
        for x in node.except_clauses:
            self.visit(x)
        if node.else_clause is not None:
            self.line(u"else:")
            self.indent()
            self.visit(node.else_clause)
            self.dedent()

    setattr(
        writer,
        "visit_TryExceptStatNode",
        functools.partial(visit_TryExceptStatNode, writer),
    )

    # 6
    def visit_ExceptClauseNode(self, node):
        # node.pattern is a list
        self.startline(u"except")
        if node.pattern is not None:
            self.put(u" ")
            self.visit(node.pattern[0])
        if node.target is not None:
            self.put(u" as ")
            self.visit(node.target)
        self.endline(":")
        self.indent()
        self.visit(node.body)
        self.dedent()

    setattr(
        writer,
        "visit_ExceptClauseNode",
        functools.partial(visit_ExceptClauseNode, writer),
    )

    # 7
    def visit_TryFinallyStatNode(self, node):
        self.visit(node.body)
        self.line(u"finally:")
        self.indent()
        self.visit(node.finally_clause)
        self.dedent()

    setattr(
        writer,
        "visit_TryFinallyStatNode",
        functools.partial(visit_TryFinallyStatNode, writer),
    )

    # 8
    def visit_AssertStatNode(self, node):
        self.startline(u"assert ")
        self.visit(node.cond)
        if node.value:
            self.put(u", ")
            self.visit(node.value)
        self.endline()

    setattr(
        writer, "visit_AssertStatNode", functools.partial(visit_AssertStatNode, writer)
    )

    # 9
    def visit_GlobalNode(self, node):
        self.startline(u"global ")
        for item in node.names[:-1]:
            self.put(item)
            self.put(u", ")
        self.put(node.names[-1])
        self.endline()

    setattr(writer, "visit_GlobalNode", functools.partial(visit_GlobalNode, writer))

    # 9
    def visit_NonlocalNode(self, node):
        self.startline(u"nonlocal ")
        for item in node.names[:-1]:
            self.put(item)
            self.put(u", ")
        self.put(node.names[-1])
        self.endline()

    setattr(writer, "visit_NonlocalNode", functools.partial(visit_NonlocalNode, writer))

    # 10
    def visit_YieldExprNode(self, node):
        self.put(u"yield ")
        self.visit(node.expr)

    setattr(
        writer, "visit_YieldExprNode", functools.partial(visit_YieldExprNode, writer)
    )

    # 10
    def visit_YieldFromExprNode(self, node):
        self.put(u"yield from ")
        self.visit(node.expr)

    setattr(
        writer,
        "visit_YieldFromExprNode",
        functools.partial(visit_YieldFromExprNode, writer),
    )

    # 11
    def remove(self, s):
        if self.result.s.endswith(s):
            self.result.s = self.result.s[: -len(s)]

    setattr(writer, "remove", functools.partial(remove, writer))

    # 12
    def visit_LambdaNode(self, node):
        self.startline(u"lambda ")
        self.comma_separated_list(node.args)
        self.put(u" : ")
        self.visit(node.retult_expr)

    setattr(writer, "visit_LambdaNode", functools.partial(visit_LambdaNode, writer))

    # 13
    def visit_CascadedCmpNode(self, node):
        self.put(u" %s " % node.operator)
        self.visit(node.operand2)
        if node.cascade:
            # recursion
            self.visit(node.cascade)

    setattr(
        writer,
        "visit_CascadedCmpNode",
        functools.partial(visit_CascadedCmpNode, writer),
    )

    def visit_PrimaryCmpNode(self, node):
        self.visit_BinopNode(node)
        if node.cascade:
            self.visit(node.cascade)

    setattr(
        writer, "visit_PrimaryCmpNode", functools.partial(visit_PrimaryCmpNode, writer)
    )
    return writer
