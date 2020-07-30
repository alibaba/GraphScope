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

""" An almost-complete Python to Cython AST transformer, with injected
    GRAPE-specific translation.

Python AST nodes are translated to corresponding Cython AST nodes as
it is, except:

1. for top-level method, a Cython type annotation is attached to the
   function signature, for example,

    .. code:: python

        @graphscope.analytical.udf.peval('sssp')
        def PEval(frag, context):
            ...

    will be translated as:

    .. code:: cython

        cdef public void IncEval(Fragment  *frag, ComputeContext  *context):
            ...

    it will make Cython understand what we really want and generate proper
    Cpp code.

2. for invokation on methods inside :code:`graphscope.analytical.udf.core`, we generate
   proper special :code:`cdef` defintions, or proper Cpp invokations,
   just like :code:`cython.declare`, for example,

    .. code:: python

        heap = graphscope.analytical.udf.heap((float, 'node'))

        modified = lang.vector(bool, [False for _ in range(inner_vertices.size())])

    will be translated as:

    .. code:: cython

        cdef priority_queue[pair[double, NodeT]] heap

        cdef vector[bool] modified([False for _ in range(inner_vertices.size())])

    Note that :code:`float` in Python is mapped to :code:`double` in
    Cython (further in Cpp code).

    More specifically, we define a series of placeholders in module
    :code:`graphscope.analytical.udf.core`, which cannot be executed in pure python mode.
    The :code:`graphscope.analytical.udf.xxx` decorators will translate those ordinary
    *"assignment and call"* into a :code:`cdef` node in Cython AST.
"""

import ast
import functools
import inspect
import textwrap
import types
import warnings

from Cython.CodeWriter import CodeWriter
from Cython.Compiler import Builtin
from Cython.Compiler import StringEncoding
from Cython.Compiler.ExprNodes import *
from Cython.Compiler.ModuleNode import *
from Cython.Compiler.Nodes import *

from graphscope.analytical.udf.patch import patch_cython_codewriter
from graphscope.analytical.udf.utils import CType
from graphscope.analytical.udf.utils import ExpectFuncDef
from graphscope.analytical.udf.utils import LinesWrapper
from graphscope.analytical.udf.utils import PregelAggregatorType
from graphscope.analytical.udf.utils import ProgramModel
from graphscope.framework.errors import check_argument


class GRAPECompiler(ast.NodeVisitor):
    def __init__(self, name, vd_type, md_type, program_model=ProgramModel.Pregel):
        """
        Args:
            name: str. The name of class.
            vd_type: str. The type of the data stored in vertex.
            md_type: str. The type of the message.
            program_model: ProgramModel. 'Pregel' or 'PIE'
        """
        self._name = name
        self._vd_type = vd_type
        self._md_type = md_type
        self._program_model = program_model

        # store aggregate function indexed by name
        self.__registed_aggregators = {}
        self.__globals = {}
        self.__func_params_name_list = []
        self.__pyx_header = LinesWrapper()

    def set_pregel_program_model(self):
        self._program_model = ProgramModel.Pregel

    def set_pie_program_model(self):
        self._program_model = ProgramModel.PIE

    def parse(self, source):
        """Parse source into cython module node object.

        source: str
           The source code may represent a statement or expression.

        Raises:
           RuntimeError: unsupported ast trans from python to cython.
        """
        tree = ast.parse(textwrap.dedent(source))
        # associate `parent` reference to every node
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                setattr(child, "__parent__", node)
        cyast = self.visit(tree)
        return cyast

    def run(self, func_or_ast, pyx_header):
        self.__pyx_header = pyx_header

        # we already has a AST: just run it
        if isinstance(func_or_ast, ast.AST):
            cyast = self.visit(func_or_ast)
        else:
            check_argument(isinstance(func_or_ast, types.FunctionType))
            # ignore varargs and keywords
            self.__func_params_name_list = inspect.getfullargspec(func_or_ast).args
            self.__globals = func_or_ast.__globals__
            cyast = self.parse(inspect.getsource(func_or_ast))

        writer = patch_cython_codewriter(CodeWriter())
        cycode = "\n".join(writer.write(cyast).lines)
        return cycode

    def compile(self, source):
        """Compile source into cython code."""
        cyast = self.parse(source)
        writer = patch_cython_codewriter(CodeWriter())
        return "\n".join(writer.write(cyast).lines)

    def make_plain_arg(self, name, arg_loc):
        return CArgDeclNode(
            arg_loc,
            base_type=CSimpleBaseTypeNode(
                arg_loc,
                name=None,
                is_basic_c_type=0,
                signed=1,
                longness=0,
                is_self_arg=False,
            ),
            declarator=CNameDeclaratorNode(arg_loc, name=name),
            not_none=0,
            or_none=0,
            default=None,
            annotation=None,
        )

    def make_value_arg(self, value_type, name, arg_loc):
        return CArgDeclNode(
            arg_loc,
            base_type=CSimpleBaseTypeNode(
                arg_loc,
                name=value_type,
                is_basic_c_type=0,
                signed=1,
                longness=0,
                is_self_arg=False,
            ),
            declarator=CNameDeclaratorNode(arg_loc, name=name),
            not_none=0,
            or_none=0,
            default=None,
            annotation=None,
        )

    def make_ptr_arg(self, ptr_type, name, arg_loc):
        return CArgDeclNode(
            arg_loc,
            base_type=CSimpleBaseTypeNode(
                arg_loc,
                name=ptr_type,
                is_basic_c_type=0,
                signed=1,
                longness=0,
                is_self_arg=False,
            ),
            declarator=CPtrDeclaratorNode(
                arg_loc, base=CNameDeclaratorNode(arg_loc, name=name)
            ),
            not_none=0,
            or_none=0,
            default=None,
            annotation=None,
        )

    def make_ref_arg(self, ref_type, name, arg_loc):
        return CArgDeclNode(
            arg_loc,
            base_type=CSimpleBaseTypeNode(
                arg_loc,
                name=ref_type,
                is_basic_c_type=0,
                signed=1,
                longness=0,
                complex=0,
                is_self_arg=False,
                templates=None,
            ),
            declarator=CReferenceDeclaratorNode(
                arg_loc, base=CNameDeclaratorNode(arg_loc, name=name)
            ),
            not_none=0,
            or_none=0,
            default=None,
            annotation=None,
        )

    def make_template_arg(
        self, value_type, value_tpls, name, arg_loc, use_ptr=False, use_ref=False
    ):
        def mk_tpl_arg(n):
            return CComplexBaseTypeNode(
                arg_loc,
                base_type=CSimpleBaseTypeNode(
                    arg_loc,
                    name=n,
                    is_basic_c_type=0,
                    signed=1,
                    longness=0,
                    is_self_arg=False,
                ),
                declarator=CNameDeclaratorNode(
                    arg_loc, name="", cname=None, default=None
                ),
            )

        tpl_type = TemplatedTypeNode(
            arg_loc,
            positional_args=[mk_tpl_arg(n) for n in value_tpls],
            keyword_args=DictNode(arg_loc, key_value_pairs=[]),
            base_type_node=CSimpleBaseTypeNode(
                arg_loc,
                name=value_type,
                is_basic_c_type=0,
                signed=1,
                longness=0,
                is_self_arg=False,
            ),
        )
        if use_ptr:
            declarator = CPtrDeclaratorNode(
                arg_loc, base=CNameDeclaratorNode(arg_loc, name=name)
            )
        elif use_ref:
            declarator = CReferenceDeclaratorNode(
                arg_loc, base=CNameDeclaratorNode(arg_loc, name=name)
            )
        else:
            declarator = CNameDeclaratorNode(arg_loc, name=name)
        return CArgDeclNode(
            arg_loc,
            base_type=tpl_type,
            declarator=declarator,
            not_none=0,
            or_none=0,
            default=None,
            annotation=None,
        )

    def loc(self, node):
        return ["", 0, 0]

    def generic_visit(self, node):
        raise NotImplementedError("AST node %s is not supported yet" % node)

    def visit_Module(self, node):
        body = self.visit(node.body[0])
        return ModuleNode(self.loc(node), body=body)

    def visit_ImportFrom(self, node):
        raise RuntimeError("ImportFrom is not supported yet.")

    def visit_Import(self, node):
        raise RuntimeError("Import is not supported yet.")

    def visit_ClassDef(self, node):
        raise RuntimeError("Class definition is not supported yet.")

    def visit_JoinedStr(self, node):
        raise RuntimeError("Joinedstr is not supported yet.")

    def visit_Constant(self, node):
        if isinstance(node.value, int):
            return IntNode(self.loc(node), value=str(node.value))
        if isinstance(node.value, float):
            # We won't have c float, we map floating types to double
            return FloatNode(self.loc(node), value=str(node.value))
        if isinstance(node.value, str):
            if node.kind == "u":
                return UnicodeNode(self.loc(node), value=node.value, bytes_value=None)
            else:
                return StringNode(
                    self.loc(node),
                    value=node.value,
                    unicode_value=StringEncoding.EncodedString(node.value),
                )
        if (
            isinstance(Ellipsis, type)
            and isinstance(node.value, Ellipsis)
            or isinstance(node.value, type(Ellipsis))
        ):
            return EllipsisNode(self.loc(node))
        if isinstance(node.value, bytes):
            return BytesNode(self.loc(node), value=node.s)
        if node.value is None:
            return NoneNode(self.loc(node))
        raise NotImplementedError("Unknown constant value: %s" % node)

    def visit_Num(self, node):
        if isinstance(node.n, int):
            return IntNode(self.loc(node), value=str(node.n))
        if isinstance(node.n, float):
            return FloatNode(self.loc(node), value=str(node.n))
        if isinstance(node.n, complex):
            raise NotImplementedError("Not support complex constant yet")
        raise NotImplementedError("Unknown constant value: %s" % node)

    def visit_Str(self, node):
        return StringNode(
            self.loc(node),
            value=node.s,
            unicode_value=StringEncoding.EncodedString(node.s),
        )

    def visit_Bytes(self, node):
        return BytesNode(self.loc(node), value=node.s)

    def visit_List(self, node):
        return ListNode(self.loc(node), args=[self.visit(elt) for elt in node.elts])

    def visit_Tuple(self, node):
        return TupleNode(self.loc(node), args=[self.visit(elt) for elt in node.elts])

    def visit_Set(self, node):
        return SetNode(self.loc(node), args=[self.visit(elt) for elt in node.elts])

    def visit_Dict(self, node):
        kvs = [
            DictItemNode(self.loc(node), key=self.visit(k), value=self.visit(v))
            for k, v in zip(node.keys, node.values)
        ]
        return DictNode(self.loc(node), key_value_pairs=kvs)

    def visit_Ellipsis(self, node):
        return EllipsisNode(self.loc(node))

    def visit_NameConstant(self, node):
        if node.value in [True, False]:
            return BoolNode(self.loc(node), value=node.value)
        else:
            return NoneNode(self.loc(node))

    def visit_Name(self, node):
        return NameNode(self.loc(node), name=node.id)

    def visit_Expr(self, node):
        expr = self.visit(node.value)
        if isinstance(expr, CVarDefNode):
            return expr
        return ExprStatNode(self.loc(node), expr=expr)

    def visit_UnaryOp(self, node):
        if isinstance(node.op, ast.UAdd):
            return UnaryPlusNode(
                self.loc(node), operator="+", operand=self.visit(node.operand)
            )
        if isinstance(node.op, ast.USub):
            return UnaryMinusNode(
                self.loc(node), operator="-", operand=self.visit(node.operand)
            )
        if isinstance(node.op, ast.Not):
            return NotNode(self.loc(node), operand=self.visit(node.operand))
        if isinstance(node.op, ast.Invert):
            return TildeNode(
                self.loc(node), operator="~", operand=self.visit(node.operand)
            )

    def visit_UAdd(self, node):
        return "+"

    def visit_USub(self, node):
        return "-"

    def visit_Not(self, node):
        return "not"

    def visit_Invert(self, node):
        return "invert"

    def visit_BinOp(self, node):
        lhs = self.visit(node.left)
        rhs = self.visit(node.right)
        op_mapping = {
            ast.Add: (AddNode, "+"),
            ast.Sub: (SubNode, "-"),
            ast.Mult: (MulNode, "*"),
            ast.Div: (DivNode, "/"),
            ast.FloorDiv: (DivNode, "//"),
            ast.Mod: (ModNode, "%"),
            ast.Pow: (PowNode, "**"),
            ast.MatMult: (MatMultNode, "@"),
            ast.LShift: (IntBinopNode, "<<"),
            ast.RShift: (IntBinopNode, ">>"),
            ast.BitOr: (IntBinopNode, "|"),
            ast.BitXor: (IntBinopNode, "^"),
            ast.BitAnd: (IntBinopNode, "&"),
        }
        op_type, op = op_mapping[type(node.op)]
        return op_type(self.loc(op), operator=op, operand1=lhs, operand2=rhs)

    def visit_Add(self, node):
        return "+"

    def visit_Sub(self, node):
        return "-"

    def visit_Mult(self, node):
        return "*"

    def visit_Div(self, node):
        return "/"

    def visit_FloorDiv(self, node):
        return "//"

    def visit_Mod(self, node):
        return "%"

    def visit_Pow(self, node):
        return "**"

    def visit_LShift(self, node):
        return "<<"

    def visit_RShift(self, node):
        return ">>"

    def visit_BitOr(self, node):
        return "|"

    def visit_BitXor(self, node):
        return "^"

    def visit_BitAnd(self, node):
        return "&"

    def visit_MatMult(self, node):
        return "@"

    def visit_AnnAssign(self, node):
        annotation = NameNode(self.loc(node), name=node.annotation.id)
        lhs = NameNode(self.loc(node), name=node.target.id, annotation=annotation)
        rhs = self.visit(node.value)
        return SingleAssignmentNode(self.loc(node), lhs=lhs, rhs=rhs)

    def visit_BoolOp(self, node):
        return BoolBinopNode(
            self.loc(node),
            operator=self.visit(node.op),
            operand1=self.visit(node.values[0]),
            operand2=self.visit(node.values[1]),
        )

    def visit_And(self, node):
        return "and"

    def visit_Or(self, node):
        return "or"

    def visit_Compare(self, node):
        operator = self.visit(node.ops[0])
        operand1 = self.visit(node.left)
        operand2 = self.visit(node.comparators[0])
        if len(node.comparators) == 1:
            # single comparison
            return PrimaryCmpNode(
                self.loc(node), operator=operator, operand1=operand1, operand2=operand2
            )
        else:
            # multiple continuous comparison
            cascade_node = CascadedCmpNode(
                self.loc(node),
                operator=self.visit(node.ops[-1]),
                operand2=self.visit(node.comparators[-1]),
            )
            for op, comparator in zip(node.ops[-2:0:-1], node.comparators[-2:0:-1]):
                cascade_node = CascadedCmpNode(
                    self.loc(node),
                    operator=self.visit(op),
                    operand2=self.visit(comparator),
                    cascade=cascade_node,
                )
            return PrimaryCmpNode(
                self.loc(node),
                operator=operator,
                operand1=operand1,
                operand2=operand2,
                cascade=cascade_node,
            )

    def visit_Eq(self, node):
        return "=="

    def visit_NotEq(self, node):
        return "!="

    def visit_Lt(self, node):
        return "<"

    def visit_LtE(self, node):
        return "<="

    def visit_Gt(self, node):
        return ">"

    def visit_GtE(self, node):
        return ">="

    def visit_Is(self, node):
        return "is"

    def visit_IsNot(self, node):
        return "is not"

    def visit_In(self, node):
        return "in"

    def visit_NotIn(self, node):
        return "not in"

    def __flatten_func_name(self, name):
        if isinstance(name, ast.Name):
            return [name.id]
        if isinstance(name, ast.Attribute):
            return self.__flatten_func_name(name.value) + [name.attr]
        return []

    def __is_graphscope_api_call(self, node):
        flat_func_name = self.__flatten_func_name(node.func)
        if len(flat_func_name) == 0:
            return False
        if flat_func_name[0] in self.__func_params_name_list:
            return True
        # check from graphscope module
        cascade = self.__globals.get(flat_func_name[0])
        if cascade is None:
            return False
        for n in flat_func_name[1:]:
            if cascade is None or not hasattr(cascade, n):
                return False
            cascade = getattr(cascade, n)
        return cascade.__module__ == "graphscope.analytical.udf.types"

    def __visit_GraphScopeAPICall(self, node):
        obj = self.__flatten_func_name(node.func)[0]
        name = node.func.attr
        if obj == "graphscope":
            # graphscope.declare()
            if name == "declare":
                var = node.args[1].id
                var_type = node.args[0].attr
                return CVarDefNode(
                    self.loc(node),
                    base_type=CSimpleBaseTypeNode(
                        self.loc(node),
                        name=var_type,
                        module_path=[],
                        is_basic_c_type=0,
                        signed=1,
                    ),
                    declarators=[CNameDeclaratorNode(self.loc(node), name=var)],
                    visibility="private",
                )
        elif obj == "context" and name == "register_aggregator":
            # context.register_aggregator()
            args = node.args
            if len(args) != 2:
                raise ValueError("Params within register_aggregator incorrect.")
            if (
                isinstance(args[1], ast.Attribute)
                and args[1].value.id == "PregelAggregatorType"
            ):
                self.__registed_aggregators[str(args[0].s)] = args[1].attr
            return SimpleCallNode(
                self.loc(node),
                function=self.visit(node.func),
                args=[self.visit(arg) for arg in node.args],
            )
        elif obj == "context" and name == "aggregate":
            # context.aggregate()
            args = node.args
            if len(args) != 2:
                raise ValueError("Params within aggregate incorrect.")
            if str(args[0].s) not in self.__registed_aggregators.keys():
                raise KeyError(
                    "Aggregator %s not exist, you may want to register first."
                    % str(args[0].s)
                )
            ctype = PregelAggregatorType.extract_ctype(
                self.__registed_aggregators[str(args[0].s)]
            )
            return SimpleCallNode(
                self.loc(node),
                function=IndexNode(
                    self.loc(node),
                    base=AttributeNode(
                        self.loc(node),
                        obj=NameNode(self.loc(node), name=obj),
                        attribute=name,
                    ),
                    index=NameNode(self.loc(node), name=str(ctype)),
                ),
                args=[self.visit(arg) for arg in node.args],
            )
        elif obj == "context" and name == "get_aggregated_value":
            # context.get_aggregated_value()
            args = node.args
            if len(args) != 1:
                raise ValueError("Params within get_aggregated_value incorrect.")
            if str(args[0].s) not in self.__registed_aggregators.keys():
                raise KeyError(
                    "Aggregator %s not exist, you may want to register first."
                    % str(args[0].s)
                )
            ctype = PregelAggregatorType.extract_ctype(
                self.__registed_aggregators[str(args[0].s)]
            )
            return SimpleCallNode(
                self.loc(node),
                function=IndexNode(
                    self.loc(node),
                    base=AttributeNode(
                        self.loc(node),
                        obj=NameNode(self.loc(node), name=obj),
                        attribute=name,
                    ),
                    index=NameNode(self.loc(node), name=str(ctype)),
                ),
                args=[self.visit(arg) for arg in node.args],
            )
        else:
            return SimpleCallNode(
                self.loc(node),
                function=self.visit(node.func),
                args=[self.visit(arg) for arg in node.args],
            )

    def visit_Call(self, node):
        if self.__is_graphscope_api_call(node):
            return self.__visit_GraphScopeAPICall(node)

        if not node.keywords:
            return SimpleCallNode(
                self.loc(node),
                function=self.visit(node.func),
                args=[self.visit(arg) for arg in node.args],
            )
        else:
            # with kwargs param
            return GeneralCallNode(
                self.loc(node),
                function=self.visit(node.func),
                positional_args=TupleNode(
                    self.loc(node), args=[self.visit(arg) for arg in node.args]
                ),
                # keyword_args=DictNode(self.loc(node), key_value_pairs=[]))
                keyword_args=self._visit_keywords(node.keywords),
            )

    def _visit_keywords(self, node):
        kvs = []
        for keyword in node:
            kvs.append(self.visit_keyword(keyword))
        return DictNode(self.loc(node), key_value_pairs=kvs, reject_duplicates=True)

    def visit_keyword(self, node):
        key = IdentifierStringNode(self.loc(node), value=node.arg)
        return DictItemNode(self.loc(node), key=key, value=self.visit(node.value))

    def visit_IfExp(self, node):
        return CondExprNode(
            self.loc(node),
            test=self.visit(node.test),
            true_val=self.visit(node.body),
            false_val=self.visit(node.orelse),
        )

    def visit_Attribute(self, node):
        return AttributeNode(
            self.loc(node), obj=self.visit(node.value), attribute=node.attr
        )

    def visit_Subscript(self, node):
        return IndexNode(
            self.loc(node), base=self.visit(node.value), index=self.visit(node.slice)
        )

    def visit_Index(self, node):
        return self.visit(node.value)

    def visit_Slice(self, node):
        start = (
            NoneNode(self.loc(node)) if node.lower is None else self.visit(node.lower)
        )
        stop = (
            NoneNode(self.loc(node)) if node.upper is None else self.visit(node.upper)
        )
        step = NoneNode(self.loc(node)) if node.step is None else self.visit(node.step)
        return SliceNode(self.loc(node), start=start, stop=stop, step=step)

    def visit_ExtSlice(self, node):
        return TupleNode(self.loc(node), args=[self.visit(dim) for dim in node.dims])

    def visit_ListComp(self, node):
        check_argument(len(node.generators) == 1)
        # has if node or not
        has_if = True if node.generators[0].ifs else False
        expression_value = self.visit(node.elt)
        generator = node.generators[0]
        iter_value = IteratorNode(
            self.loc(generator.iter), sequence=self.visit(generator.iter)
        )
        comp_node = ComprehensionAppendNode(self.loc(generator), expr=expression_value)
        if has_if:
            check_argument(len(node.generators[0].ifs) == 1)
            # construct IfStatNode
            condition = self.visit(node.generators[0].ifs[0])
            body = comp_node
            if_stat_node = IfStatNode(
                self.loc(node),
                if_clauses=[
                    IfClauseNode(self.loc(node), condition=condition, body=body)
                ],
                else_clause=None,
            )
            loop = ForInStatNode(
                self.loc(node),
                target=self.visit(generator.target),
                iterator=iter_value,
                body=if_stat_node,
                else_clause=None,
                is_async=False,
            )
        else:
            loop = ForInStatNode(
                self.loc(node),
                target=self.visit(generator.target),
                iterator=iter_value,
                body=comp_node,
                else_clause=None,
                is_async=False,
            )
        return ComprehensionNode(
            self.loc(node),
            loop=loop,
            append=comp_node,
            type=Builtin.list_type,
            has_local_scope=True,
        )

    def visit_SetComp(self, node):
        assert len(node.generators) == 1
        # has if node or not
        has_if = True if node.generators[0].ifs else False
        expression_value = self.visit(node.elt)
        generator = node.generators[0]
        iter_value = IteratorNode(
            self.loc(generator.iter), sequence=self.visit(generator.iter)
        )
        comp_node = ComprehensionAppendNode(self.loc(generator), expr=expression_value)
        if has_if:
            assert len(node.generators[0].ifs) == 1
            # construct IfStatNode
            condition = self.visit(node.generators[0].ifs[0])
            body = comp_node
            if_stat_node = IfStatNode(
                self.loc(node),
                if_clauses=[
                    IfClauseNode(self.loc(node), condition=condition, body=body)
                ],
                else_clause=None,
            )
            loop = ForInStatNode(
                self.loc(node),
                target=self.visit(generator.target),
                iterator=iter_value,
                body=if_stat_node,
                else_clause=None,
                is_async=False,
            )
        else:
            loop = ForInStatNode(
                self.loc(node),
                target=self.visit(generator.target),
                iterator=iter_value,
                body=comp_node,
                else_clause=None,
                is_async=False,
            )
        return ComprehensionNode(
            self.loc(node), loop=loop, append=comp_node, type=Builtin.set_type
        )

    def visit_DictComp(self, node):
        assert len(node.generators) == 1
        # has if node or not
        has_if = True if node.generators[0].ifs else False
        generator = node.generators[0]
        iter_value = IteratorNode(
            self.loc(generator.iter), sequence=self.visit(generator.iter)
        )
        comp_node = DictComprehensionAppendNode(
            self.loc(generator),
            key_expr=self.visit(node.key),
            value_expr=self.visit(node.value),
        )
        if has_if:
            assert len(node.generators[0].ifs) == 1
            # construct IfStatNode
            condition = self.visit(node.generators[0].ifs[0])
            body = comp_node
            if_stat_node = IfStatNode(
                self.loc(node),
                if_clauses=[
                    IfClauseNode(self.loc(node), condition=condition, body=body)
                ],
                else_clause=None,
            )
            loop = ForInStatNode(
                self.loc(node),
                target=self.visit(generator.target),
                iterator=iter_value,
                body=if_stat_node,
                else_clause=None,
                is_async=False,
            )
        else:
            loop = ForInStatNode(
                self.loc(node),
                target=self.visit(generator.target),
                iterator=iter_value,
                body=comp_node,
                else_clause=None,
                is_async=False,
            )
        return ComprehensionNode(
            self.loc(node), loop=loop, append=comp_node, type=Builtin.dict_type
        )

    def visit_Assign(self, node):
        # `tuple` represents a multiple assign
        assert len(node.targets) == 1
        if (
            hasattr(node.targets[0], "id")
            and node.targets[0].id in self.__func_params_name_list
        ):
            raise RuntimeError("Can't assign to internal variables.")
        lhs = self.visit(node.targets[0])
        rhs = self.visit(node.value)
        return SingleAssignmentNode(self.loc(node), lhs=lhs, rhs=rhs)

    def visit_AugAssign(self, node):
        return InPlaceAssignmentNode(
            self.loc(node),
            operator=self.visit(node.op),
            lhs=self.visit(node.target),
            rhs=self.visit(node.value),
        )

    def visit_Raise(self, node):
        return RaiseStatNode(
            self.loc(node),
            exc_type=self.visit(node.exc),
            exc_value=None,
            exc_tb=None,
            cause=None if node.cause is None else self.visit(node.cause),
        )

    def visit_ExceptHandler(self, node):
        if node.type:
            pattern = [self.visit(node.type)]
            if node.name:
                target = NameNode(self.loc(node), name=node.name)
            else:
                target = None
        else:
            pattern = None
            target = None
        body = StatListNode(
            self.loc(node), stats=[self.visit(stat) for stat in node.body]
        )
        return ExceptClauseNode(
            self.loc(node),
            pattern=pattern,
            target=target,
            body=body,
            is_except_as=False,
        )

    def visit_Try(self, node):
        body = StatListNode(
            self.loc(node), stats=[self.visit(stat) for stat in node.body]
        )
        except_clauses = [self.visit(ec) for ec in node.handlers]
        if node.orelse:
            else_clause = StatListNode(
                self.loc(node), stats=[self.visit(stat) for stat in node.orelse]
            )
        else:
            else_clause = None

        try_except_stat_node = TryExceptStatNode(
            self.loc(node),
            body=body,
            except_clauses=except_clauses,
            else_clause=else_clause,
        )
        # with `finally` statement or not
        if node.finalbody:
            final_clause = StatListNode(
                self.loc(node), stats=[self.visit(stat) for stat in node.finalbody]
            )
            return TryFinallyStatNode(
                self.loc(node), body=try_except_stat_node, finally_clause=final_clause
            )
        else:
            return try_except_stat_node

    def visit_Assert(self, node):
        return AssertStatNode(
            self.loc(node),
            cond=self.visit(node.test),
            value=self.visit(node.msg) if node.msg else None,
        )

    def visit_Delete(self, node):
        return DelStatNode(
            self.loc(node), args=[self.visit(target) for target in node.targets]
        )

    def visit_Pass(self, node):
        return PassStatNode(self.loc(node))

    def visit_If(self, node):
        condition = self.visit(node.test)
        body = StatListNode(
            self.loc(node), stats=[self.visit(stat) for stat in node.body]
        )
        if node.orelse:
            else_body = StatListNode(
                self.loc(node), stats=[self.visit(stat) for stat in node.orelse]
            )
        else:
            else_body = None
        return IfStatNode(
            self.loc(node),
            if_clauses=[IfClauseNode(self.loc(node), condition=condition, body=body)],
            else_clause=else_body,
        )

    def visit_For(self, node):
        target_value = self.visit(node.target)
        iter_value = IteratorNode(self.loc(node.iter), sequence=self.visit(node.iter))
        body = StatListNode(
            self.loc(node), stats=[self.visit(stat) for stat in node.body]
        )
        if node.orelse:
            else_body = StatListNode(
                self.loc(node), stats=[self.visit(stat) for stat in node.orelse]
            )
        else:
            else_body = None
        return ForInStatNode(
            self.loc(node),
            target=target_value,
            iterator=iter_value,
            body=body,
            else_clause=else_body,
            is_async=False,
        )

    def visit_While(self, node):
        condition = self.visit(node.test)
        body = StatListNode(
            self.loc(node), stats=[self.visit(stat) for stat in node.body]
        )
        if node.orelse:
            else_body = StatListNode(
                self.loc(node), stats=[self.visit(stat) for stat in node.orelse]
            )
        else:
            else_body = None
        return WhileStatNode(
            self.loc(node), condition=condition, body=body, else_clause=else_body
        )

    def visit_withitem(self, node):
        return self.visit(node.context_expr)

    def visit_With(self, node):
        # multiple items is not supported yet
        assert len(node.items) == 1
        manager = self.visit(node.items[0])
        target = self.visit(node.items[0].optional_vars)
        body = StatListNode(
            self.loc(node), stats=[self.visit(stat) for stat in node.body]
        )
        return WithStatNode(self.loc(node), manager=manager, target=target, body=body)

    def visit_Break(self, node):
        return BreakStatNode(self.loc(node))

    def visit_Continue(self, node):
        return ContinueStatNode(self.loc(node))

    def visit_FunctionDef(self, node):
        def is_static_method(func):
            return (
                func.decorator_list
                and isinstance(func.decorator_list[0], ast.Name)
                and (func.decorator_list[0].id == "staticmethod")
            )

        if not is_static_method(node):
            raise RuntimeError("Missing decorator staticmethod.")

        function_name = node.name
        function_return_type = "void"

        if self._program_model == ProgramModel.PIE:  # PIE program model
            if function_name == ExpectFuncDef.INIT.value:
                args = node.args.args
                assert len(args) == 2, "The number of parameters does not match"
                args = [
                    self.make_ref_arg("Fragment", args[0].arg, self.loc(args[0])),
                    self.make_template_arg(
                        "Context",
                        [self._vd_type, self._md_type],
                        args[1].arg,
                        self.loc(args[1]),
                        use_ref=True,
                    ),
                ]
            elif function_name == ExpectFuncDef.PEVAL.value:
                args = node.args.args
                assert len(args) == 2, "The number of parameters does not match"
                args = [
                    self.make_ref_arg("Fragment", args[0].arg, self.loc(args[0])),
                    self.make_template_arg(
                        "Context",
                        [self._vd_type, self._md_type],
                        args[1].arg,
                        self.loc(args[1]),
                        use_ref=True,
                    ),
                ]
            elif function_name == ExpectFuncDef.INCEVAL.value:
                args = node.args.args
                assert len(args) == 2, "The number of parameters does not match"
                args = [
                    self.make_ref_arg("Fragment", args[0].arg, self.loc(args[0])),
                    self.make_template_arg(
                        "Context",
                        [self._vd_type, self._md_type],
                        args[1].arg,
                        self.loc(args[1]),
                        use_ref=True,
                    ),
                ]
            else:
                raise RuntimeError(
                    "Not recognized method named {}".format(function_name)
                )

        elif self._program_model == ProgramModel.Pregel:
            if function_name == ExpectFuncDef.INIT.value:
                args = node.args.args
                assert len(args) == 2, "The number of parameters does not match"
                args = [
                    self.make_template_arg(
                        "Vertex",
                        [self._vd_type, self._md_type],
                        args[0].arg,
                        self.loc(args[0]),
                        use_ref=True,
                    ),
                    self.make_template_arg(
                        "Context",
                        [self._vd_type, self._md_type],
                        args[1].arg,
                        self.loc(args[1]),
                        use_ref=True,
                    ),
                ]
            elif function_name == ExpectFuncDef.COMPUTE.value:
                args = node.args.args
                assert len(args) == 3, "The number of parameters does not match"
                args = [
                    self.make_template_arg(
                        "MessageIterator",
                        [self._md_type],
                        args[0].arg,
                        self.loc(args[0]),
                    ),
                    self.make_template_arg(
                        "Vertex",
                        [self._vd_type, self._md_type],
                        args[1].arg,
                        self.loc(args[1]),
                        use_ref=True,
                    ),
                    self.make_template_arg(
                        "Context",
                        [self._vd_type, self._md_type],
                        args[2].arg,
                        self.loc(args[2]),
                        use_ref=True,
                    ),
                ]
            elif function_name == ExpectFuncDef.COMBINE.value:
                args = node.args.args
                assert len(args) == 1, "The number of parameters does not match"
                args = [
                    self.make_template_arg(
                        "MessageIterator",
                        [self._md_type],
                        args[0].arg,
                        self.loc(args[0]),
                    )
                ]
                function_return_type = self._md_type
            else:
                raise RuntimeError(
                    "Not recognized method named {}".format(function_name)
                )

        base_type = CSimpleBaseTypeNode(
            self.loc(node),
            name=function_return_type,
            is_basic_c_type=1,
            signed=1,
            longness=0,
            is_self_arg=False,
        )

        declarator_name = function_name
        declarator = CFuncDeclaratorNode(
            self.loc(node),
            base=CNameDeclaratorNode(self.loc(node), name=declarator_name),
            args=args,
            has_varargs=False,
            exception_value=None,
            exception_check=False,
            nogil=True,
            with_gil=False,
            overridable=False,
        )
        # traverse body
        body = StatListNode(
            self.loc(node), stats=[self.visit(expr) for expr in node.body]
        )
        return CFuncDefNode(
            self.loc(node),
            visibility="public",
            base_type=base_type,
            declarator=declarator,
            body=body,
            modifiers=[],
            api=False,
            overridable=False,
            is_const_method=False,
        )

    def visit_Lambda(self, node):
        return LambdaNode(
            self.loc(node),
            args=[
                self.make_plain_arg(arg.arg, self.loc(arg)) for arg in node.args.args
            ],
            star_arg=None,
            starstar_arg=None,
            retult_expr=self.visit(node.body),
        )

    def visit_Return(self, node):
        if node.value is None:
            value = None
        else:
            value = self.visit(node.value)
        return ReturnStatNode(self.loc(node), value=value)

    def visit_Yield(self, node):
        return YieldExprNode(self.loc(node), expr=self.visit(node.value))

    def visit_YieldFrom(self, node):
        return YieldFromExprNode(self.loc(node), expr=self.visit(node.value))

    def visit_Global(self, node):
        return GlobalNode(self.loc(node), names=node.names)

    def visit_Nonlocal(self, node):
        return NonlocalNode(self.loc(node), names=node.names)

    def visit_Await(self, node):
        return AwaitExprNode(self.loc(node), expr=self.visit(node.value))
