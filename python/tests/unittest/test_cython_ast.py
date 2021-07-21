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

import ast
import textwrap

import pytest
from Cython.Compiler.ModuleNode import ModuleNode
from Cython.Compiler.TreeFragment import TreeFragment
from Cython.Compiler.TreeFragment import parse_from_strings

import graphscope
from graphscope.analytical.udf.compile import GRAPECompiler


@pytest.fixture(scope="module")
def compiler():
    compiler = GRAPECompiler("Compile Test", "double", "double")
    yield compiler


def test_module_import(compiler):
    with pytest.raises(RuntimeError, match="Import is not supported yet"):
        s1 = "import a"
        r1 = compiler.compile(s1)

    with pytest.raises(RuntimeError, match="Import is not supported yet"):
        s2 = "import a.b as b"
        r2 = compiler.compile(s2)

    with pytest.raises(RuntimeError, match="ImportFrom is not supported yet"):
        s3 = "from a import b"
        r3 = compiler.compile(s3)

    with pytest.raises(RuntimeError, match="ImportFrom is not supported yet"):
        s4 = "from a.b import b as b"
        r4 = compiler.compile(s4)


def test_class_def(compiler):
    with pytest.raises(RuntimeError, match="Class definition is not supported yet"):
        s1 = "class A(object):\n    pass"
        r1 = compiler.compile(s1)


def test_return(compiler):
    s1 = "return"
    e1 = "return"
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = "return 10"
    e2 = "return 10"
    r2 = compiler.compile(s2)
    assert r2 == e2

    s3 = "return None"
    e3 = "return None"
    r3 = compiler.compile(s3)
    assert r3 == e3


def test_delete(compiler):
    s1 = "del a"
    e1 = "del a"
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = "del a[0]"
    e2 = "del a[0]"
    r2 = compiler.compile(s2)
    assert r2 == e2


def test_assign(compiler):
    s1 = "a = 10"
    e1 = "a = 10"
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = "a, b = 10, 20"
    e2 = "(a, b) = (10, 20)"
    r2 = compiler.compile(s2)
    assert r2 == e2

    s3 = "a, b = (c, d)"
    e3 = "(a, b) = (c, d)"
    r3 = compiler.compile(s3)
    assert r3 == e3

    # multi assign
    s4 = "a, b = 10"
    e4 = "(a, b) = 10"
    r4 = compiler.compile(s4)
    assert r4 == e4


def test_aug_assign(compiler):
    # AugAssign
    s4 = "a += 10"
    e4 = "a += 10"
    r4 = compiler.compile(s4)
    assert r4 == e4

    s5 = "a -= 10"
    e5 = "a -= 10"
    r5 = compiler.compile(s5)
    assert r5 == e5

    s6 = "a *= 10"
    e6 = "a *= 10"
    r6 = compiler.compile(s6)
    assert r6 == e6

    s7 = "a @= 10"
    e7 = "a @= 10"
    r7 = compiler.compile(s7)
    assert r7 == e7

    s8 = "a /= 10"
    e8 = "a /= 10"
    r8 = compiler.compile(s8)
    assert r8 == e8

    s9 = "a %= 10"
    e9 = "a %= 10"
    r9 = compiler.compile(s9)
    assert r9 == e9

    s10 = "a &= 10"
    e10 = "a &= 10"
    r10 = compiler.compile(s10)
    assert r10 == e10

    s11 = "a |= 10"
    e11 = "a |= 10"
    r11 = compiler.compile(s11)
    assert r11 == e11

    s12 = "a ^= 10"
    e12 = "a ^= 10"
    r12 = compiler.compile(s12)
    assert r12 == e12

    s13 = "a <<= 10"
    e13 = "a <<= 10"
    r13 = compiler.compile(s13)
    assert r13 == e13

    s14 = "a >>= 10"
    e14 = "a >>= 10"
    r14 = compiler.compile(s14)
    assert r14 == e14

    s15 = "a **= 10"
    e15 = "a **= 10"
    r15 = compiler.compile(s15)
    assert r15 == e15

    s16 = "a //= 10"
    e16 = "a //= 10"
    r16 = compiler.compile(s16)
    assert r16 == e16


def test_ann_assign(compiler):
    s17 = "a : int = 10"
    e17 = "a : int = 10"
    r17 = compiler.compile(s17)
    assert r17 == e17


def test_for(compiler):
    s1 = textwrap.dedent(
        """\
        for i in range(10):
            pass"""
    )
    e1 = textwrap.dedent(
        """\
        for i in range(10):
            pass"""
    )
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = textwrap.dedent(
        """\
        for i in [1, 2, 3]:
            pass"""
    )
    e2 = textwrap.dedent(
        """\
        for i in [1, 2, 3]:
            pass"""
    )
    r2 = compiler.compile(s2)
    assert r2 == e2

    # for else
    s3 = textwrap.dedent(
        """\
        for i in range(10):
            pass
        else:
            pass"""
    )
    e3 = textwrap.dedent(
        """\
        for i in range(10):
            pass
        else:
            pass"""
    )
    r3 = compiler.compile(s3)
    assert r3 == e3

    # async for is unsupported yet, since we can't import asyncio


def test_while(compiler):
    s1 = textwrap.dedent(
        """\
        while True:
            pass"""
    )
    e1 = textwrap.dedent(
        """\
        while True:
            pass"""
    )
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = textwrap.dedent(
        """\
        while a:
            pass"""
    )
    e2 = textwrap.dedent(
        """\
        while a:
            pass"""
    )
    r2 = compiler.compile(s2)
    assert r2 == e2

    s3 = textwrap.dedent(
        """\
        while i < 10:
            pass"""
    )
    e3 = textwrap.dedent(
        """\
        while i < 10:
            pass"""
    )
    r3 = compiler.compile(s3)
    assert r3 == e3


def test_if(compiler):
    s1 = textwrap.dedent(
        """\
        if True:
            pass"""
    )
    e1 = textwrap.dedent(
        """\
        if True:
            pass"""
    )
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = textwrap.dedent(
        """\
        if a:
            pass"""
    )
    e2 = textwrap.dedent(
        """\
        if a:
            pass"""
    )
    r2 = compiler.compile(s2)
    assert r2 == e2

    s3 = textwrap.dedent(
        """\
        if i < 10:
            pass"""
    )
    e3 = textwrap.dedent(
        """\
        if i < 10:
            pass"""
    )
    r3 = compiler.compile(s3)
    assert r3 == e3

    # test with else
    s4 = textwrap.dedent(
        """\
        if True:
            pass
        else:
            pass"""
    )
    e4 = textwrap.dedent(
        """\
        if True:
            pass
        else:
            pass"""
    )
    r4 = compiler.compile(s4)
    assert r4 == e4

    # test with elif
    # s5 and e5 are equivalent
    s5 = textwrap.dedent(
        """\
        if a:
            pass
        elif b:
            pass
        else:
            pass"""
    )
    e5 = textwrap.dedent(
        """\
        if a:
            pass
        else:
            if b:
                pass
            else:
                pass"""
    )
    r5 = compiler.compile(s5)
    assert r5 == e5

    s6 = textwrap.dedent(
        """\
        if a:
            pass
        else:
            if b:
                pass
            else:
                pass"""
    )
    e6 = textwrap.dedent(
        """\
        if a:
            pass
        else:
            if b:
                pass
            else:
                pass"""
    )
    r6 = compiler.compile(s6)
    assert r6 == e6

    # test nesting
    s7 = textwrap.dedent(
        """\
        if a:
            if b:
                pass
            else:
                pass
            pass
        else:
            pass"""
    )
    e7 = textwrap.dedent(
        """\
        if a:
            if b:
                pass
            else:
                pass
            pass
        else:
            pass"""
    )
    r7 = compiler.compile(s7)
    assert r7 == e7


def test_with(compiler):
    s1 = textwrap.dedent(
        """\
        with fin.open('file') as f:
            pass"""
    )
    e1 = textwrap.dedent(
        """\
        with fin.open('file') as f:
            pass"""
    )
    r1 = compiler.compile(s1)
    assert r1 == s1

    s2 = textwrap.dedent(
        """\
        with a as b:
            pass"""
    )
    e2 = textwrap.dedent(
        """\
        with a as b:
            pass"""
    )
    r2 = compiler.compile(s2)
    assert r2 == s2

    # multiple items with is not supported yet
    with pytest.raises(AssertionError):
        s3 = textwrap.dedent(
            """\
            with a as f, b as c:
                pass"""
        )
        r3 = compiler.compile(s3)


def test_raise(compiler):
    s1 = "raise RuntimeError"
    e1 = "raise RuntimeError"
    r1 = compiler.compile(s1)
    assert r1 == s1


def test_try(compiler):
    # try except else
    s1 = textwrap.dedent(
        """\
        try:
            pass
        except:
            pass
        else:
            pass"""
    )
    e1 = textwrap.dedent(
        """\
        try:
            pass
        except:
            pass
        else:
            pass"""
    )
    r1 = compiler.compile(s1)
    assert r1 == e1

    # try multiple except with else
    s2 = textwrap.dedent(
        """\
        try:
            pass
        except IOError as e:
            pass
        except RuntimeError as e:
            pass
        else:
            pass"""
    )
    e2 = textwrap.dedent(
        """\
        try:
            pass
        except IOError as e:
            pass
        except RuntimeError as e:
            pass
        else:
            pass"""
    )
    r2 = compiler.compile(s2)
    assert r2 == e2

    # try except
    s3 = textwrap.dedent(
        """\
        try:
            pass
        except:
            pass"""
    )
    e3 = textwrap.dedent(
        """\
        try:
            pass
        except:
            pass"""
    )
    r3 = compiler.compile(s3)
    assert r3 == e3

    # try except with finally
    s5 = textwrap.dedent(
        """\
        try:
            pass
        except:
            pass
        finally:
            pass"""
    )
    e5 = textwrap.dedent(
        """\
        try:
            pass
        except:
            pass
        finally:
            pass"""
    )
    r5 = compiler.compile(s5)
    assert r5 == e5


def test_assert(compiler):
    s1 = "assert a == 10"
    e1 = "assert a == 10"
    r1 = compiler.compile(s1)
    assert r1 == s1

    s2 = "assert a == 10, 'error msg here.'"
    e2 = "assert a == 10, 'error msg here.'"
    r2 = compiler.compile(s2)
    assert r2 == s2


def test_global(compiler):
    s1 = "global a, b, c"
    e1 = "global a, b, c"
    r1 = compiler.compile(s1)
    assert r1 == e1


def test_non_local(compiler):
    s1 = "nonlocal a, b, c"
    e1 = "nonlocal a, b, c"
    r1 = compiler.compile(s1)
    assert r1 == e1


def test_expr(compiler):
    # bool op
    s1 = "a or b"
    e1 = "a or b"
    r1 = compiler.compile(s1)
    assert r1 == e1

    s2 = "a and b"
    e2 = "a and b"
    r2 = compiler.compile(s2)
    assert r2 == e2

    # named expr
    s3 = "a"
    e3 = "a"
    r3 = compiler.compile(s3)
    assert r3 == e3

    # bin op
    s4 = "a + b"  # add
    e4 = "a + b"
    r4 = compiler.compile(s4)
    assert r4 == e4

    s5 = "a - b"  # sub
    e5 = "a - b"
    r5 = compiler.compile(s5)
    assert r5 == e5

    s6 = "a * b"  # mul
    e6 = "a * b"
    r6 = compiler.compile(s6)
    assert r6 == e6

    s7 = "a @= b"  # matmult
    e7 = "a @= b"
    r7 = compiler.compile(s7)
    assert r7 == e7

    s8 = "a / b"  # div
    e8 = "a / b"
    r8 = compiler.compile(s8)
    assert r8 == e8

    s9 = "a % b"  # mod
    e9 = "a % b"
    r9 = compiler.compile(s9)
    assert r9 == e9

    s10 = "a ** b"  # pow
    e10 = "a ** b"
    r10 = compiler.compile(s10)
    assert r10 == e10

    s11 = "a << b"  # lshift
    e11 = "a << b"
    r11 = compiler.compile(s11)
    assert r11 == e11

    s12 = "a >> b"  # rshift
    e12 = "a >> b"
    r12 = compiler.compile(s12)
    assert r12 == e12

    s13 = "a | b"  # bit or
    e13 = "a | b"
    r13 = compiler.compile(s13)
    assert r13 == e13

    s14 = "a ^ b"  # bit xor
    e14 = "a ^ b"
    r14 = compiler.compile(s14)
    assert r14 == e14

    s15 = "a & b"  # bit and
    e15 = "a & b"
    r15 = compiler.compile(s15)
    assert r15 == e15

    s16 = "a // b"  # floor div
    e16 = "a // b"
    r16 = compiler.compile(s16)
    assert r16 == e16

    # unary op
    s17 = "~a"  # invert
    e17 = "~a"
    r17 = compiler.compile(s17)
    assert r17 == e17

    s18 = "+a"  # uadd
    e18 = "+a"
    r18 = compiler.compile(s18)
    assert r18 == e18

    s19 = "-a"  # usub
    e19 = "-a"
    r19 = compiler.compile(s19)
    assert r19 == e19

    s20 = "not a"  # not
    e20 = "not a"
    r20 = compiler.compile(s20)
    assert r20 == e20

    # dict
    s21 = textwrap.dedent(
        """\
        {'a': 10, 'b': 'str'}"""
    )
    e21 = textwrap.dedent(
        """\
        {'a': 10, 'b': 'str'}"""
    )
    r21 = compiler.compile(s21)
    assert r21 == e21

    # set
    s22 = textwrap.dedent(
        """\
        a = {1, 2, 3}"""
    )
    e22 = textwrap.dedent(
        """\
        a = {1, 2, 3}"""
    )
    r22 = compiler.compile(s22)
    assert r22 == e22

    # tuple
    s23 = textwrap.dedent(
        """\
        a = (1, 2, 3)"""
    )
    e23 = textwrap.dedent(
        """\
        a = (1, 2, 3)"""
    )
    r23 = compiler.compile(s23)
    assert r23 == e23

    # list
    s24 = textwrap.dedent(
        """\
        a = [1, 2, 3]"""
    )
    e24 = textwrap.dedent(
        """\
        a = [1, 2, 3]"""
    )
    r24 = compiler.compile(s24)
    assert r24 == e24

    # list comprehension
    s25 = textwrap.dedent(
        """\
        [a for a in 'human' if a in 'man']"""
    )
    e25 = textwrap.dedent(
        """\
        [a for a in 'human' if a in 'man']"""
    )
    r25 = compiler.compile(s25)
    assert r25 == e25

    s26 = textwrap.dedent(
        """\
        [a for a in 'human']"""
    )
    e26 = textwrap.dedent(
        """\
        [a for a in 'human']"""
    )
    r26 = compiler.compile(s26)
    assert r26 == e26

    # set comprehension
    s27 = textwrap.dedent(
        """\
        {a for a in 'human' if a in 'man'}"""
    )
    e27 = textwrap.dedent(
        """\
        {a for a in 'human' if a in 'man'}"""
    )
    r27 = compiler.compile(s27)
    assert r27 == e27

    s28 = textwrap.dedent(
        """\
        {a for a in 'human'}"""
    )
    e28 = textwrap.dedent(
        """\
        {a for a in 'human'}"""
    )
    r28 = compiler.compile(s28)
    assert r28 == e28

    # dict comprehension
    s29 = textwrap.dedent(
        """\
        {k: v for (k, v) in d.items() if k in ['a'] or v in [20]}"""
    )
    e29 = textwrap.dedent(
        """\
        {k: v for (k, v) in d.items() if k in ['a'] or v in [20]}"""
    )
    r29 = compiler.compile(s29)
    assert r29 == e29

    s30 = textwrap.dedent(
        """\
        {k: v for (k, v) in d.items()}"""
    )
    e30 = textwrap.dedent(
        """\
        {k: v for (k, v) in d.items()}"""
    )
    r30 = compiler.compile(s30)
    assert r30 == e30

    # generator exp
    s31 = textwrap.dedent(
        """\
        [num ** 2 for num in range(5)]"""
    )
    e31 = textwrap.dedent(
        """\
        [num ** 2 for num in range(5)]"""
    )
    r31 = compiler.compile(s31)
    assert r31 == e31

    # await is unsupported yet, since we can't import asyncio

    # yield
    s32 = "yield (a, b)"
    e32 = "yield (a, b)"
    r32 = compiler.compile(s32)
    assert r32 == e32

    # yield from
    s33 = "yield from [1, 2, 3]"
    e33 = "yield from [1, 2, 3]"
    r33 = compiler.compile(s33)
    assert r33 == e33

    # compare expr
    s34 = "a == b"  # eq
    e34 = "a == b"
    r34 = compiler.compile(s34)
    assert r34 == e34

    s35 = "a != b"  # not eq
    e35 = "a != b"
    r35 = compiler.compile(s35)
    assert r35 == e35

    s36 = "a < b"  # lt
    e36 = "a < b"
    r36 = compiler.compile(s36)
    assert r36 == e36

    s37 = "a <= b"  # lte
    e37 = "a <= b"
    r37 = compiler.compile(s37)
    assert r37 == e37

    s38 = "a > b"  # gt
    e38 = "a > b"
    r38 = compiler.compile(s38)
    assert r38 == e38

    s39 = "a >= b"  # gte
    e39 = "a >= b"
    r39 = compiler.compile(s39)
    assert r39 == e39

    s40 = "a is b"  # is
    e40 = "a is b"
    r40 = compiler.compile(s40)
    assert r40 == e40

    s41 = "a is not b"  # is not
    e41 = "a is not b"
    r41 = compiler.compile(s41)
    assert r41 == e41

    s42 = "a in b"  # in
    e42 = "a in b"
    r42 = compiler.compile(s42)
    assert r42 == e42

    s43 = "a not in b"  # not in
    e43 = "a not in b"
    r43 = compiler.compile(s43)
    assert r43 == e43

    s44 = "a ^ b"  # bit xor
    e44 = "a ^ b"
    r44 = compiler.compile(s44)
    assert r44 == e44

    # call
    s45 = "f()"
    e45 = "f()"
    r45 = compiler.compile(s45)
    assert r45 == e45

    s46 = "f(a, b)"
    e46 = "f(a, b)"
    r46 = compiler.compile(s46)
    assert r46 == e46

    s47 = "c = f(a, b=20)"
    e47 = "c = f(a, b=20)"
    r47 = compiler.compile(s47)
    assert r47 == e47

    s48 = "f(a=10, b=20)"
    e48 = "f(a=10, b=20)"
    r48 = compiler.compile(s48)
    assert r48 == e48

    # formated and joinedstr expr
    with pytest.raises(RuntimeError, match="Joinedstr is not supported yet"):
        s49 = textwrap.dedent(
            """\
            f'{x}.{y}'
        """
        )
        r49 = compiler.compile(s49)

    # complex compare
    s50 = "5 < 6 <= 7 > 4 >= 3 > 2 != 1"  # not in
    e50 = "5 < 6 <= 7 > 4 >= 3 > 2 != 1"
    r50 = compiler.compile(s50)
    assert r50 == e50


def test_misc(compiler):
    # pass keyword
    s1 = "pass"
    e1 = "pass"
    r1 = compiler.compile(s1)
    assert r1 == e1

    # break keyword
    s2 = "break"
    e2 = "break"
    r2 = compiler.compile(s2)
    assert r2 == e2

    # continue keyword
    s3 = "continue"
    e3 = "continue"
    r3 = compiler.compile(s3)
    assert r3 == e3

    # str format
    s4 = textwrap.dedent(
        """\
        '{}.{}'.format('a', 'b')"""
    )
    e4 = textwrap.dedent(
        """\
        '{}.{}'.format('a', 'b')"""
    )
    r4 = compiler.compile(e4)
    assert r4 == e4


def test_lambda(compiler):
    s1 = "lambda x, y : x + y"
    e1 = "lambda x, y : x + y"
    r1 = compiler.compile(s1)
    assert r1 == e1


def test_function_def(compiler):
    # Only support function definition from Pregel/PIE model.
    with pytest.raises(RuntimeError, match="Not recognized method"):
        s1 = textwrap.dedent(
            """\
            @staticmethod
            def f(*args, **kwargs):
                pass"""
        )
        r1 = compiler.compile(s1)

    compiler.set_pregel_program_model()
    # Pregel Init
    s2 = textwrap.dedent(
        """\
        @staticmethod
        def Init(v, context):
            pass"""
    )
    e2 = textwrap.dedent(
        """\
        cdef public void Init(Vertex[double, double] &v, Context[double, double] &context):
            pass"""
    )
    r2 = compiler.compile(s2)
    assert r2 == e2

    # Pregel Compute
    s3 = textwrap.dedent(
        """\
        @staticmethod
        def Compute(messages, v, context):
            pass"""
    )
    e3 = textwrap.dedent(
        """\
        cdef public void Compute(MessageIterator[double] messages, Vertex[double, double] &v, Context[double, double] &context):
            pass"""
    )
    r3 = compiler.compile(s3)
    assert r3 == e3

    # Pregel Combine
    s4 = textwrap.dedent(
        """\
        @staticmethod
        def Combine(messages):
            pass"""
    )
    e4 = textwrap.dedent(
        """\
        cdef public double Combine(MessageIterator[double] messages):
            pass"""
    )
    r4 = compiler.compile(s4)
    assert r4 == e4

    compiler.set_pie_program_model()
    # PIE Init
    s5 = textwrap.dedent(
        """\
        @staticmethod
        def Init(frag, context):
            pass"""
    )
    e5 = textwrap.dedent(
        """\
        cdef public void Init(Fragment &frag, Context[double, double] &context):
            pass"""
    )
    r5 = compiler.compile(s5)
    assert r5 == e5

    # Pregel Compute
    s6 = textwrap.dedent(
        """\
        @staticmethod
        def PEval(frag, context):
            pass"""
    )
    e6 = textwrap.dedent(
        """\
        cdef public void PEval(Fragment &frag, Context[double, double] &context):
            pass"""
    )
    r6 = compiler.compile(s6)
    assert r6 == e6

    # Pregel Combine
    s7 = textwrap.dedent(
        """\
        @staticmethod
        def IncEval(frag, context):
            pass"""
    )
    e7 = textwrap.dedent(
        """\
        cdef public void IncEval(Fragment &frag, Context[double, double] &context):
            pass"""
    )
    r7 = compiler.compile(s7)
    assert r7 == e7
    # await is unsupported yet, since we can't import asyncio
