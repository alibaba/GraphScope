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

"""
grape.nx.compat is a set of tools to create a compatible layer with networkx,
by reusing part of networkx code, both sources and tests, without copy-and-paste,
to make the graph algorithms runs on grape-engine.
"""

import copy
import functools
import inspect
import warnings
from enum import Enum
from types import FunctionType
from types import LambdaType
from types import ModuleType

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import imp

__all__ = [
    "internal_name",
    "replace_module_context",
    "import_as_graphscope_nx",
    "patch_docstring",
    "with_graphscope_nx_context",
]


def internal_name(name):
    return name and name.startswith("__") and name.endswith("__")


def copy_function(func, global_ctx=None):
    """
    Copy the function, with given base global_ctx, and register the result function into the
    given base ctx.

    References
    ----------
    1. https://stackoverflow.com/a/13503277/5080177
    """
    if global_ctx is None:
        global_ctx = func.__globals__
    fn = FunctionType(
        func.__code__,
        global_ctx,
        name=func.__name__,
        argdefs=func.__defaults__,
        closure=func.__closure__,
    )
    # functools.update_wrapper would update all the metadata other then __kwdefaults__
    fn.__kwdefaults__ = copy.copy(func.__kwdefaults__)
    return functools.update_wrapper(fn, func)


def copy_property(prop, global_ctx=None):
    """
    Copy the property, with given base global_ctx.

    References
    ----------
    1. https://stackoverflow.com/q/28613518
    """
    fget = (
        copy_function(prop.fget, global_ctx)
        if isinstance(prop.fget, FunctionType)
        else prop.fget
    )
    fset = (
        copy_function(prop.fset, global_ctx)
        if isinstance(prop.fset, FunctionType)
        else prop.fset
    )
    fdel = (
        copy_function(prop.fdel, global_ctx)
        if isinstance(prop.fdel, FunctionType)
        else prop.fdel
    )
    return property(fget, fset, fdel)


def copy_class(cls):
    if issubclass(cls, Enum):
        return cls

    class T(*cls.__bases__):
        pass

    setattr(T, "__name__", cls.__name__)
    setattr(T, "__doc__", getattr(cls, "__doc__"))
    if hasattr(cls, "__annotations__"):
        setattr(T, "__annotations__", getattr(cls, "__annotations__"))
    return with_graphscope_nx_context(cls, patch_docstring=True)(T)


def replace_context(global_ctx, source_module, target_module):
    """
    Replacing :code:`source_module` with :code:`target_module` in the given :code:`global_ctx`.
    """
    for k, v in global_ctx.items():
        if isinstance(v, ModuleType):
            if v.__name__ == source_module.__name__:
                global_ctx[k] = target_module
    return global_ctx


def load_the_module(module_or_name):
    if isinstance(module_or_name, ModuleType):
        module_or_name = module_or_name.__name__
    names = module_or_name.split(".")
    module_path = imp.find_module(names[0])
    try:
        for name in names[1:]:
            module_path = imp.find_module(name, [module_path[1]])
    except Exception:
        return module_or_name
    module = imp.load_module(module_or_name, *module_path)
    return apply_networkx_patches(module)


def apply_networkx_patches(module):
    # there'a some name conflicts in networkx and we need to be careful
    # e.g.,
    #
    #     networkx.algorithms.approximation.connectivity
    # vs. networkx.algorithms.connectivity
    #
    if module.__name__ == "networkx.algorithms":
        mod_connectivity = load_the_module("networkx.algorithms.connectivity")
        setattr(module, "connectivity", mod_connectivity)
    return module


def replace_module_context(  # noqa: C901
    module_or_value,
    source_module,
    target_module,
    expand=True,
    decorators=None,
):
    """
    Patching all calls of `nx.xxx` inside the given method with graphscope.nx.xxx, and register
    the **copied** method in the caller's module.

    Note that this patch method won't affect the original module, i.e., the :code:`nx` module.

    This patch method can be used like (assuming we are inside module grape.nx.modx):

    ..codeblock: python

        from network import relabel

        import_as_graphscope_nx(relabel)

    such statements will import **ALL** variables in module :code:`relabel`, both public
    ones (exposed in :code:`__all__`) and private ones, to current module (i.e.,
    :code:`graphscope.nx.modx`).

    This patch method can also been used on per method, for fine-grained control,

        with_graphscope_nx_context(relabel.relabel_nodes)

    such statements will import method :code:`relabel_nodes` to current module (i.e.,
    :code:`grape.nx.modx`). Note that patching a standalone function doesn't affect the
    methods called by the patched function.
    """

    if decorators is None:
        decorators = []
    if not isinstance(decorators, (list, tuple)):
        decorators = [decorators]

    # get the caller's module, and since this function might be called by import_as_grape_nx
    # in this module, we go up and findout the real caller.
    for loc in inspect.stack()[1:]:
        mod = inspect.getmodule(loc[0])
        if mod.__name__ != __name__:
            break

    if isinstance(module_or_value, ModuleType):
        # reload a copy of the module, to rewrite
        module = load_the_module(module_or_value)

        members_skip_patch = set(
            {
                "__all__",
                "__builtins__",
                "__cached__",
                "__doc__",
                "__file__",
                "__loader__",
                "__name__",
                "__package__",
                "__spec__",
            }
        )
        # find a common __globals__, since they are in the same module
        global_ctx = dict(inspect.getmembers(module))

        # nb: __all__ is not enough, since private variable exists
        members_to_patch = set(dir(module)).difference(members_skip_patch)
        for var_name in members_to_patch:
            var = getattr(module, var_name)
            if getattr(var, "__module__", None) != module.__name__:
                continue
            if hasattr(var, "__globals__"):
                for k, v in getattr(var, "__globals__").items():
                    if k not in global_ctx:
                        global_ctx[k] = v
        global_ctx = replace_context(
            copy.copy(global_ctx), source_module, target_module
        )

        # patch every member
        for var_name in members_to_patch:
            var = getattr(module, var_name)

            if getattr(var, "__module__", None) != module.__name__:
                if (
                    isinstance(var, ModuleType)
                    and var.__name__ == source_module.__name__
                ):
                    target_value = target_module
                else:
                    target_value = var
                setattr(module, var_name, target_value)
                if expand:
                    setattr(mod, var_name, target_value)
                continue

            if isinstance(var, ModuleType):
                if var.__name__ == source_module.__name__:
                    target_value = target_module
                else:
                    target_value = var
                setattr(module, var_name, target_value)
                if expand:
                    setattr(mod, var_name, getattr(module, var_name))
                continue

            if isinstance(var, (FunctionType, LambdaType)):
                fn = copy_function(var, global_ctx)
                for dec in decorators:
                    fn = dec(fn)
                global_ctx[var_name] = fn
                setattr(module, var_name, fn)
                if expand:
                    setattr(mod, var_name, fn)
                continue

            if inspect.isclass(var):
                # special case for `argmap`: `copy_class` cannot handle complex
                # arguments properly.
                #
                # see also: https://github.com/alibaba/GraphScope/pull/507
                if var.__name__ == "argmap":
                    global_ctx[var_name] = var
                    setattr(module, var_name, var)
                    if expand:
                        setattr(mod, var_name, var)
                    continue

                target_class = copy_class(var)
                for dec in decorators:
                    target_class = dec(target_class)
                global_ctx[var_name] = target_class
                setattr(module, var_name, target_class)
                if expand:
                    setattr(mod, var_name, target_class)
                continue

            # otherwise, try deepcopy
            if expand:
                setattr(mod, var_name, copy.deepcopy(var))

        # extend "__all__"
        if expand:
            if not hasattr(mod, "__all__"):
                setattr(mod, "__all__", [])
            if hasattr(module, "__all__"):
                getattr(mod, "__all__").extend(getattr(module, "__all__"))
            else:
                getattr(mod, "__all__").extend(
                    name for name in dir(module) if not internal_name(name)
                )

        # attach the module
        setattr(mod, module.__name__, module)
        return module

    if inspect.isclass(module_or_value):
        target_class = copy_class(module_or_value)
        for dec in decorators:
            target_class = dec(target_class)
        setattr(mod, module_or_value.__name__, target_class)
        getattr(mod, "__all__").append(module_or_value.__name)
        return target_class

    if isinstance(module_or_value, (FunctionType, LambdaType)):
        # nb: shallow copy the __globals__ should be enough, and could avoid potential recursive loop.
        global_ctx = replace_context(
            copy.copy(module_or_value.__globals__), source_module, target_module
        )
        func = copy_function(module_or_value, global_ctx)
        for dec in decorators:
            func = dec(func)
        setattr(mod, module_or_value.__name__, func)
        getattr(mod, "__all__").append(module_or_value.__name__)
        return func

    if isinstance(module_or_value, (int, float, str, list, tuple)):
        setattr(mod, module_or_value.__name__, copy.deepcopy(module_or_value))
        return module_or_value

    raise RuntimeError(
        "Unknown value to patch context: %r (or type %s)"
        % (module_or_value, type(module_or_value))
    )


def import_as_graphscope_nx(module_or_value, expand=True, decorators=None):
    """
    With context :mod:`networkx` with :mod:`graphscope.nx` inside the given module
    or value (normally function).

    Parameters
    ----------
    module_or_value:
        module or value that will be imported
    expand: bool
        If expand is `True`, indicates `from module_or_value import *`, otherwise `import module_or_value`.

    See Also
    --------
    replace_module_context: a general context patch method.
    """
    import networkx

    from graphscope import nx as gs_nx

    return replace_module_context(
        module_or_value, networkx, gs_nx, expand=expand, decorators=decorators
    )


def with_module_map(  # noqa: C901
    source_method_or_class,
    source_module,
    target_module,
    patch_docstring=True,
):
    def patch_method_wrapper(target_method):
        if not isinstance(target_method, (FunctionType, LambdaType, property)):
            raise RuntimeError("The method cannot be transformed on class")
        source_global_ctx = replace_context(
            copy.copy(source_method_or_class.__globals__), source_module, target_module
        )
        global_ctx = copy.copy(target_method.__globals__)
        for k, v in source_global_ctx.items():
            if k not in global_ctx:
                global_ctx[k] = v
        fn = copy_function(source_method_or_class, global_ctx)
        if patch_docstring:
            setattr(fn, "__doc__", getattr(source_method_or_class, "__doc__"))
        return fn

    def patch_class_wrapper(target_class):
        if not inspect.isclass(target_class):
            raise RuntimeError("The class cannot be transformed on method")
        existing_methods = getattr(target_class, "__dict__")
        source_methods = getattr(source_method_or_class, "__dict__")

        global_ctx = replace_context(
            dict(inspect.getmembers(inspect.getmodule(target_class))),
            source_module,
            target_module,
        )
        for name, meth in source_methods.items():
            if hasattr(meth, "__globals__"):
                for k, v in getattr(meth, "__globals__").items():
                    if k not in global_ctx:
                        global_ctx[k] = v

        for name, meth in source_methods.items():
            if name in existing_methods or isinstance(meth, ModuleType):
                continue

            if isinstance(meth, classmethod):
                meth = meth.__func__
                is_class_method = True
            else:
                is_class_method = False

            if isinstance(meth, staticmethod):
                meth = meth.__func__
                is_static_method = True
            else:
                is_static_method = False

            if isinstance(meth, (FunctionType, LambdaType, property)):
                if hasattr(meth, "__globals__"):
                    for k, v in getattr(meth, "__globals__").items():
                        if k not in global_ctx:
                            global_ctx[k] = v

                # run replacing module context again for accurance
                global_ctx = replace_context(global_ctx, source_module, target_module)
                fn = (
                    copy_property(meth, global_ctx)
                    if isinstance(meth, property)
                    else copy_function(meth, global_ctx)
                )
                if patch_docstring:
                    setattr(fn, "__doc__", getattr(meth, "__doc__"))

                if is_static_method:
                    fn = staticmethod(fn)

                if is_class_method:
                    fn = classmethod(fn)
                setattr(target_class, name, fn)
            else:
                # shallow copy is enough
                setattr(target_class, name, copy.copy(meth))

        if patch_docstring:
            setattr(target_class, "__doc__", getattr(source_method_or_class, "__doc__"))
        return target_class

    if inspect.isclass(source_method_or_class):
        return patch_class_wrapper
    if isinstance(source_method_or_class, (FunctionType, LambdaType, property)):
        return patch_method_wrapper

    raise NotImplementedError(
        "Invalid python value to patch, only class and function/method is supported"
    )


def with_graphscope_nx_context(source_method_or_class, patch_docstring=True):
    import networkx

    from graphscope import nx as gs_nx

    return with_module_map(
        source_method_or_class, networkx, gs_nx, patch_docstring=patch_docstring
    )


def patch_docstring(source_method):
    def patch_wrapper(target_method):
        setattr(target_method, "__doc__", getattr(source_method, "__doc__"))
        return target_method

    return patch_wrapper
