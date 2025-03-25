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

import os
import shutil
import subprocess
from typing import Dict, Any, List

import sphinx
import sphinx.application
from sphinx import addnodes
from sphinx.environment.adapters.toctree import TocTree

from furo.navigation import get_navigation_tree

# set maximum_recent_versions to 1 to avoid creating too many files, which may cause failure when uploading to cloudflare
def resolve_git_tags(maximum_recent_versions=1): 
    git = shutil.which("git")
    if git is None:
        return "latest", []
    try:
        head = subprocess.check_output(
            [git, "describe", "--exact-match", "--tags", "HEAD"],
            stderr=subprocess.DEVNULL,
        )
        head = head.decode("utf-8", errors="ignore").strip()
    except:
        head = "latest"
    releases = (
        subprocess.check_output([git, "tag", "--list", "--sort=-creatordate"])
        .decode("utf-8", errors="ignore")
        .strip()
        .split()
    )
    releases = [r for r in releases if r.startswith("v")][0:maximum_recent_versions]
    return head, releases


current_version, releases = resolve_git_tags()


def concat_path(page, base):
    if "/" in page:
        return page[: (page.rfind("/") + 1)] + base
    return base


class TocTreeExt(TocTree):
    def get_local_toctree_for(self, master_doc, docname, builder, collapse, **kwargs):
        """Return the global TOC nodetree.

        Like `get_local_toctree` in `TocTree`, but don't generate
        toctree for master_doc, rather, generate toctree for the
        given doc.
        """
        doctree = self.env.get_doctree(master_doc)
        toctrees = []  # type: List[Element]
        if "includehidden" not in kwargs:
            kwargs["includehidden"] = True
        if "maxdepth" not in kwargs:
            kwargs["maxdepth"] = 0
        kwargs["collapse"] = collapse
        for toctreenode in doctree.traverse(addnodes.toctree):
            toctree = self.resolve(docname, builder, toctreenode, prune=True, **kwargs)
            if toctree:
                toctrees.append(toctree)
        if not toctrees:
            return None
        result = toctrees[0]
        for toctree in toctrees[1:]:
            result.extend(toctree.children)
        return result


def _compute_navigation_tree(
    app: sphinx.application.Sphinx, pagename: str, context: Dict[str, Any]
) -> str:
    toctree = TocTreeExt(app.builder.env)
    if "zh" in pagename:
        master_doc = "zh/index"
    else:
        master_doc = "index"
    toctree_html = app.builder.render_partial(
        toctree.get_local_toctree_for(
            master_doc,
            docname=pagename,
            builder=app.builder,
            collapse=False,
            titles_only=True,
            maxdepth=-1,
            includehidden=False,
        )
    )["fragment"]
    return get_navigation_tree(toctree_html)


def _html_page_context(
    app: sphinx.application.Sphinx,
    pagename: str,
    templatename: str,
    context: Dict[str, Any],
    doctree: Any,
):
    version = current_version
    if os.environ.get("TAG_VER", None) == "stable":
        version = "stable"
    context["version"] = version
    context["versions"] = ["latest", "stable"] + releases
    context["concat_path"] = concat_path

    # customize toctree for en/zh_CN docs
    context["furo_navigation_tree"] = _compute_navigation_tree(app, pagename, context)


def setup(app: sphinx.application.Sphinx) -> Dict[str, Any]:
    app.connect("html-page-context", _html_page_context, priority=1000)
    return {
        "version": sphinx.__display_version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
