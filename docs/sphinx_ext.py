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
import sphinx
from sphinx import addnodes
from sphinx.builders.html import StandaloneHTMLBuilder
from sphinx.environment.adapters.toctree import TocTree
import subprocess


def resolve_git_tags():
    try:
        head = subprocess.check_output(['git', 'describe', '--exact-match', '--tags', 'HEAD'], stderr=subprocess.DEVNULL)
        head = head.decode('utf-8').strip()
    except:
        head = 'latest'
    releases = subprocess.check_output(['git', 'tag', '--list', '--sort=-creatordate']).decode('utf-8').strip().split()
    return head, releases


def concat_path(page, base):
    if '/' in page:
        return page[:(page.rfind('/') + 1)] + base
    else:
        return base


class TocTreeExt(TocTree):
    def get_local_toctree_for(self, master_doc, docname, builder, collapse, **kwargs):
        ''' Like `get_local_toctree` in `TocTree`, but don't generate
            toctree for master_doc, rather, generate toctree for the
            given doc.
        '''
        """Return the global TOC nodetree."""
        doctree = self.env.get_doctree(master_doc)
        toctrees = []  # type: List[Element]
        if 'includehidden' not in kwargs:
            kwargs['includehidden'] = True
        if 'maxdepth' not in kwargs:
            kwargs['maxdepth'] = 0
        kwargs['collapse'] = collapse
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

class StandaloneHTMLBuilderExt(StandaloneHTMLBuilder):
    ''' Extend the standard `StandaloneHTMLBuilder` with a `toctree_for` derivate in
        context to creating toctrees for zh_CN documentations.
    '''
    def _get_local_toctree_ext(self, master_doc, pagename, collapse, **kwargs):
        if 'includehidden' not in kwargs:
            kwargs['includehidden'] = False
        if kwargs.get('maxdepth') == '':
            kwargs.pop('maxdepth')
        return self.render_partial(TocTreeExt(self.env).get_local_toctree_for(
            master_doc, pagename, self, collapse, **kwargs))['fragment']

    def handle_page(self, pagename, *args, **kwargs):
        version, releases = resolve_git_tags()
        if os.environ.get('TAG_VER', None) == 'stable':
            version = 'stable'
        self.globalcontext['version'] = version
        # self.globalcontext['stable'] = releases[0]
        self.globalcontext['versions'] = ['latest', 'stable'] + releases

        self.globalcontext['concat_path'] = concat_path
        self.globalcontext['toctree_for'] = lambda master_doc, **kwargs: self._get_local_toctree_ext(master_doc, pagename, **kwargs)
        return super(StandaloneHTMLBuilderExt, self).handle_page(pagename, *args, **kwargs)

def setup(app):
    app.add_builder(StandaloneHTMLBuilderExt, override=True)
    return {'version': sphinx.__display_version__, 'parallel_read_safe': True}
