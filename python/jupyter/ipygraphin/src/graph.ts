// Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

import '../css/index.css';
import {
  ISerializers,
  WidgetModel,
  DOMWidgetView
} from '@jupyter-widgets/base';
import React from 'react';
import ReactDOM from 'react-dom';
import { GraphScopeComponent } from '@antv/graphin-graphscope';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const widgets = require('@jupyter-widgets/base');

import { MODULE_NAME, MODULE_VERSION } from './version';

export class GraphModel extends WidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: 'GraphModel',
      _model_module: GraphModel.model_module,
      _model_module_version: GraphModel.model_module_version,
      nodes: [],
      edges: [],
      value: '',
      zoom: false
    };
  }

  static serializers: ISerializers = {
    nodes: { deserialize: widgets.unpack_models },
    edges: { deserialize: widgets.unpack_models },
    ...WidgetModel.serializers
  };

  static model_module = MODULE_NAME;
  static model_module_version = MODULE_VERSION;
}

export class GraphView extends DOMWidgetView {
  private dom: HTMLDivElement;
  private graphData: any;

  constructor(params: any) {
    super({
      model: params.model,
      options: params.options
    });

    this.el.addEventListener('contextmenu', this.onClick.bind(this));
  }

  onClick(evt: any) {
    evt.stopPropagation();
    evt.preventDefault();
    this.send({
      type: 'contextmenu',
      params: evt
    });
  }

  valueChanged() {
    const value = this.model.get('value');

    console.log('change value', value);

    if (value) {
      const currentData = JSON.parse(value);
      console.log('json parse value', currentData);
      this.graphData = currentData;
      this.renderGraph(currentData);
    }
  }

  render() {
    //Python attributes that must be sync. with frontend
    this.model.on('change:value', this.valueChanged, this);

    if (this.dom) {
      ReactDOM.unmountComponentAtNode(this.dom);
    } else {
      this.valueChanged();
    }
  }

  queryNeighbors(nodeId: string, degree: number) {
    const model = this.graphData.nodes.filter(
      (node: any) => node.id === nodeId
    );

    console.log('queryNeighbors', nodeId, degree, model);
    this.send({
      nodeId,
      degree
    });
    // setData({
    //   nodes: [...this.graphData.nodes, ...newData.nodes],
    //   edges: [...this.graphData.edges, ...newData.edges]
    // } as any);
  }

  handleNodeClick(model: any, type: string) {
    console.log('click node', model);
  }

  renderGraph(data: any) {
    // dom 不存在时，创建 dom 元素并添加到 this.el 中
    if (!this.dom) {
      this.el.classList.add('custom-widget');
      this.dom = document.createElement('div');
      this.dom.style.width = '100%';
      this.dom.style.height = '400px';
      this.dom.style.position = 'relative';

      this.el.append(this.dom);
    }

    console.log('zoomcanvas', this.model.get('zoom'));

    setTimeout(() => {
      ReactDOM.render(
        React.createElement(
          GraphScopeComponent,
          {
            graphDOM: this.dom,
            // width: 1000,
            height: 400,
            neighbors: this.queryNeighbors.bind(this),
            data: data,
            hasMinimap: true,
            hasContextMenu: true,
            zoomCanvas: this.model.get('zoom')
          },
          null
        ),
        this.el
      );
    }, 16);
  }
}
