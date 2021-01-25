import '../css/index.css';
import {
  ISerializers,
  WidgetModel,
  DOMWidgetView
} from '@jupyter-widgets/base';
import React from 'react';
import ReactDOM from 'react-dom';
import { Utils } from '@antv/graphin';
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
      value: 'x',
      graphData: {},
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

    this.graphData = Utils.mock(8)
      .circle()
      .graphin();
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
    const graphData = this.model.get('graphData');

    console.log('change value', value, graphData);
    // const textDom = document.createElement('span');
    // console.log(textDom);
    // textDom.innerText = value + 'xxx';

    // this.el.append(textDom);
  }

  render() {
    this.el.classList.add('custom-widget');

    //Python attributes that must be sync. with frontend
    // this.model.on('change:data', this._updateMinZoom, this);
    this.model.on('change:value', this.valueChanged, this);
    // this.model.on('change:graphData', this.valueChanged, this);
    this.model.on('change:enabledZoom', this.value_changed, this);

    if (this.dom) {
      ReactDOM.unmountComponentAtNode(this.dom);
    }

    this.value_changed();

    // this.el.addEventListener('click', this.onClick.bind(this));
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

  value_changed() {
    // dom 不存在时，创建 dom 元素并添加到 this.el 中
    if (!this.dom) {
      this.dom = document.createElement('div');
      this.dom.style.width = '1000px';
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
            width: 1000,
            height: 400,
            neighbors: this.queryNeighbors.bind(this),
            data: this.graphData,
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
