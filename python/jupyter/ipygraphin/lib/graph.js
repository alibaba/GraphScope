import '../css/index.css';
import { WidgetModel, DOMWidgetView } from '@jupyter-widgets/base';
import React from 'react';
import ReactDOM from 'react-dom';
import Graphin, { Utils } from '@antv/graphin';
// eslint-disable-next-line @typescript-eslint/no-var-requires
const widgets = require('@jupyter-widgets/base');
import { MODULE_NAME, MODULE_VERSION } from './version';
export class GraphModel extends WidgetModel {
    defaults() {
        return Object.assign(Object.assign({}, super.defaults()), { _model_name: 'GraphModel', _model_module: GraphModel.model_module, _model_module_version: GraphModel.model_module_version, nodes: [], edges: [], value: 'x' });
    }
}
GraphModel.serializers = Object.assign({ nodes: { deserialize: widgets.unpack_models }, edges: { deserialize: widgets.unpack_models } }, WidgetModel.serializers);
GraphModel.model_module = MODULE_NAME;
GraphModel.model_module_version = MODULE_VERSION;
export class GraphView extends DOMWidgetView {
    constructor(params) {
        super({
            model: params.model,
            options: params.options
        });
        this.model.on('change:value', this.valueChanged, this);
    }
    valueChanged() {
        const value = this.model.get('value');
        console.log('change value', value);
        const textDom = document.createElement('span');
        console.log(textDom);
        // textDom.text = value + 'xxx';
        // this.el.append(textDom);
    }
    render() {
        this.el.classList.add('custom-widget');
        //Python attributes that must be sync. with frontend
        // this.model.on('change:data', this._updateMinZoom, this);
        if (this.dom) {
            ReactDOM.unmountComponentAtNode(this.dom);
        }
        this.value_changed();
    }
    value_changed() {
        this.dom = document.createElement('div');
        this.dom.style.width = '1000px';
        this.dom.style.height = '400px';
        this.el.append(this.dom);
        setTimeout(() => {
            ReactDOM.render(React.createElement(Graphin, {
                graphDOM: this.dom,
                width: 1000,
                height: 400,
                data: Utils.mock(6)
                    .circle()
                    .graphin()
            }, null), this.dom);
        }, 16);
    }
}
//# sourceMappingURL=graph.js.map