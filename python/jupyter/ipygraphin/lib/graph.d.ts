import '../css/index.css';
import { ISerializers, WidgetModel, DOMWidgetView } from '@jupyter-widgets/base';
export declare class GraphModel extends WidgetModel {
    defaults(): {
        _model_name: string;
        _model_module: any;
        _model_module_version: any;
        nodes: never[];
        edges: never[];
        value: string;
        _view_module: string;
        _view_name: string;
        _view_module_version: string;
        _view_count: number;
    };
    static serializers: ISerializers;
    static model_module: any;
    static model_module_version: any;
}
export declare class GraphView extends DOMWidgetView {
    private dom;
    constructor(params: any);
    valueChanged(): void;
    render(): void;
    value_changed(): void;
}
