var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { IJupyterWidgetRegistry } from '@jupyter-widgets/base';
import { MODULE_NAME, MODULE_VERSION } from './version';
const EXTENSION_ID = 'jupyter-graphin:plugin';
/**
 * The example plugin.
 */
const examplePlugin = {
    id: EXTENSION_ID,
    requires: [IJupyterWidgetRegistry],
    activate: activateWidgetExtension,
    autoStart: true
};
export default examplePlugin;
/**
 * Activate the widget extension.
 */
function activateWidgetExtension(app, registry) {
    registry.registerWidget({
        name: MODULE_NAME,
        version: MODULE_VERSION,
        exports: () => __awaiter(this, void 0, void 0, function* () { return yield import(/* webpackChunkName: "jupyter-graphin" */ './index'); })
    });
}
//# sourceMappingURL=plugin.js.map