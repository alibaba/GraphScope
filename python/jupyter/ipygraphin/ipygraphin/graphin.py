import ipywidgets as widgets
from traitlets import Unicode, TraitType
from spectate import mvc
from ._frontend import module_name, module_version

class Mutable(TraitType):
    """A base class for mutable traits using Spectate"""

    _model_type = None
    _event_type = "change"

    def instance_init(self, obj):
        default = self._model_type()

        @mvc.view(default)
        def callback(default, events):
            change = dict(
                new=getattr(obj, self.name),
                name=self.name,
                type=self._event_type,
            )
            obj.notify_change(change)

        setattr(obj, self.name, default)


class MutableDict(Mutable):
    """A mutable dictionary trait"""

    _model_type = mvc.Dict


@widgets.register
class GraphModel(widgets.DOMWidget):
  """ Graph Widget """

  # Name of the widget model class in front-end
  _model_name = Unicode("GraphModel").tag(sync=True)

  # Name of the front-end module containing widget model
  _model_module = Unicode(module_name).tag(sync=True)

  # Version of the front-end module containing widget model
  _model_module_version = Unicode(module_version).tag(sync=True)

  # Name of the widget view class in front-end
  _view_name = Unicode("GraphView").tag(sync=True)

  # Name of the front-end module containing widget view
  _view_module = Unicode(module_name).tag(sync=True)

  # Version of the front-end module containing widget view
  _view_module_version = Unicode(module_version).tag(sync=True)

  # Widget specific property.
  # Widget properties are defined as traitlets. Any property tagged with `sync=True`
  # is automatically synced to the frontend *any* time it changes in Python.
  # It is synced back to Python from the frontend *any* time the model is touched.
  # data: { nodes: [], edges: [] }
  data = MutableDict().tag(sync=True)

  # 测试使用的文本值
  value = Unicode('x').tag(sync=True)

  # 查询图数据
  def queryGraphData(self, vertices, hop):
    # 调用 python 接口，获取查询的图数据，格式为 JSON，同时修改 data 值
    self.value = 'result data'
    print('vertices', vertices)
    print('hop', hop)
