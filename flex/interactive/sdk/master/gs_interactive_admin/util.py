import datetime

import typing
from gs_interactive_admin import typing_utils


def _deserialize(data, klass):
    """Deserializes dict, list, str into an object.

    :param data: dict, list or str.
    :param klass: class literal, or string of class name.

    :return: object.
    """
    if data is None:
        return None

    if klass in (int, float, str, bool, bytearray):
        return _deserialize_primitive(data, klass)
    elif klass == object:
        return _deserialize_object(data)
    elif klass == datetime.date:
        return deserialize_date(data)
    elif klass == datetime.datetime:
        return deserialize_datetime(data)
    elif typing_utils.is_generic(klass):
        if typing_utils.is_list(klass):
            return _deserialize_list(data, klass.__args__[0])
        if typing_utils.is_dict(klass):
            return _deserialize_dict(data, klass.__args__[1])
    else:
        return deserialize_model(data, klass)


def _deserialize_primitive(data, klass):
    """Deserializes to primitive type.

    :param data: data to deserialize.
    :param klass: class literal.

    :return: int, long, float, str, bool.
    :rtype: int | long | float | str | bool
    """
    try:
        value = klass(data)
    except UnicodeEncodeError:
        value = data
    except TypeError:
        value = data
    return value


def _deserialize_object(value):
    """Return an original value.

    :return: object.
    """
    return value


def deserialize_date(string):
    """Deserializes string to date.

    :param string: str.
    :type string: str
    :return: date.
    :rtype: date
    """
    if string is None:
      return None
    
    try:
        from dateutil.parser import parse
        return parse(string).date()
    except ImportError:
        return string


def deserialize_datetime(string):
    """Deserializes string to datetime.

    The string should be in iso8601 datetime format.

    :param string: str.
    :type string: str
    :return: datetime.
    :rtype: datetime
    """
    if string is None:
      return None
    
    try:
        from dateutil.parser import parse
        return parse(string)
    except ImportError:
        return string


def deserialize_model(data, klass):
    """Deserializes list or dict to model.

    :param data: dict, list.
    :type data: dict | list
    :param klass: class literal.
    :return: model object.
    """
    instance = klass()

    if not instance.openapi_types:
        return data

    for attr, attr_type in instance.openapi_types.items():
        if data is not None \
                and instance.attribute_map[attr] in data \
                and isinstance(data, (list, dict)):
            value = data[instance.attribute_map[attr]]
            setattr(instance, attr, _deserialize(value, attr_type))

    return instance


def _deserialize_list(data, boxed_type):
    """Deserializes a list and its elements.

    :param data: list to deserialize.
    :type data: list
    :param boxed_type: class literal.

    :return: deserialized list.
    :rtype: list
    """
    return [_deserialize(sub_data, boxed_type)
            for sub_data in data]


def _deserialize_dict(data, boxed_type):
    """Deserializes a dict and its elements.

    :param data: dict to deserialize.
    :type data: dict
    :param boxed_type: class literal.

    :return: deserialized dict.
    :rtype: dict
    """
    return {k: _deserialize(v, boxed_type)
            for k, v in data.items() }


META_SERVICE_KEY = 'service'
INSTANCE_LIST_KEY = 'instance_list'
META_PRIMARY_KEY = 'primary'
METADATA_KEY = 'metadata'
GRAPH_META_KEY = 'graph_meta'
JOB_META_KEY = 'job_meta'
PLUGIN_META_KEY = 'plugin_meta'
STATUS_META_KEY = 'status'


class MetaKeyHelper(object):
    def __init__(self, namespace="interactive", instance_name="default"):
        self.namespace = namespace
        self.instance_name = instance_name
        self._service_root = "/" + "/".join([namespace, instance_name, META_SERVICE_KEY])
        self._meta_root ="/" + "/".join([namespace, instance_name, METADATA_KEY])
        
    def graph_meta_prefix(self):
        return "/".join([self._meta_root, GRAPH_META_KEY])
    
    def plugin_meta_prefix(self):
        return "/".join([self._meta_root, PLUGIN_META_KEY])
    
    def job_meta_prefix(self):
        return "/".join([self._meta_root, JOB_META_KEY])
    
    def service_prefix(self):
        return self._service_root
    
    def service_instance_list_prefix(self, graph_id, service_name):
        return "/".join([self._service_root, graph_id, service_name, INSTANCE_LIST_KEY])
    
    def service_primary_key(self, graph_id, service_name):
        return "/".join([self._service_root, graph_id, service_name, META_PRIMARY_KEY])
    
    def graph_status_key(self, graph_id):
        return "/".join([self._meta_root, STATUS_META_KEY, graph_id])
    
    def decode_service_key(self, key):
        """
        Decode a key into instance_list_key or primary_key. 
        /namespace/instance_name/service/graph_id/service_name/instance_list/ip:port
        /namespace/instance_name/service/graph_id/service_name/primary
        return graph_id, service_name, endpoint, <ip:port>
        """
        keys = key.split("/")
        if len(keys) < 7:
            raise ValueError("Invalid key: %s" % key)
        graph_id, service_name, key_type = keys[4:7]
        if key_type == INSTANCE_LIST_KEY:
            return graph_id, service_name, keys[-1]
        elif key_type == META_PRIMARY_KEY:
            return graph_id, service_name, None
        else:
            raise ValueError("Invalid key type: %s" % key_type)
       
        
    
    