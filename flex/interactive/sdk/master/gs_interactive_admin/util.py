import datetime

import typing
import subprocess
import threading
import os
import oss2
from gs_interactive_admin import typing_utils
import logging
import yaml

from gs_interactive_admin.core.config import OSS_BUCKET_NAME, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT

logger = logging.getLogger("interactive")

def dump_file(data, file_path):
    if isinstance(data, str):
        with open(file_path, "w") as f:
            f.write(data)
        return file_path
    elif isinstance(data, dict):
        # dump as yaml
        with open(file_path, "w") as f:
            yaml.dump(data, f)
        return file_path
    else:
        raise ValueError(f"Invalid data type: {type(data)}")

class SubProcessRunner(object):
    def __init__(self, graph_id, command, callback, log_file):
        self._graph_id = graph_id
        self.command = command
        self.callback = callback
        self.log_file = log_file
        self.process_id = None
        self.thread = None

    @property
    def graph_id(self):
        return self._graph_id

    def start(self):
        def target():
            with open(self.log_file, "w") as log:
                process = subprocess.Popen(self.command, stdout=log, stderr=log)
                self.process_id = process.pid

            process.wait()
            logger.info(
                f"Job process {self.process_id} finished with code {process.returncode}, calling callback"
            )
            self.callback(process)

        self.thread = threading.Thread(target=target)
        self.thread.start()
        return self.thread, self.process_id

    def is_alive(self):
        return self.thread.is_alive()


def remove_nones(data: dict):
    """
    Recursively remove None values from a dictionary.
    """
    if isinstance(data, dict):
        return {
            key: remove_nones(value) for key, value in data.items() if value is not None
        }
    elif isinstance(data, list):
        return [remove_nones(item) for item in data]
    else:
        return data


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
        if (
            data is not None
            and instance.attribute_map[attr] in data
            and isinstance(data, (list, dict))
        ):
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
    return [_deserialize(sub_data, boxed_type) for sub_data in data]


def _deserialize_dict(data, boxed_type):
    """Deserializes a dict and its elements.

    :param data: dict to deserialize.
    :type data: dict
    :param boxed_type: class literal.

    :return: deserialized dict.
    :rtype: dict
    """
    return {k: _deserialize(v, boxed_type) for k, v in data.items()}


META_SERVICE_KEY = "service"
INSTANCE_LIST_KEY = "instance_list"
META_PRIMARY_KEY = "primary"
METADATA_KEY = "metadata"
GRAPH_META_KEY = "graph_meta"
JOB_META_KEY = "job_meta"
PLUGIN_META_KEY = "plugin_meta"
STATUS_META_KEY = "status"
STATISTICS_KEY = "statistics"


class MetaKeyHelper(object):
    def __init__(self, namespace="interactive", instance_name="default"):
        self.namespace = namespace
        self.instance_name = instance_name
        self._root = "/" + "/".join([namespace, instance_name])
        self._service_root = "/" + "/".join(
            [namespace, instance_name, META_SERVICE_KEY]
        )
        self._meta_root = "/" + "/".join([namespace, instance_name, METADATA_KEY])
        
    @property 
    def root(self):
        return self._root

    def graph_meta_prefix(self):
        return "/".join([self._meta_root, GRAPH_META_KEY])

    def plugin_meta_prefix(self, graph_id):
        """Plugin is unique for graph scope, not global.

        Returns:
            _type_: _description_
        """
        return "/".join([self._meta_root, graph_id + "_" + PLUGIN_META_KEY])

    def job_meta_prefix(self):
        return "/".join([self._meta_root, JOB_META_KEY])

    def service_prefix(self):
        return self._service_root

    def service_instance_list_prefix(self, graph_id, service_name):
        return "/".join([self._service_root, graph_id, INSTANCE_LIST_KEY, service_name])

    def service_primary_key(self, graph_id, service_name):
        return "/".join([self._service_root, graph_id, META_PRIMARY_KEY, service_name])

    def graph_status_key(self, graph_id):
        return "/".join([self._meta_root, STATUS_META_KEY, graph_id])

    def graph_statistics_key(self, graph_id):
        return "/".join([self._meta_root, STATISTICS_KEY, graph_id])

    def decode_service_key(self, key):
        """
        Decode a key into instance_list_key or primary_key.
        /namespace/instance_name/service/graph_id/instance_list/service_name/ip:port
        /namespace/instance_name/service/graph_id/primary
        return graph_id, service_name, endpoint, <ip:port>
        """
        keys = key.split("/")
        keys = list(filter(None, keys))
        if len(keys) > 7 or len(keys) < 5:
            raise ValueError(f"Invalid key: {keys}")
        logger.info(f"keys {keys}")
        key_type = keys[4]
        if key_type == INSTANCE_LIST_KEY:
            # graph_id, service_name, endpoint
            return keys[3], keys[5], keys[6]
        elif key_type == META_PRIMARY_KEY:
            if len(keys) == 5:
                return keys[3], None, None
            else:
                logger.warning(f"Got invalid key: {keys}")
                return None
        else:
            raise ValueError(f"Invalid key type: {key_type}, {keys}")


def get_current_time_stamp_ms():
    return int(datetime.datetime.now().timestamp() * 1000)

def check_field_in_dict(data, field):
    if field not in data:
        raise ValueError(f"Field {field} not found in {data}")

class OssReader(object):
    def __init__(self):
        self._auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
        self._bucket = oss2.Bucket(self._auth, OSS_ENDPOINT, OSS_BUCKET_NAME)

    def read(self, key):
        """
        Read a file under the bucket.
        """
        return self._bucket.get_object(key).read().decode("utf-8")