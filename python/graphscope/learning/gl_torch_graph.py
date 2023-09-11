class GLTorchGraph(object):
    def __init__(self, server_list):
        assert len(server_list) == 4
        self._master_addr, self._server_client_master_port = server_list[0].split(":")
        self._train_master_addr, self._train_loader_master_port = server_list[1].split(
            ":"
        )
        self._val_master_addr, self._val_loader_master_port = server_list[2].split(":")
        self._test_master_addr, self._test_loader_master_port = server_list[3].split(
            ":"
        )
        assert (
            self._master_addr
            == self._train_master_addr
            == self._val_master_addr
            == self._test_master_addr
        )

    @property
    def master_addr(self):
        return self._master_addr

    @property
    def server_client_master_port(self):
        return self._server_client_master_port

    @property
    def train_loader_master_port(self):
        return self._train_loader_master_port

    @property
    def val_loader_master_port(self):
        return self._val_loader_master_port

    @property
    def test_loader_master_port(self):
        return self._test_loader_master_port

    @staticmethod
    def check_params(schema, config):
        def check_edge(edge):
            if not isinstance(edge, tuple) or len(edge) != 3:
                raise ValueError("Each edge should be a tuple of length 3")
            for vertex_label in [edge[0], edge[2]]:
                if vertex_label not in schema.vertex_labels:
                    raise ValueError(f"Invalid edge label: {vertex_label}")
            if edge[1] not in schema.edge_labels:
                raise ValueError(f"Invalid edge label: {edge[1]}")

        def check_edges(edges):
            for edge in edges:
                check_edge(edge)
                if edge in edges[edges.index(edge) + 1 :]:
                    raise ValueError(f"Duplicated edge: {edge}")

        def check_features(feature_names, properties):
            data_type = None
            property_name = ""
            property_dict = {property.name: property for property in properties}
            for feature in feature_names:
                if feature not in property_dict:
                    raise ValueError(f"Feature '{feature}' does not exist")
                property = property_dict[feature]
                if data_type is None:
                    data_type = property.data_type
                    property_name = property.name
                if data_type != property.data_type:
                    raise ValueError(
                        f"Inconsistent DataType: '{data_type}' for {property_name} and '{property.data_type}' for {property.name}"
                    )

        def check_node_features(node_features):
            if node_features is None:
                return
            for label, feature_names in node_features.items():
                if label not in schema.vertex_labels:
                    raise ValueError(f"Invalid vertex label: {label}")
                check_features(feature_names, schema.get_vertex_properties(label))

        def check_edge_features(edge_features):
            if edge_features is None:
                return
            for edge, feature_names in edge_features.items():
                check_edge(edge)
                check_features(feature_names, schema.get_edge_properties(edge[1]))

        def check_node_labels(node_labels):
            if node_labels is None:
                return
            for label, property_name in node_labels.items():
                if label not in schema.vertex_labels:
                    raise ValueError(f"Invalid vertex label: {label}")
                vertex_property_names = [
                    property.name for property in schema.get_vertex_properties(label)
                ]
                if property_name not in vertex_property_names:
                    raise ValueError(
                        f"Invalid property name '{property_name}' for vertex label '{label}'"
                    )

        def check_edge_weights(edge_weights):
            if edge_weights is None:
                return
            for edge, property_name in edge_weights.items():
                check_edge(edge)
                edge_property_names = [
                    property.name for property in schema.get_edge_properties(edge[1])
                ]
                if property_name not in edge_property_names:
                    raise ValueError(
                        f"Invalid property name '{property_name}' for edge '{edge}'"
                    )

        def check_random_node_split(random_node_split):
            if random_node_split is None:
                return
            if not isinstance(random_node_split, dict):
                raise ValueError("Random node split should be a dictionary")
            if "num_val" not in random_node_split:
                raise ValueError("Missing 'num_val' in random node split")
            if "num_test" not in random_node_split:
                raise ValueError("Missing 'num_test' in random node split")
            if len(random_node_split) != 2:
                raise ValueError("Invalid parameters in random node split")

        check_edges(config.get("edges"))
        check_node_features(config.get("node_features"))
        check_edge_features(config.get("edge_features"))
        check_node_labels(config.get("node_labels"))
        check_random_node_split(config.get("random_node_split"))
        check_edge_weights(config.get("edge_weights"))
