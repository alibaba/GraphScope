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

import copy
import json


class ____:
    def __init__(self):
        self.ops = []
        self.renew = True

    def renew_if_needed(self):
        if self.renew:
            target = copy.deepcopy(self)
        else:
            target = self
        target.renew = False
        return target

    def check_if_continable(self):
        if self.ops and self.ops[-1]["op"] == "repeat":
            raise ValueError(".repeat() is the final operator in a sampler")

    def __repr__(self) -> str:
        return json.dumps(self.ops)

    def __str__(self) -> str:
        return self.__repr__()

    def V(self, label):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "V", "label": label})
        return target

    def E(self, label):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "E", "label": label})
        return target

    def by(self, strategy):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "by", "strategy": strategy})
        return target

    def sample(self, k):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "sample", "k": k})
        return target

    def batch(self, batch_size):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "batch", "batch_size": batch_size})
        return target

    def outV(self, label):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "outV", "label": label})
        return target

    def inV(self, label):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "inV", "label": label})
        return target

    def values(self):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "values"})
        return target

    def repeat(self, repeats=128):
        self.check_if_continable()
        target = self.renew_if_needed()
        target.ops.append({"op": "repeat", "repeats": repeats})
        return target


___ = ____()


class QueryInterpreter:
    def __init__(self):
        pass

    def _retrieve_values(self, target):
        if isinstance(target, list):
            return [nodes.float_attrs for nodes in target], target[0].labels
        else:
            return target.float_attrs, target.labels

    def run(self, g, query):
        if isinstance(query, str):
            query = json.loads(query)
        r = g
        repeats = 128
        for q in query:
            if q["op"] == "V":
                r = r.V(q["label"])
            elif q["op"] == "E":
                r = r.V(q["label"])
            elif q["op"] == "by":
                r = r.by(q["strategy"])
            elif q["op"] == "sample":
                r = r.sample(q["k"])
            elif q["op"] == "batch":
                r = r.batch(q["batch_size"])
            elif q["op"] == "outV":
                r = r.outV(q["label"])
            elif q["op"] == "inV":
                r = r.inV(q["label"])
            elif q["op"] == "values":
                r = r.values(self._retrieve_values)
            elif q["op"] == "repeats":
                repeats = q["repeats"]
        dataset, round = [], 0
        while round < repeats:
            try:
                dataset.append(r.next())
                round += 1
            except Exception:
                break
        return dataset
