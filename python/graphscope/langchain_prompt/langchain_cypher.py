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

from __future__ import annotations

import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from langchain.callbacks.manager import CallbackManagerForChainRun
from langchain.chains.base import Chain
from langchain.chains.graph_qa.prompts import CYPHER_QA_PROMPT
from langchain.chains.llm import LLMChain
from langchain.prompts.prompt import PromptTemplate
from langchain.schema import BasePromptTemplate
from langchain.schema.language_model import BaseLanguageModel

import graphscope

# The patterns to replace in the generated Cypher query
PATTERN_TRANSFER = [("-[*]-", "-[]-")]

Cases = """Right Cases:
querys: 列举出鲁迅的一个别名可以吗？ answer:match (:ENTITY{name:'鲁迅'})<--(h)-[:Relationship{name:'别名'}]->(q) return distinct q.name limit 1
querys: 我们常用的301SH不锈钢带的硬度公差是多少，你知道吗？ answers:match(p:ENTITY{name:'301SH不锈钢带'})-[:Relationship{name:'硬度公差'}]-> (q) return q.name
Wrong Cases:
querys: 12344加油这首歌真好听，你知道歌曲原唱是谁吗？ answers: MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) WHERE m.name = '12345加油' RETURN a.name
querys: 七宗梦是什么时候上映的？ answers: MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) WHERE m.name = '七宗梦' RETURN a.name LIMIT 30"""


INTERMEDIATE_STEPS_KEY = "intermediate_steps"

CYPHER_GENERATION_TEMPLATE = """Task:Generate Cypher statement to query a graph database.
Cases:
{cases}
Schema:
{schema}
Instructions:
Note: Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
Do not include any text except the generated Cypher statement.
You must use the relaship or property shown in the schema!!! do not use other keys!!!
You must use the relaship or property shown in the schema!!! do not use other keys!!!
You must use the relaship or property shown in the schema!!! do not use other keys!!!
你必须使用Sechema中出现的关键词！！！

The question is:
{question}
You must use the relaship or property shown in the schema!!! do not use other keys!!!"""
CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question", "cases"], template=CYPHER_GENERATION_TEMPLATE
)


CHECK_SCHEMA_TEMPLATE = """Task: Check the schema
{query}
Schema:
{schema}
Check the properities and relashions in the query, replace all the keywards that did not shown in the schema!!!
Check the properities and relashions in the query, replace all the keywards that did not shown in the schema!!!
Check the properities and relashions in the query, replace all the keywards that did not shown in the schema!!!
if correct, return the origianl query!!!
Note: Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
Do not include any text except the generated Cypher statement.
Note: Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
Do not include any text except the generated Cypher statement.
"""
CHECK_SCHEMA_PROMPT = PromptTemplate(
    input_variables=["query", "schema"], template=CHECK_SCHEMA_TEMPLATE
)


def extract_cypher(text: str) -> str:
    """Extract Cypher code from a text.

    Args:
        text: Text to extract Cypher code from.

    Returns:
        Cypher code extracted from the text.
    """
    # The pattern to find Cypher code enclosed in triple backticks
    pattern = r"```(.*?)```"

    # Find all matches in the input text
    matches = re.findall(pattern, text, re.DOTALL)

    cypher_query = matches[0] if matches else text

    # Replace any patterns that are not supported by the graph database
    for pattern, replacement in PATTERN_TRANSFER:
        cypher_query = cypher_query.replace(pattern, replacement)
    return cypher_query


class GraphCypherQAChain(Chain):
    """Chain for question-answering against a graph by generating Cypher statements."""

    graph: graphscope.Graph
    cypher_generation_chain: LLMChain
    check_schema_chain: LLMChain
    qa_chain: LLMChain
    input_key: str = "query"  #: :meta private:
    output_key: str = "result"  #: :meta private:
    top_k: int = 10
    """Number of results to return from the query"""
    return_intermediate_steps: bool = False
    """Whether or not to return the intermediate steps along with the final answer."""
    return_direct: bool = False
    """Whether or not to return the result of querying the graph directly."""

    @property
    def input_keys(self) -> List[str]:
        """Return the input keys.

        :meta private:
        """
        return [self.input_key]

    @property
    def output_keys(self) -> List[str]:
        """Return the output keys.

        :meta private:
        """
        _output_keys = [self.output_key]
        return _output_keys

    @property
    def _chain_type(self) -> str:
        return "graph_cypher_chain"

    @classmethod
    def from_llm(
        cls,
        llm: BaseLanguageModel,
        *,
        qa_prompt: BasePromptTemplate = CYPHER_QA_PROMPT,
        cypher_prompt: BasePromptTemplate = CYPHER_GENERATION_PROMPT,
        check_prompt: BasePromptTemplate = CHECK_SCHEMA_PROMPT,
        **kwargs: Any,
    ) -> GraphCypherQAChain:
        """Initialize from LLM."""
        qa_chain = LLMChain(llm=llm, prompt=qa_prompt)
        cypher_generation_chain = LLMChain(llm=llm, prompt=cypher_prompt)
        check_schema_chain = LLMChain(llm=llm, prompt=check_prompt)

        return cls(
            qa_chain=qa_chain,
            cypher_generation_chain=cypher_generation_chain,
            check_schema_chain=check_schema_chain,
            **kwargs,
        )

    def _call(
        self,
        inputs: Dict[str, Any],
        run_manager: Optional[CallbackManagerForChainRun] = None,
    ) -> Dict[str, Any]:
        """Generate Cypher statement, use it to look up in db and answer question."""
        _run_manager = run_manager or CallbackManagerForChainRun.get_noop_manager()
        callbacks = _run_manager.get_child()
        question = inputs[self.input_key]

        intermediate_steps: List = []
        """Initialize from Graph."""

        generated_cypher = self.cypher_generation_chain.run(
            {"question": question, "schema": self.graph.schema, "cases": Cases},
            callbacks=callbacks,
        )

        # Extract the Cypher code from the generated text
        generated_cypher = extract_cypher(generated_cypher)
        generated_cypher = self.check_schema_chain.run(
            {"query": generated_cypher, "schema": self.graph.schema},
            callbacks=callbacks,
        )
        generated_cypher = extract_cypher(generated_cypher)

        _run_manager.on_text("Generated Cypher:", end="\n", verbose=self.verbose)
        _run_manager.on_text(
            generated_cypher, color="green", end="\n", verbose=self.verbose
        )

        intermediate_steps.append({"query": generated_cypher})

        # context = graph_interface.execute(generated_cypher, lang="cypher")
        # intermediate_steps.append({"context": context})

        # final_result = context

        chain_result: Dict[str, Any] = {self.output_key: generated_cypher}
        if self.return_intermediate_steps:
            chain_result[INTERMEDIATE_STEPS_KEY] = intermediate_steps

        return chain_result
