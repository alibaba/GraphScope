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


from langchain.chat_models import ChatOpenAI

from .langchain_cypher import GraphCypherQAChain


def query_to_cypher(
    graph, query: str, endpoint: str = None, api_key: str = None, **kwargs
) -> str:
    """
    The `query_to_cypher` function is a wrapper around the `LangChain` library, facilitating
    the conversion of natural language queries into the Cypher query language for graphscope databases.
    It interacts with a Large Language Model (LLM) for query translation, suitable for graph database operations.

    Args:
        graph (graphscope.Graph): Graph object for context in LLM.
        query (str): Natural language query for translation.
        endpoint (str, optional): URL for the OpenAI API. Defaults to standard endpoint.
        api_key (str, optional): Authentication key for OpenAI API.

    Optional Args (**kwargs):
        model_name (str): OpenAI model for processing queries. Defaults to 'gpt-3.5-turbo'.
        temperature (float): Randomness in output, 0 (deterministic) to 1 (diverse). Defaults to 0.
        verbose (bool): Enables verbose mode for process details. Defaults to True.
        return_intermediate_steps (bool): If true, returns steps of query transformation.

    Returns:
        cypher_query (str): Cypher query for graph databases.
        answer: Direct response from LLM to the input query.

    Note:
        - Requires internet and may incur API usage charges.
        - A valid OpenAI API key is necessary.

    Example:
        cypher_query, answer = query_to_cypher(graph, "What are the movies starring Tom Hanks?")
        Generates a Cypher query for use in graph databases.
    """
    if endpoint is None:
        endpoint = "https://api.openai.com"
    if api_key is None:
        print("Please input your OpenAI API key")
        return None

    model_name = kwargs.get("model_name", "gpt-3.5-turbo")
    temperature = kwargs.get("temperature", 0)
    verbose = kwargs.get("verbose", True)
    return_intermediate_steps = kwargs.get("return_intermediate_steps", False)

    chain = GraphCypherQAChain.from_llm(
        ChatOpenAI(
            openai_api_base=endpoint,
            openai_api_key=api_key,
            model_name=model_name,
            temperature=temperature,
        ),
        graph=graph,
        verbose=verbose,
        return_intermediate_steps=return_intermediate_steps,
    )

    results = chain.run(query)
    query_cypher = results[1].query
    answer = results[0]

    return query_cypher, answer
