import json
import os
from typing import List

from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain_community.vectorstores import FAISS
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
import config
from pydantic import BaseModel, Field
import json

my_config = config.Config()
class GeneratedCode(BaseModel):
    """Code generation output format"""
    code: str = Field(description="c++/java/python code")
    explanation: str = Field(description="design rationale")


class CodeEvaluation(BaseModel):
    """Evaluation metrics format"""
    strengths: str = Field(description="strengths")
    disadvantage: str = Field(description="disadvantage")
    compliance_score: float = Field(..., ge=0, le=100, alias="Compliance score")
    correctness_score: float = Field(..., ge=0, le=100, alias="Correctness score")
    readability_score: float = Field(..., ge=0, le=100, alias="Readability score")


class EvaluationsResponse(BaseModel):
    """Container for multiple code evaluations"""
    evaluations: List[CodeEvaluation]

llm = ChatOpenAI(
    openai_api_base="https://chatapi.littlewheat.com/v1",
    openai_api_key=my_config.get_api_key(),
    model = 'gpt-4o',
    temperature=0.7
)

embeddings = OpenAIEmbeddings(
    openai_api_base="https://chatapi.littlewheat.com/v1",
    openai_api_key=my_config.get_api_key()
)

platforms = ['Flash', 'Ligra', 'Grape', 'PowerGraph', 'Pregel', 'Graphx', 'Gthinker']
# platforms = ['Gthinker']

# algorithms = ['kClique']
algorithms = ['PageRank', 'SSSP', 'kCore', 'BC', 'LPA', 'TriangleCounting', 'kClique', 'CC']

levels = ['1', '2', '3', '4']

def Evaluation(platform, algorithm):
    if not os.path.isdir('knowledge_base/' + platform + '_index'):
        my_config.build_faiss_index('knowledge_base/' + platform, 'knowledge_base/' + platform + '_index')

    try:
        vectorstore = FAISS.load_local(
            folder_path='knowledge_base/' + platform + '_index',
            embeddings=embeddings,
            allow_dangerous_deserialization=True 
        )
    except Exception as e:
        print(f"load error: {str(e)}")

    retriever = vectorstore.as_retriever()
    # search_tool = Tool(
    #     name="Knowledge Search",
    #     func=retriever.get_relevant_documents,
    #     description="Useful for searching information in the knowledge base"
    # )

    generate_code_prompt_template = ChatPromptTemplate.from_template(my_config.get_CODE_GEN_TEMPLATE())
    evaluation_prompy_template = ChatPromptTemplate.from_template(my_config.get_EVAL_TEMPLATE())
    code_parser = PydanticOutputParser(pydantic_object=GeneratedCode)
    ev_parser = PydanticOutputParser(pydantic_object=EvaluationsResponse)
    codes = []
    for level in levels:
        prompt = generate_code_prompt_template.format(
            algorithm=algorithm,
            language=my_config.get_language(platform),
            prompt_level=my_config.get_prompt(platform, algorithm, level)
        )

        qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=retriever,
            return_source_documents=True
        )

        # print(prompt)

        response = qa_chain.invoke({"query": prompt})
        # print(response['result'])

        parsed_result = code_parser.parse(response['result'])
        json_output = parsed_result.model_dump_json()

        data = json.loads(json_output)
        codes.append(data["code"])
        print(data["code"])


    ev_chain = evaluation_prompy_template | llm

    # print(evaluation_prompy)

    response = ev_chain.invoke({
        "algorithm": algorithm,
        "standard_code": my_config.get_standard_code(platform, algorithm),
        "evaluate_code": my_config.get_evaluate_code(codes)
    })



    # print(response.content)

    ev_parsed_result = ev_parser.parse(response.content)
    json_output = ev_parsed_result.model_dump_json()
    data = json.loads(json_output)
    for i in range(len(codes)):
        print(data['evaluations'][i])



def main():
    platform = input("Enter platform (Pregel, Grape, GraphX, Gthinker, Flash, PowerGraph, Ligra): ")
    algorithm = input("Enter algorithm (PageRank, SSSP, kCore, BC, LPA, TriangleCounting, kClique, CC): ")

    # platform = 'Flash'
    # algorithm = 'TriangleCounting'


    if platform not in platforms or algorithm not in algorithms:
        print("Please choose the correct platform and algorithm.")
        exit(0)

    Evaluation(platform, algorithm)


if __name__ == "__main__":
    main()
