import graphscope
import pytest

graphscope.set_option(show_log=True)
graphscope.set_option(initializing_interactive_engine=False)


def test_create_session():
    s = graphscope.session(
        k8s_gs_image="registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:docker_schedule_ci",
        k8s_gie_graph_manager_image="registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:docker_schedule_ci",
    )
    info = s.info
    assert info["status"] == "active"

    graph = s.g()
    interactive = s.gremlin(graph)
    print(interactive)

    s.close
