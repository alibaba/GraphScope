import pytest

import graphscope.nx as nx


@pytest.mark.usefixtures("graphscope_session")
class TestAsteroidal:
    def test_is_at_free(self):
        is_at_free = nx.asteroidal.is_at_free

        cycle = nx.cycle_graph(6)
        assert not is_at_free(cycle)

        path = nx.path_graph(6)
        assert is_at_free(path)

        small_graph = nx.complete_graph(2)
        assert is_at_free(small_graph)

        petersen = nx.petersen_graph()
        assert not is_at_free(petersen)

        clique = nx.complete_graph(6)
        assert is_at_free(clique)
