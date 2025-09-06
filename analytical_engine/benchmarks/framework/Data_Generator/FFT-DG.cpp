#include <bits/stdc++.h>
#include <random>

struct edge_weight{
    int vertex; 
    int weight;
};
// std::random_device rd;
std::default_random_engine eng(0);
std::uniform_real_distribution<> distr(0.0, 1.0);
uint64_t N, M = 0;
int SCALE;
std::string TYPE, FEATURE;
std::vector<std::vector<int> > graph;
std::vector<std::vector<int> > graph_ligra;
std::vector<std::vector<edge_weight> > graph_ligra_sssp;
double percentages[3] = {0.45, 0.9, 1.0};
std::vector<int> degree;
std::vector<int> id;
struct bucket {
    int min_degree, max_degree;
};
std::vector<bucket> buckets;

std::mt19937 gen(0);
std::uniform_int_distribution<int> disint(1, 100);

// int targetEdge(int x, int s) {
//     int generated_edges = 0;
//     for (int i = 0; i < s; ++i) {
//         generated_edges += std::ceil(percentages[i] * degree[x]);
//     }
//     generated_edges = std::min(generated_edges, (int) degree[x]);
//     int to_generate = std::min((int) degree[x] - generated_edges, (int) ceil(percentages[s] * degree[x]));
//     return to_generate;
// }

/* generate degree distribution (Facebook) */
void solveDistribution(int N);

/* initialize the graph and degree */
void initialize();

/* generate community for this graph */
void Groupgenerator();

std::string itoa(int x) {
    char a[25];
    sprintf(a, "%d", x);
    return a;
}

void output_snap(std::string framework) {
    std::fstream ofs("./" + framework + "-edges-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        for (auto &j : graph[i]) {
            if (j > i) {
                ofs << i << ' ' << j << '\n';
            }
        }
    }
}

void output_snap_weight(std::string framework) {
    std::fstream ofs("./" + framework + "-edges-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        for (auto &j : graph[i]) {
            if (j > i) {
                int w = disint(gen);
                ofs << i << ' ' << j << ' ' << w <<'\n';
            }
        }
    }
}

void output_adj(std::string framework) {
    std::fstream ofs("./" + framework + "-adj-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        ofs << i << "\t" << graph[i].size();
        for (auto &j : graph[i]) {
            ofs << ' ' << j;
        }
        ofs << "\n";
    }
}

void output_weighted_adj(std::string framework) {
    std::fstream ofs("./" + framework + "-adj-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        ofs << i;
        for (auto &j : graph[i]) {
            ofs << " " << j << " 1";
        }
        ofs << "\n";
    }
}

void output_labeled_adj(std::string framework) {
    std::fstream ofs("./" + framework + "-adj-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        ofs << i << " " << i;
        for (auto &j : graph[i]) {
            ofs << " " << j;
        }
        ofs << "\n";
    }
}

void output_powergraph_bc(std::string framework) {
    std::fstream ofs("./" + framework + "-adj-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        ofs << i;
        for (auto &j : graph[i]) {
            ofs << " " << j;
        }
        ofs << "\n";
    }
}

void output_ligra(std::string framework) {
    std::fstream ofs("./" + framework + "-adj-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    ofs << "AdjacencyGraph" << '\n';
    
    uint64_t num = 0;
    // int num2 = 3;
    uint64_t all_edge = 0;
    graph_ligra.resize(N);

    for (int i = 0; i < N; ++i) {
        for (int j = 0;j<graph[i].size();j++) {
            graph_ligra[i].push_back(j);
            graph_ligra[j].push_back(i);
        }
    }

    for (int i = 0; i < N; ++i) {
        sort(graph_ligra[i].begin(), graph_ligra[i].end());
        all_edge+=graph_ligra[i].size();
    }
    ofs << N << '\n';
    ofs << all_edge << '\n';

    for (int i = 0; i < N; ++i) {
        ofs << num << "\n";
        // if(num2%1000 == 0)
        // std::cout<<num2++<<std::endl;
        num += graph_ligra[i].size();
    }

    for (int i = 0; i < N; ++i) {
        // std::cout<<num2<<std::endl;
        for (int j = 0;j<graph_ligra[i].size();j++) {
            ofs << graph_ligra[i][j] << "\n";
            // num2++;

        }
    }
}

void output_ligra_sssp(std::string framework) {
    std::fstream ofs("./" + framework + "-adj-" + itoa(SCALE) + "-" + FEATURE + ".txt", std::ios::out);
    ofs << "WeightedAdjacencyGraph" << '\n';
    
    uint64_t num = 0;
    // int num2 = 3;
    uint64_t all_edge = 0;
    graph_ligra_sssp.resize(N);

    for (int i = 0; i < N; ++i) {
        for (int j = 0;j<graph[i].size();j++) {
            int w = disint(gen);
            // int w=0;
            graph_ligra_sssp[i].push_back({j, w});
            graph_ligra_sssp[j].push_back({i, w});
        }
    }

    for (int i = 0; i < N; ++i) {
        // sort(graph_ligra_sssp[i].begin(), graph_ligra_sssp[i].end());
        all_edge+=graph_ligra_sssp[i].size();
    }
    ofs << N << '\n';
    ofs << all_edge << '\n';

    for (int i = 0; i < N; ++i) {
        ofs << num << "\n";
        // if(num2%1000 == 0)
        // std::cout<<num2++<<std::endl;
        num += graph_ligra_sssp[i].size();
    }

    for (int i = 0; i < N; ++i) {
        // std::cout<<num2<<std::endl;
        for (int j = 0;j<graph_ligra_sssp[i].size();j++) {
            ofs << graph_ligra_sssp[i][j].vertex << "\n";
            // num2++;

        }
    }

     for (int i = 0; i < N; ++i) {
        // std::cout<<num2<<std::endl;
        for (int j = 0;j<graph_ligra_sssp[i].size();j++) {
            ofs << graph_ligra_sssp[i][j].weight << "\n";
            // num2++;

        }
    }
}

void output_vertice(std::string framework) {
    std::fstream ofs("./" + framework + "-vertices-" + itoa(N) + FEATURE + ".txt", std::ios::out);
    for (int i = 0; i < N; ++i) {
        ofs << i << '\n';
    }
}

int main(int argc, char** argv) {
    printf("start...\n");
    SCALE = std::atoi(argv[1]);
    TYPE = argv[2];
    FEATURE = argv[3];
    printf("SCALE = %d\n", SCALE);
    if (SCALE == 7) {
        N = 1254000;
    }   else if (SCALE == 8) {
        N = 3600000;
    }   else if (SCALE == 9) {
        N = 27200000;
    }   else if (SCALE == 95) {
        N = 77000000;
    }   else if (SCALE == 10) {
        N = 210000000;
    }   else {
        assert(0);
    }
    printf("Generating graph with %ld vetices, TYPE = %s, FEATURE = %s\n", N, TYPE.c_str(), FEATURE.c_str());
    double alpha = 1000.0;
    int group_size = N;
    if (FEATURE == "Density") {
        N /= 3;
        alpha = 20.0;
        solveDistribution(N);
        printf("generate dense graph, alpha = %f\n", alpha);

    }   else if (FEATURE == "Diameter") {
        group_size = N / (100 / 5);
        solveDistribution(N);
        alpha = 80.0;
        printf("generate large diameter, group size is %d\n", group_size);

    }   else if (FEATURE == "Standard") {
        solveDistribution(N);

    }   else {
        assert(0);
    }
    
    initialize();

    if (FEATURE == "Diameter") {
        std::shuffle(id.begin() + 1, id.end(), eng);
        for (int i = 0; i < N - 1; ++i) {
            graph[id[i]].push_back(id[i + 1]);
            graph[id[i + 1]].push_back(id[i]);
        }
        for (int i = 0; i < N; ++i) {
            // if (i % (N / 100) == 0) std::cout << i << " / " << N << std::endl;
            int j = i;
            int c = 0;
            while (graph[id[i]].size() < degree[id[i]]) {
                double f = distr(eng);
                double temp_k = j + ((1.0 / f - 1) * c / alpha) + 1;
                if (temp_k >= (double) N) break;
                int k = (int) temp_k;
                if (i / group_size != k / group_size) break;
                if (graph[id[k]].size() < degree[id[k]]) {
                    graph[id[i]].push_back(id[k]);
                    graph[id[k]].push_back(id[i]);
                }
                c = c + k - j;
                j = k;
            }
        }
    }   else {
        long long test = 0;
        clock_t start = clock() ;

        // our method
        for (int step = 0; step < 3; step++) {
            std::shuffle(id.begin(), id.end(), eng);
            for (int i = 0; i < N; ++i) {
                int j = i;
                int c = 0;
                while (graph[id[i]].size() < degree[id[i]] * percentages[step]) {
                    double f = distr(eng);
                    test++;
                    double temp_k = j + ((1.0 / f - 1) * c / alpha) + 1;
                    if (temp_k >= (double) N) break;
                    int k = (int) temp_k;
                    if (i / group_size != k / group_size) break;
                    if (graph[id[k]].size() < degree[id[k]] * percentages[step]) {
                        graph[id[i]].push_back(id[k]);
                        graph[id[k]].push_back(id[i]);
                    }
                    c = c + k - j;
                    j = k;
                }
            }
        }

        // ldbc method
        // for (int step = 0; step < 3; step++) {
        //     std::shuffle(id.begin(), id.end(), eng);
        //     for (int i = 0; i < N; ++i) {
        //         double base_prob = 0.95;
        //         double lim_prob = 0.2;
        //         double real_prob = 1;
        //         for (int j = i + 1;j < N; j++) {
        //             if (graph[id[i]].size() >= degree[id[i]] * percentages[step]) break;
        //             double f = distr(eng);
        //             test++;
        //             real_prob *= base_prob;
        //             if (f < real_prob || f < lim_prob) {
        //                 if (graph[id[j]].size() < degree[id[j]] * percentages[step]) {
        //                     graph[id[i]].push_back(id[j]);
        //                     graph[id[j]].push_back(id[i]);
        //                 }
        //             }
        //         }
        //     }
        // }

        clock_t end = clock() ;
        std::cout << "total time: " << 1.0 * (end - start) / CLOCKS_PER_SEC << " sec" << std::endl;
        std::cout << "total test: " << test << " trials" << std::endl;
    }

    

    // delete redundant edges
    for (int i = 0;i < N; i++) {
        std::sort(graph[i].begin(), graph[i].end());
        auto end_unique = std::unique(graph[i].begin(), graph[i].end());
        graph[i].erase(end_unique, graph[i].end());
    }
    // printf("Generating communities.\n");
    // Groupgenerator();
    
    // delete double edges
    for (int i = 0;i < N; i++) {
        graph[i].erase(std::remove_if(graph[i].begin(), graph[i].end(), [i](int elem) {
        return elem < i;
    }), graph[i].end());
        M += graph[i].size();
        // M += degree[i];
    }
    printf("Output. Totally %ld verices and %ld edges.\n", N, M);

    if (TYPE == "flash") {
        output_snap("flash");

    }   else if (TYPE == "grape") {
        output_vertice("grape");
        output_snap("grape");

    }   else if (TYPE == "flash-sssp") {
        output_snap_weight("flash-sssp");

    }   else if (TYPE == "grape-sssp") {
        output_vertice("grape-sssp");
        output_snap_weight("grape-sssp");

    }   else if (TYPE == "powergraph") {
        output_snap("powergraph");

    }   else if (TYPE == "powergraph-bc") {
        output_powergraph_bc("powergraph-bc");

    }   else if (TYPE == "powergraph-lpa") {
        output_labeled_adj("powergraph-lpa");

    }   else if (TYPE == "pregel+") {
        output_adj("pregel+");

    }   else if (TYPE == "ligra") {
        output_ligra("ligra");

    }   else if (TYPE == "graphx") {
        output_snap("graphx");
    
    }   else if (TYPE == "graphx-weight") {
        output_snap_weight("graphx-weight");

    }   else if (TYPE == "ligra-sssp") {
        output_ligra_sssp("ligra-sssp");

    }   else {
        assert(0);
    }

    
    
    
    return 0;
}

void solveDistribution(int N) {
    int mean = round(pow(N, 0.512 - 0.028 * log10(N)));
    std::ifstream ifs("./facebookBucket100.dat", std::ios::in);
    double min_degree, max_degree;
    int id;
    int siz = 0;
    buckets.resize(100);
    // std::cout << "check\n";
    for (int i = 0; i < 100; i++) {
        ifs >> min_degree >> max_degree >> id;
        siz += (max_degree + min_degree) / 2;
        buckets[i].min_degree = min_degree;
        buckets[i].max_degree = max_degree;
    }
    double new_min_degree;
    double new_max_degree;
    for (bucket &bucket: buckets) {
        new_min_degree = 1.0 * bucket.min_degree * mean / 190;
        new_max_degree = 1.0 * bucket.max_degree * mean / 190;
        if (new_max_degree < new_min_degree) new_max_degree = new_min_degree;
        bucket.min_degree = new_min_degree;
        bucket.max_degree = new_max_degree;
    }

}

int nextDegree() {
    int bucket = rand() % buckets.size();
    int min_degree = (int) buckets[bucket].min_degree;
    int max_degree = (int) buckets[bucket].max_degree;
    return rand() % (max_degree - min_degree + 1) + min_degree;
}

void initialize() {
    uint64_t tot_d = 0;
    for (int i = 0; i < N; ++i) {
        graph.push_back(std::vector<int>());
        int d = nextDegree();
        if (FEATURE == "Density") {
            d *= 9;
        }
        degree.push_back(d);
        id.push_back(i);
        tot_d += d;
    }
    std::cout << "total degree =  " << tot_d <<  std::endl;
}

void Groupgenerator() {
    std::ofstream ofs("./LDBC-community.txt", std::ios::out);

    for (int i = 0; i < N; i++) {
        double random = distr(eng);
        if (random >= 0.05) continue;

        // each people has 5% probability to create at most 4 groups
        int num_group = (int) (distr(eng) * 4);

        while (num_group--) {
            std::unordered_map<int, int> mp;
            mp[i] = 2;
            for (auto &neighbor: graph[i]) {
                mp[neighbor] = 1;
            }
            int num_member = (int) (distr(eng) * 100);
            int iter = 0;

            ofs << i;
            while (num_member && iter < 10000) {
                iter++;
                random = distr(eng);
                if (random < 0.3 && !graph[i].empty()) {
                    int idx = (int) (distr(eng) * graph[i].size());
                    int v = graph[i][idx];
                    if (mp[v] == 1) {
                        ofs << " " << v;
                        mp[v] = 2;
                        num_member--;
                    }
                } else {
                    // int v = (int) (distr(eng) * N);
                    int v = (int) (distr(eng) * 1000);
                    if (distr(eng) > 0.5) v = i + v; else v = i - v;
                    if (v < 0 || v >= graph.size()) continue;
                    // v = std::max(v, 0);
                    // v = std::min(v, (int) graph.size() - 1);
                    random = distr(eng);
                    if (random < 0.1 && mp[v] != 2) {
                        ofs << " " << v;
                        mp[v] = 2;
                        num_member--;
                    }
                }
            }
            ofs << "\n";
        }
    }
}