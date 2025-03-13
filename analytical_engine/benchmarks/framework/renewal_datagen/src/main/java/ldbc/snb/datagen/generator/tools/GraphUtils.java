package ldbc.snb.datagen.generator.tools;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GraphUtils {

    public static List<Double> clusteringCoefficientList(PersonGraph graph) {
        List<Double> CC = new ArrayList<>();
        for (Long l : graph.persons()) {
            int triangles = 0;
            Set<Long> neighbors = graph.neighbors(l);
            for (Long n : neighbors) {
                Set<Long> neighbors2 = graph.neighbors(n);
                Set<Long> aux = new HashSet<>(neighbors);
                aux.retainAll(neighbors2);
                triangles += aux.size();
            }
            int degree = neighbors.size();
            double localCC = 0;
            if (degree > 1)
                localCC = triangles / (double) (degree * (degree - 1));
            CC.add(localCC);

        }
        return CC;
    }
}
