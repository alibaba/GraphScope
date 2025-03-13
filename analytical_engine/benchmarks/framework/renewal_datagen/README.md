# Renewal LDBC Datagen

### 1. Distance Hop Generator

add `DistanceHopKnowsGenerator.java` to generate social networks faster

use `alpha` to control Clustering Coefficient

### 2. Diameter Generator (in the `Diameter` branch, cover the original version)

1. connect the network (`DistanceHopKnowsGenerator.java`, line 23)
2. add edges in each block (`DistanceHopKnowsGenerator.java`, line 45)
3. trace the head and the tail vertex in each group (`HadoopKnowsGenrator.java`, line 90)
4. add edges between the head and the tail vertices among groups (`HadoopMergeFriendshipFiles.java`, line 53)

NOTE: do not shuffle the persons