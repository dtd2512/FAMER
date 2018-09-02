package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the NumeriacalSimilarity method with a defined maximum distance
 */
public class NumeriacalSimilarityWithMaxDis {
    private Double maxToleratedDis;
    public NumeriacalSimilarityWithMaxDis(Double MaxToleratedDis){maxToleratedDis = MaxToleratedDis;}
    public double computeSimilarity(Double in1, Double in2) {
        double simdegree;
        if (Math.abs(in1 - in2) < maxToleratedDis)
            simdegree = 1 - (Math.abs(in1 - in2) / maxToleratedDis);
        else simdegree = 0;
        return simdegree;
    }
}
