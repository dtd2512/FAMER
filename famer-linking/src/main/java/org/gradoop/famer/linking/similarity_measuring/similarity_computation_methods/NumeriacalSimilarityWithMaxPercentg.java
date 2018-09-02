package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the NumeriacalSimilarity method with a defined maximum percentage of distance
 */
public class NumeriacalSimilarityWithMaxPercentg {
    private Double maxToleratedPercentage;
    public NumeriacalSimilarityWithMaxPercentg(Double MaxToleratedPercentage){maxToleratedPercentage = MaxToleratedPercentage;}
    public double computeSimilarity(Double in1, Double in2) {
        double simdegree ;
        Double pc = Math.abs(in1 - in2) * 100 / Math.max(in1, in2);
        if (pc < maxToleratedPercentage)
            simdegree = 1 - (pc / maxToleratedPercentage);
        else simdegree = 0;
        return simdegree;
    }
}
