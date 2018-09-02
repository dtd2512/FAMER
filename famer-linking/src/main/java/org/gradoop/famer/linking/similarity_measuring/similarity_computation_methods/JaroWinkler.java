package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the JaroWinkler method
 */
public class JaroWinkler {
    private double threshold;
    public JaroWinkler(double Threshold){threshold = Threshold;}
    public double computeSimilarity(String str1, String str2) {
        double simdegree;
        info.debatty.java.stringsimilarity.JaroWinkler jw = new info.debatty.java.stringsimilarity.JaroWinkler(threshold);
        simdegree = jw.similarity(str1, str2);
        return simdegree;
    }
}
