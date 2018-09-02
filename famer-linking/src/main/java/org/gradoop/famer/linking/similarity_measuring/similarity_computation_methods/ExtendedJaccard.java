package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 *  * computes the similarity degree of two strings using the ExtendedJaccard method
 */
public class ExtendedJaccard {
    private String tokenizer;
    private Double threshold;
    private Double jaroWinklerThreshold;
    public ExtendedJaccard(String Tokenizer, Double Threshold, Double JaroWinklerThreshold){
        tokenizer = Tokenizer;
        threshold = Threshold;
        jaroWinklerThreshold = JaroWinklerThreshold;
    }
    public double ComputeSimilarity (String str1, String str2){
        double simdegree = 0;
        String[] str1tokens = str1.split(tokenizer);
        String[] str2tokens = str2.split(tokenizer);
        JaroWinkler jw = new JaroWinkler(jaroWinklerThreshold);
        int nonSimilarPairs = 0;
        for (int i = 0; i < str1tokens.length; i++){
            for (int j = 0; j < str2tokens.length; j++){
                double jwsim = jw.computeSimilarity(str1tokens[i],str2tokens[j]);
                if (jwsim >= threshold)
                    simdegree++;
                else
                    nonSimilarPairs++;
            }
        }
        simdegree /= (simdegree+nonSimilarPairs);
        return simdegree;
    }
}
