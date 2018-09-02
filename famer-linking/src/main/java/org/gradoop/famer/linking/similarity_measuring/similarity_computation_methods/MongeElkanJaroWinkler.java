package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the MongeElkanJaroWinkler method
 */
public class MongeElkanJaroWinkler {
    private String tokenizer;
    private Double threshold;
    public MongeElkanJaroWinkler (String Tokenizer, Double Threshold){
        tokenizer = Tokenizer;
        threshold = Threshold;
    }
    public double computeSimilarity (String str1, String str2){
        double simdegree = 0;
        String[] str1tokens = str1.split(tokenizer);
        String[] str2tokens = str2.split(tokenizer);
        double max = 0;
        JaroWinkler jw = new JaroWinkler(threshold);
        for (int i = 0; i < str1tokens.length; i++){
            for (int j = 0; j < str2tokens.length; j++){
                double jwsim = jw.computeSimilarity(str1tokens[i],str2tokens[j]);
                if (jwsim > max)
                    max = jwsim;
            }
            simdegree += max;
        }
        simdegree /= str1tokens.length;
        return simdegree;
    }
}
