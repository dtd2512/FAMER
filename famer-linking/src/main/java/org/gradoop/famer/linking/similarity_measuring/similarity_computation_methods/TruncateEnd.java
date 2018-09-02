package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the TruncateEnd method
 */
public class TruncateEnd {
    private Integer length;
    public TruncateEnd (Integer Length){
        length = Length;
    }
    public double computeSimilarity(String str1, String str2){
        double simdegree = 0.0;
        if(str1.length() < length || str2.length() < length){
            if (str1.equals(str2))
                simdegree = 1;
        }
        if(str1.substring(str1.length()-length).equals(str2.substring(str1.length()-length)))
            simdegree = 1;
        return simdegree;
    }
}
