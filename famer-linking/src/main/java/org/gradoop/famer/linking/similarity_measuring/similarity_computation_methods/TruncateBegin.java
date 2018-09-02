package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the TruncateBegin method
 */
public class TruncateBegin {
    private Integer length;
    public TruncateBegin (Integer Length){
        length = Length;
    }
    public double computeSimilarity(String str1, String str2){
        double simdegree = 0.0;
        if(str1.length() < length || str2.length() < length){
            if (str1.equals(str2))
                simdegree = 1.0;
        }
        if(str1.substring(0,length).equals(str2.substring(0,length)))
            simdegree = 1;
        return simdegree;
    }
}
