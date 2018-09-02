package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComputationMethod;

/**
 * computes the similarity degree of two strings using the LongestCommonSubstring method
 */
public class LongestCommonSubstring {
    private Integer minLength;
    private SimilarityComputationMethod method;
    public LongestCommonSubstring(Integer MinLength, SimilarityComputationMethod SecondMethod){
        minLength = MinLength;
        method = SecondMethod;
    }
    public double ComputeSimilarity (String str1, String str2){
        double simdegree = 0;
        char[] str1Array = str1.toCharArray();
        char[] str2Array = str2.toCharArray();
        int str1Size = str1Array.length;
        int str2Size = str2Array.length;
        int i,j,k, sumSim;
        i = j= k= sumSim = 0;
        while (i<str1Size && j<str2Size){
            if (str1Array[i]==str2Array[j]){
                i++;
                j++;
                k++;
            }
            else {
                if (k < minLength) {
                    if(j>=str2Array.length) {
                        i++;
                        j=0;
                    }
                    else
                        j++;

                }
                else //if(k >= minLength)
                {
                    sumSim +=k;
                    for (int t=i;t<str1Size;t++)
                        str1Array[t-k]=str1Array[t];
                    str1Size = str1Size-k;
                    for (int t=i;t<str2Size;t++)
                        str2Array[t-k]=str2Array[t];
                    str2Size = str2Size-k;
                    i=j=k=0;
                }
            }
        }
        switch (method){
            case OVERLAP:
                simdegree = sumSim/(double) Math.min(str1.length(),str2.length());
                break;
            case JACARD:
                simdegree = sumSim/(double) (str1.length()+str2.length()-sumSim);
                break;
            case DICE:
                simdegree = 2*sumSim/(double) (str1.length()+str2.length());
                break;
        }

        return simdegree;
    }
}
