package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComputationMethod;

import java.util.ArrayList;
import java.util.Collection;

/**
 * computes the similarity degree of two strings using the QGram method
 */
public class QGrams {
    private Integer length;
    private Boolean padding;
    private SimilarityComputationMethod method;
    public QGrams (Integer Length, Boolean Padding, SimilarityComputationMethod SecondMethod){
        length = Length;
        padding = Padding;
        method = SecondMethod;

    }
    public Double computeSimilarity(String str1, String str2){
        Double simdegree = 0d;
        Collection<String> str1QGrams = new ArrayList<String>();
        Collection<String> str2QGrams = new ArrayList<String>();
        if (padding) {
            if (str1.length() >= length) {
                str1QGrams.add(str1.substring(0, length - 1));
                str1QGrams.add(str1.substring(str1.length() - length + 1, str1.length()));
            }
            if (str2.length() >= length) {
                str2QGrams.add(str2.substring(0, length - 1));
                str2QGrams.add(str2.substring(str2.length() - length + 1, str2.length()));
            }
        }
        for (int i=0; i < str1.length() && i+length<= str1.length(); i++)
            str1QGrams.add(str1.substring(i,i+length));
        for (int i=0; i < str2.length() && i+length <= str2.length(); i++)
            str2QGrams.add(str2.substring(i,i+length));
//        Set<String> setstr1QGrams = new HashSet<String>(str1QGrams);
//        Set<String> setstr2QGrams = new HashSet<String>(str2QGrams);
//        for (String s:setstr1QGrams){
//            if (setstr2QGrams.contains(s))
//                simdegree ++;
//        }
        int str1QGramsSize = str1QGrams.size();
        int str2QGramsSize = str2QGrams.size();

        for (String s:str1QGrams){
            if (str2QGrams.contains(s)) {
                simdegree++;
                str2QGrams.remove(s);
            }
        }
        switch (method){
            case OVERLAP:
                simdegree = simdegree/Math.min(str1QGramsSize,str2QGramsSize);
                break;
            case JACARD:
                simdegree = simdegree/(str1QGramsSize+str2QGramsSize-simdegree);
                break;
            case DICE:
                simdegree = (float) 2 * simdegree / (str1QGramsSize + str2QGramsSize);
                break;
        }


        return simdegree;
    }


    public Double computeSimilarity2(String str1, String str2){

        Double simdegree = 0d;
        Collection<String> str1QGrams = new ArrayList<String>();
        Collection<String> str2QGrams = new ArrayList<String>();
        if (padding) {
            if (str1.length() >= length) {
                str1QGrams.add(str1.substring(0, length - 1));
                str1QGrams.add(str1.substring(str1.length() - length + 1, str1.length()));
            }
            if (str2.length() >= length) {
                str2QGrams.add(str2.substring(0, length - 1));
                str2QGrams.add(str2.substring(str2.length() - length + 1, str2.length()));
            }
        }
        if (str1.length()<length || str2.length()<length){
            if (str1.equals(str2))
                return 1d;
            else
                return 0d;
        }
        for (int i=0; i < str1.length() && i+length<= str1.length(); i++)
            str1QGrams.add(str1.substring(i,i+length));
        for (int i=0; i < str2.length() && i+length <= str2.length(); i++)
            str2QGrams.add(str2.substring(i,i+length));
//        Set<String> setstr1QGrams = new HashSet<String>(str1QGrams);
//        Set<String> setstr2QGrams = new HashSet<String>(str2QGrams);
//        for (String s:setstr1QGrams){
//            if (setstr2QGrams.contains(s))
//                simdegree ++;
//        }
        int str1QGramsSize = str1QGrams.size();
        int str2QGramsSize = str2QGrams.size();

        for (String s:str1QGrams){
            if (str2QGrams.contains(s)) {
                simdegree++;
                str2QGrams.remove(s);
            }
        }


//        if (method.equals("Overlap") || method.equals("O") || method.equals("1"))
//            simdegree = simdegree/Math.min(str1QGramsSize,str2QGramsSize);
//        else if (method.equals("Jaccard") || method.equals("J") || method.equals("2"))
//            simdegree = simdegree/(str1QGramsSize+str2QGramsSize-simdegree);
//
//        else if (method.equals("Dice") || method.equals("D") || method.equals("3"))
//            simdegree = (float) 2 * simdegree / (str1QGramsSize + str2QGramsSize);
        switch (method){
            case OVERLAP:
                simdegree = simdegree/Math.min(str1QGramsSize,str2QGramsSize);
                break;
            case JACARD:
                simdegree = simdegree/(str1QGramsSize+str2QGramsSize-simdegree);
                break;
            case DICE:
                simdegree = (float) 2 * simdegree / (str1QGramsSize + str2QGramsSize);
                break;
        }




//        if (Double.isNaN(simdegree)) {
//            if(str1.contains("\n") || str2.contains("\n"))
//                System.out.println("sdkfsdkflks");
//            System.out.println("str1: "+str1 + "************" + "str2: "+str2);
//
//           System.out.println("str1QGramsSize: "+str1QGramsSize);
//
//        }

        return simdegree;
    }
}
