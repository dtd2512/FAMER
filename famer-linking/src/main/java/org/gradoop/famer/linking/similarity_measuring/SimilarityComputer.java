package org.gradoop.famer.linking.similarity_measuring;

import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.QGramsComponent;
import org.gradoop.famer.linking.similarity_measuring.data_structures.*;
import org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods.*;

import java.io.Serializable;

/**
 */
public class SimilarityComputer
implements Serializable {


    private SimilarityComponent similarityComponent;
    public SimilarityComputer (SimilarityComponent SimilarityComponent){similarityComponent = SimilarityComponent;}



    public SimilarityField computeSimilarity (String value1, String value2){

//        try {
//            value1 = URLDecoder.decode(value1, "utf-8");
//            value2 = URLDecoder.decode(value2, "utf-8");
//        }
//        catch (Exception e){}
//
//        value1 = value1.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().trim().replaceAll("\\s+", " ");
//        value2 = value2.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().trim().replaceAll("\\s+", " ");
        value1 = value1.toLowerCase().trim().replaceAll("\\s+", " ");
        value2 = value2.toLowerCase().trim().replaceAll("\\s+", " ");
//        if (value1.length()==0 && value2.length()==0)
//            return 1d;
//        if (value1.length()==0 || value2.length()==0)
//            return 0d;
        Double simdegree = 0d;
        SimilarityComputationMethod similarityComputationMethod = similarityComponent.getSimilarityComputationMethod();

        switch (similarityComputationMethod){
            case JAROWINKLER:
                simdegree = new JaroWinkler(((JaroWinklerComponent)similarityComponent).getThreshold()).computeSimilarity(value1, value2);
                break;
            case TRUNCATE_BEGIN:
                simdegree = new TruncateBegin(((TruncateComponent)similarityComponent).getLength()).computeSimilarity(value1, value2);
                break;
            case TRUNCATE_END:
                simdegree = new TruncateEnd(((TruncateComponent)similarityComponent).getLength()).computeSimilarity(value1, value2);
                break;
            case EDIT_DISTANCE:
                simdegree = new EditDistanceLevenshtein().computeSimilarity(value1, value2);
                break;
            case QGRAMS:
                org.gradoop.famer.linking.similarity_measuring.data_structures.QGramsComponent qgramsComponent = (org.gradoop.famer.linking.similarity_measuring.data_structures.QGramsComponent)similarityComponent;
                simdegree = new QGrams(qgramsComponent.getLength(), qgramsComponent.getPadding(), qgramsComponent.getMethod()).computeSimilarity2(value1, value2);
                break;
            case MONGE_ELKAN:
                MongeElkanComponent mongeElkanComponent = (MongeElkanComponent) similarityComponent;
                simdegree = new MongeElkanJaroWinkler(mongeElkanComponent.getTokenizer(), mongeElkanComponent.getThreshold()).computeSimilarity(value1, value2);
                break;
            case EXTENDED_JACCARD:
                ExtendedJaccardComponent extendedJaccardComponent = (ExtendedJaccardComponent) similarityComponent;
                simdegree = new ExtendedJaccard(extendedJaccardComponent.getTokenizer(), extendedJaccardComponent.getThreshold(), extendedJaccardComponent.getJaroWinklerThreshold()).ComputeSimilarity(value1, value2);
                break;
            case LONGEST_COMMON_SUBSTRING:
                LongestCommSubComponent longestCommSubComponent = (LongestCommSubComponent) similarityComponent;
                simdegree = new LongestCommonSubstring(longestCommSubComponent.getMinLength(), longestCommSubComponent.getMethod()).ComputeSimilarity(value1, value2);
                break;
            case NUMERICAL_SIMILARITY_MAXDISTANCE:
                NumeriacalSimilarityWithMaxDisComponent disComponent = (NumeriacalSimilarityWithMaxDisComponent)similarityComponent;
                simdegree = new NumeriacalSimilarityWithMaxDis(disComponent.getMaxToleratedDis()).computeSimilarity(Double.parseDouble(value1), Double.parseDouble(value2));
                break;
            case NUMERICAL_SIMILARITY_MAXPERCENTAGE:
                NumeriacalSimilarityWithMaxPercentgComponent perComponent = (NumeriacalSimilarityWithMaxPercentgComponent)similarityComponent;
                simdegree = new NumeriacalSimilarityWithMaxDis(perComponent.getMaxToleratedPercentage()).computeSimilarity(Double.parseDouble(value1), Double.parseDouble(value2));
                break;
        }
        return new SimilarityField(similarityComponent.getId(), simdegree, similarityComponent.getWeight());
    }



    public double computeGeoDistance(String strlat1, String strlon1, String strlat2, String strlon2) {
        double lat1 = Double.parseDouble(strlat1);
        double lat2 = Double.parseDouble(strlat2);
        double lon1 = Double.parseDouble(strlon1);
        double lon2 = Double.parseDouble(strlon2);
        Double arccosParam = Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon1 - lon2);
        if (arccosParam<-1)
            arccosParam = -1d;
        if (arccosParam >1)
            arccosParam = 1d;
        double distance = Math.acos(arccosParam)*6371;
        return distance;
    }
    public double convertDistance2Simdegree(Double distance){
//        distance /= 6371;
//        if (distance == 0)
//            return 1;
//        distance = 1/(distance*100);
//        if(distance>=1)
//            return 1;
//        return distance;
        if (distance>13458)
            return 0;
        else return 1;
    }

    @Override
    public String toString() {
        StringBuffer buffer= new StringBuffer();



//        buffer.append("Similarity between [" + srcAttribute+"] and ["+ targetAttribute+"]; ");
//        buffer.append("weight: " + Weight+"; ");
//        buffer.append("threshold: " + Threshold+"("+ SecondMethodThreshold+ "); ");
//        buffer.append("simMeasure: " + similarityComputationMethod+" "+ SecondMethod +" "+integerParam);
//        return buffer.toString();
        return null;

    }
}