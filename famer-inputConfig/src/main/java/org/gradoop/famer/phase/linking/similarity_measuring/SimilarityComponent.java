package org.gradoop.famer.phase.linking.similarity_measuring;

import org.gradoop.famer.linking.similarity_measuring.data_structures.*;
import org.w3c.dom.Element;

/**
 */
public class SimilarityComponent {
    private String srcAttribute;
    private String targetAttribute;
    private String srcGraphLabel;
    private String srcLabel;   
    private String targetGraphLabel;
    private String targetLabel;    
    private Double weight;
    private SimilarityComputationMethod method;
    private String id;
    /////////////////////////////////////////////
    private String stringParam;
    private Double doubleParam;
    private Double secondDoubleParam;
    private Integer integerParam;
    private SimilarityComputationMethod secondSimilarityComputationMethod;
    private Boolean booleanParam;

    public SimilarityComponent(Element PhaseContent){
        id = PhaseContent.getElementsByTagName("Id").item(0).getTextContent();
        srcAttribute = PhaseContent.getElementsByTagName("SrcAttribute").item(0).getTextContent();
        targetAttribute = PhaseContent.getElementsByTagName("TargetAttribute").item(0).getTextContent();
        srcGraphLabel = PhaseContent.getElementsByTagName("SrcGraphLabel").item(0).getTextContent();
        targetGraphLabel = PhaseContent.getElementsByTagName("TargetGraphLabel").item(0).getTextContent();
        weight = Double.parseDouble(PhaseContent.getElementsByTagName("Weight").item(0).getTextContent());
        method = SimilarityComputationMethod.valueOf(PhaseContent.getElementsByTagName("similarityComputationMethod").item(0).getTextContent());
        switch (method){
            case EXTENDED_JACCARD:
                stringParam = PhaseContent.getElementsByTagName("StringParam").item(0).getTextContent();
                doubleParam = Double.parseDouble(PhaseContent.getElementsByTagName("DoubleParam").item(0).getTextContent());
                secondDoubleParam = Double.parseDouble(PhaseContent.getElementsByTagName("SecondDoubleParam").item(0).getTextContent());
                break;
            case JAROWINKLER:
                doubleParam = Double.parseDouble(PhaseContent.getElementsByTagName("DoubleParam").item(0).getTextContent());
                break;
            case LONGEST_COMMON_SUBSTRING:
                integerParam = Integer.parseInt(PhaseContent.getElementsByTagName("IntegerParam").item(0).getTextContent());
                secondSimilarityComputationMethod = SimilarityComputationMethod.valueOf(PhaseContent.getElementsByTagName("similarityComputationMethod").item(0).getTextContent());
                break;
            case MONGE_ELKAN:
                stringParam = PhaseContent.getElementsByTagName("StringParam").item(0).getTextContent();
                doubleParam = Double.parseDouble(PhaseContent.getElementsByTagName("DoubleParam").item(0).getTextContent());
                break;
            case NUMERICAL_SIMILARITY_MAXDISTANCE:
            case NUMERICAL_SIMILARITY_MAXPERCENTAGE:
                doubleParam = Double.parseDouble(PhaseContent.getElementsByTagName("DoubleParam").item(0).getTextContent());
                break;
            case QGRAMS:
                integerParam = Integer.parseInt(PhaseContent.getElementsByTagName("IntegerParam").item(0).getTextContent());
                secondSimilarityComputationMethod = SimilarityComputationMethod.valueOf(PhaseContent.getElementsByTagName("similarityComputationMethod").item(0).getTextContent());
                booleanParam = Boolean.parseBoolean(PhaseContent.getElementsByTagName("BooleanParam").item(0).getTextContent());
                break;
            case TRUNCATE_BEGIN:
            case TRUNCATE_END:
                integerParam = Integer.parseInt(PhaseContent.getElementsByTagName("IntegerParam").item(0).getTextContent());
                break;
        }
    }

    public org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent toSimilarityComponent (){
        switch (method){
            case EXTENDED_JACCARD:
                return new ExtendedJaccardComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, stringParam, doubleParam, secondDoubleParam);
            case JAROWINKLER:
                return new JaroWinklerComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, doubleParam);
            case LONGEST_COMMON_SUBSTRING:
                return new LongestCommSubComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, integerParam, secondSimilarityComputationMethod);
            case MONGE_ELKAN:
                return new MongeElkanComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, stringParam, doubleParam);
            case NUMERICAL_SIMILARITY_MAXDISTANCE:
                return new NumeriacalSimilarityWithMaxDisComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, doubleParam);
            case NUMERICAL_SIMILARITY_MAXPERCENTAGE:
                return new NumeriacalSimilarityWithMaxPercentgComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, doubleParam);
            case QGRAMS:
                return new QGramsComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, integerParam, booleanParam, secondSimilarityComputationMethod);
            case TRUNCATE_BEGIN:
            case TRUNCATE_END:
                return new TruncateComponent(id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight, integerParam);
            case EDIT_DISTANCE:
                return new org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent(
                        id, method, srcGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight);
        }
        return null;
    }
}
