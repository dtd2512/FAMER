package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class LongestCommSubComponent extends SimilarityComponent{
    private Integer minLength;
    private SimilarityComputationMethod method;
    public LongestCommSubComponent(
    		String ComponentId,
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Integer MinLength,
            SimilarityComputationMethod SecondMethod) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        minLength = MinLength;
        method = SecondMethod;
    }
    public Integer getMinLength(){return minLength;}
    public SimilarityComputationMethod getMethod(){return method;}
}
