package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class MongeElkanComponent extends SimilarityComponent{
    private String tokenizer;
    private Double threshold;
    public MongeElkanComponent(
    		String ComponentId,
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            String Tokenizer,
            Double Threshold) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        threshold = Threshold;
        tokenizer = Tokenizer;
    }
    public String getTokenizer(){return tokenizer;}
    public Double getThreshold(){return threshold;}
}
