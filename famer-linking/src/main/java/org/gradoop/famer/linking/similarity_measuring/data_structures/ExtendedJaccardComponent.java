package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class ExtendedJaccardComponent extends SimilarityComponent{
    private String tokenizer;
    private Double threshold;
    private Double jaroWinklerThreshold;
    public ExtendedJaccardComponent(
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
            Double Threshold, 
            Double JaroWinklerThreshold) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        tokenizer = Tokenizer;
        threshold = Threshold;
        jaroWinklerThreshold = JaroWinklerThreshold;
    }
    public String getTokenizer(){return tokenizer;}
    public Double getThreshold(){return threshold;}
    public Double getJaroWinklerThreshold(){return jaroWinklerThreshold;}
}
