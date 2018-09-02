package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class JaroWinklerComponent extends SimilarityComponent{
    private Double threshold;
    public JaroWinklerComponent(
    		String ComponentId, 
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute, 
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute, 
            Double Weight,
            Double Threshold) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        threshold = Threshold;
    }
    public Double getThreshold(){return threshold;}
}
