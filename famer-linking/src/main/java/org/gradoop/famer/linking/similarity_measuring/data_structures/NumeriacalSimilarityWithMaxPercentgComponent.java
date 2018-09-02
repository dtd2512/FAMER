package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class NumeriacalSimilarityWithMaxPercentgComponent extends SimilarityComponent{
    private Double maxToleratedPercentage;

    public NumeriacalSimilarityWithMaxPercentgComponent(
    		String ComponentId,
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Double MaxToleratedPercentage) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        maxToleratedPercentage = MaxToleratedPercentage;
    }
    public Double getMaxToleratedPercentage(){return maxToleratedPercentage;}
}
