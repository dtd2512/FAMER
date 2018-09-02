package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class NumeriacalSimilarityWithMaxDisComponent extends SimilarityComponent{
    private Double maxToleratedDis;

    public NumeriacalSimilarityWithMaxDisComponent(
    		String ComponentId,
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Double MaxToleratedDis) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        maxToleratedDis = MaxToleratedDis;
    }
    public Double getMaxToleratedDis(){return maxToleratedDis;}
}
