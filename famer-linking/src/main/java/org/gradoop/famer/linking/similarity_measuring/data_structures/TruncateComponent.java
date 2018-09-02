package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class TruncateComponent extends SimilarityComponent{
    private Integer length;
    public TruncateComponent(
    		String ComponentId,
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Integer Length) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        length = Length;
    }
    public Integer getLength(){return length;}
}
