package org.gradoop.famer.linking.similarity_measuring.data_structures;

import com.sun.org.apache.xpath.internal.operations.Bool;

/**
 */
public class QGramsComponent extends SimilarityComponent{
    private Integer length;
    private Boolean padding;
    private SimilarityComputationMethod method;
    public QGramsComponent(
    		String ComponentId,
    		SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute, 
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute, 
            Double Weight,
            Integer Length, 
            Boolean Padding, 
            SimilarityComputationMethod SecondMethod) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        length = Length;
        padding = Padding;
        method = SecondMethod;
    }
    public Integer getLength(){return length;}
    public Boolean getPadding(){return padding;}
    public SimilarityComputationMethod getMethod(){return method;}
}
