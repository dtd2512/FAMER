package org.gradoop.famer.linking.similarity_measuring.data_structures;

import java.io.Serializable;

/**
 */
public class SimilarityComponent implements Serializable {
    private String srcAttribute;
    private String targetAttribute;
    private String srcGraphLabel;
    private String targetGraphLabel;
    private String srcLabel;
    private String targetLabel;   
    private Double weight;
    private SimilarityComputationMethod similarityComputationMethod;
    private String id;

    public SimilarityComponent(
    		String componentId, 
    		SimilarityComputationMethod similarityComputationMethod,
            String sourceGraphLabel,
            String srcLabel,
            String srcAttribute, 
            String targetGraphLabel,
            String targetLabel,
            String targetAttribute, 
            Double weight) {
        this.srcAttribute = srcAttribute;
        this.targetAttribute = targetAttribute;
        this.srcGraphLabel = sourceGraphLabel;
        this.targetGraphLabel = targetGraphLabel;
        this.srcLabel = srcLabel;
        this.targetLabel = targetLabel;       
        this.weight = weight;
        this.similarityComputationMethod = similarityComputationMethod;
        this.id = componentId;
    }

    public SimilarityComputationMethod getSimilarityComputationMethod(){return similarityComputationMethod;}
    public String getSrcAttribute(){return srcAttribute;}
    public String getTargetAttribute(){return targetAttribute;}
    public String getId(){return id;}
    public Double getWeight(){return weight;}
    public String getSrcGraphLabel(){return srcGraphLabel;}
    public String getTargetGraphLabel(){return targetGraphLabel;}
    public String getSrcLabel(){return srcLabel;}
    public String getTargetLabel(){return targetLabel;}    
}
