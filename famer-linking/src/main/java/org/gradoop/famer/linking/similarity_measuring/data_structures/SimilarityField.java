package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public class SimilarityField {
    
	private String id;
    private Double similarityValue;
    private Double weight;
    
    public SimilarityField(){}
    public SimilarityField(String id){ this.id = id; }
    public SimilarityField (String id, Double similarityValue, Double weight){
        this.id = id;
        this.similarityValue = similarityValue;
        this.weight = weight;
    }
    
    public void setSimilarityValue(Double similarityValue){ this.similarityValue = similarityValue; }
    public Double getSimilarityValue(){ return similarityValue; }
    public String getId(){ return id; }
    public Double getWeight() { return weight; }
}