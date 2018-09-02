package org.gradoop.famer.linking.similarity_measuring.data_structures;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class SimilarityFieldList {
    private Collection<SimilarityField> similarityFields;
    public SimilarityFieldList(Collection<SimilarityField> SimilarityFields){similarityFields = SimilarityFields;}
    public SimilarityFieldList(){similarityFields = new ArrayList<>();}
    public SimilarityField getSimilarityField (String Id){
        for (SimilarityField similarityField: similarityFields){
            if (similarityField.getId().equals(Id))
                return similarityField;
        }
        return null;
    }
    public void add (SimilarityField SimilarityField){similarityFields.add(SimilarityField);}
    public Boolean isEmpty(){
        if (similarityFields.size() > 0)
            return false;
        return true;
    }
    public Collection<SimilarityField> getSimilarityFields(){
    	return similarityFields;
    }
}
