package org.gradoop.famer.linking.linking.data_structures;

import org.gradoop.famer.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.gradoop.famer.linking.selection.data_structures.SelectionComponent;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent;

import java.io.Serializable;
import java.util.Collection;

/**
 */
public class LinkerComponent implements Serializable{
    private Collection<BlockingComponent> blockingComponents;
    private Collection<SimilarityComponent> similarityComponents;
    private SelectionComponent selectionComponent;
    private Boolean keepCurrentEdges;
    private Boolean recomputeSimilarityForCurrentEdges;
    private String edgeLabel;
    public LinkerComponent(Collection<BlockingComponent> BlockingComponents, Collection<SimilarityComponent> SimilarityComponents,
                           SelectionComponent SelectionComponent, Boolean KeepCurrentEdges, Boolean RecomputeSimilarityForCurrentEdges,
                           String EdgeLabel){
        blockingComponents = BlockingComponents;
        similarityComponents = SimilarityComponents;
        selectionComponent = SelectionComponent;
        keepCurrentEdges = KeepCurrentEdges;
        recomputeSimilarityForCurrentEdges = RecomputeSimilarityForCurrentEdges;
        edgeLabel = EdgeLabel;
    }
    public Collection<BlockingComponent> getBlockingComponents(){return blockingComponents;}
    public Collection<SimilarityComponent> getSimilarityComponents(){return similarityComponents;}
    public SelectionComponent getSelectionComponent(){return selectionComponent;}
    public Boolean getKeepCurrentEdges(){return keepCurrentEdges;}
    public Boolean getRecomputeSimilarityForCurrentEdges(){return recomputeSimilarityForCurrentEdges;}
    public String getEdgeLabel(){return edgeLabel;}
}
