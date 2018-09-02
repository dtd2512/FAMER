package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class addLinkVis_Label implements MapFunction <Edge, Edge>{
    private String labelProperty;
    public addLinkVis_Label(String LabelProperty){
        labelProperty = LabelProperty;
    }
    @Override
    public Edge map(Edge value) throws Exception {
        Double simDegree = Double.parseDouble(value.getPropertyValue(labelProperty).toString());
        value.setProperty("vis_label",(float) Math.round(simDegree * 100) / 100);
        return value;
    }
}
