package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 */
public class addVertexVis_Label implements MapFunction <Vertex, Vertex> {
    private Collection <Tuple3<String, Integer, Integer>> propertyList;
    public addVertexVis_Label (Collection <Tuple3<String, Integer, Integer>> PropertyList){ propertyList = PropertyList;}
    @Override
    public Vertex map(Vertex value) throws Exception {
        String vis_label = "";
        for (Tuple3<String, Integer, Integer> property:propertyList)
            vis_label += (" "+generateLabel (value, property));
        value.setProperty("vis_label", vis_label.substring(1));
        return value;
    }

    private String generateLabel (Vertex vertex, Tuple3<String, Integer, Integer> property){
        String label = "";
        if (property.f1 == -1 && property.f2 == -1)
            label = vertex.getPropertyValue(property.f0).toString();
        else if (property.f1 != -1 && property.f2 == -1)
            label = vertex.getPropertyValue(property.f0).toString().substring(property.f1);
        else if (property.f1 != -1 && property.f2 != -1 && property.f1 <= property.f2)
            label = vertex.getPropertyValue(property.f0).toString().substring(property.f1, property.f2);
        return label;
    }
}
