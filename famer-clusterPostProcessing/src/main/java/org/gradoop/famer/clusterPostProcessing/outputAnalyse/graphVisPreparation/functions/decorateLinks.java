package org.gradoop.famer.clusterPostProcessing.outputAnalyse.graphVisPreparation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class decorateLinks implements MapFunction <Edge, Edge>{
    @Override
    public Edge map(Edge value) throws Exception {
        if (Boolean.parseBoolean(value.getPropertyValue("IsCorrect").toString()))
            value.setProperty("Color", "green");
        else
            value.setProperty("Color", "red");


        if (Integer.parseInt(value.getPropertyValue("isSelected").toString()) == 0)
            value.setProperty("Style", "dashed");
        else if (Integer.parseInt(value.getPropertyValue("isSelected").toString()) == 1)
            value.setProperty("Style", "solid");
        else if (Integer.parseInt(value.getPropertyValue("isSelected").toString()) == 2)
            value.setProperty("Style", "bold");

        return value;
    }
}
