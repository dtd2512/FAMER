package org.gradoop.famer.clustering.util;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * filter transformation to filter out edges with a certain degree of similarity and less
 */
public class FilterEdges
        implements FilterFunction<Edge> {
    private double threshold;
    public FilterEdges (double in){
        threshold = in;
    }
    public boolean filter(Edge in) throws Exception {
        return Double.parseDouble(in.getPropertyValue("value").toString())>=threshold;
    }
}

