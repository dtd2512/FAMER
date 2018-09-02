package org.gradoop.famer.clusterPostProcessing.clusterRepair.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class filterEdges implements FlatMapFunction <Edge, Edge>{
    private Integer edgeStatus;
    public filterEdges (Integer EdgeStatus){ edgeStatus = EdgeStatus;}
    @Override
    public void flatMap(Edge value, Collector<Edge> out) throws Exception {

        if (Integer.parseInt(value.getPropertyValue("isSelected").toString()) > edgeStatus)
            out.collect(value);
    }
}
