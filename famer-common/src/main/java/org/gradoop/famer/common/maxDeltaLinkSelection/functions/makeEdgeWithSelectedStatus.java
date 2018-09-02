package org.gradoop.famer.common.maxDeltaLinkSelection.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class makeEdgeWithSelectedStatus implements GroupReduceFunction <Tuple3<Edge, String, Integer>, Edge>{

    @Override
    public void reduce(Iterable<Tuple3<Edge, String, Integer>> values, Collector<Edge> out) throws Exception {
        boolean isSrcSelected = false;
        boolean isTrgtSelected = false;
        Edge e = new Edge();
        for (Tuple3<Edge, String, Integer> value: values){
            e = value.f0;
            if (value.f2 == 1)
                isSrcSelected = true;
            else if (value.f2 == 2)
                isTrgtSelected = true;
        }
        if (isSrcSelected && isTrgtSelected)
            e.setProperty("isSelected", 2);
        else if (isSrcSelected || isTrgtSelected)
            e.setProperty("isSelected", 1);
        else
            e.setProperty("isSelected", 0);
        out.collect(e);
    }
}
