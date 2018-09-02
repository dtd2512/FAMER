package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class minus {
    private DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> firstList;
    private DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> secondList;
    public minus(DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> first, DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> second){
        firstList = first;
        secondList = second;
    }
    public DataSet<Tuple6<Vertex, String, Vertex, String, Double, String>> execute (){
        return firstList.union(secondList).groupBy(1,5).reduceGroup(new reducer());
    }

    private class reducer implements GroupReduceFunction<Tuple6<Vertex, String, Vertex, String, Double, String>, Tuple6<Vertex, String, Vertex, String, Double, String>> {
        Tuple6<Vertex, String, Vertex, String, Double, String> out = null;
        int cnt = 0;
        @Override
        public void reduce(Iterable<Tuple6<Vertex, String, Vertex, String, Double, String>> input, Collector<Tuple6<Vertex, String, Vertex, String, Double, String>> output) throws Exception {
            for(Tuple6<Vertex, String, Vertex, String, Double, String> in:input){
                cnt++;
                out = in;
            }
            if (cnt == 1)
                output.collect(out);
        }
    }
}
