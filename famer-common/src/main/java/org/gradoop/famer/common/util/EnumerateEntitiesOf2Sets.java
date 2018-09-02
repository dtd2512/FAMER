package org.gradoop.famer.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.famer.common.util.functions.addLabel;
import org.gradoop.famer.common.util.functions.enumerate;

/**
 */
public class EnumerateEntitiesOf2Sets<T> {
    private DataSet<T> first;
    private DataSet<T> second;
    public EnumerateEntitiesOf2Sets (DataSet<T> First, DataSet<T> Second){
        first = First;
        second = Second;
    }
    public DataSet<Tuple3<T, Integer,Integer>> execute(){
        DataSet<Tuple2<T, String>> firstWithLabel = first.map(new addLabel("first"));
        DataSet<Tuple2<T, String>> secondWithLabel = second.map(new addLabel("second"));
        return firstWithLabel.union(secondWithLabel).groupBy(0).reduceGroup(new enumerate("first","second"));
    }
}
