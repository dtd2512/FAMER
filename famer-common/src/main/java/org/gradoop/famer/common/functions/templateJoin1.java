package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class templateJoin1<A,B> implements JoinFunction<Tuple2<A,B>, Tuple1<B>, A>{
    @Override
    public A join(Tuple2<A, B> first, Tuple1<B> second) throws Exception {
        return first.f0;
    }
}
