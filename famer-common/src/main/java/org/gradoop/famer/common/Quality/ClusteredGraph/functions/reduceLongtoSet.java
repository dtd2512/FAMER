package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by alieh on 2/10/17.
 */
public class reduceLongtoSet implements ReduceFunction<Long> {
    public Long reduce(Long value1, Long value2) throws Exception {
        return value1 + value2;
    }
}

