package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.JoinFunction;

/**
 */
public class joinGetSecond<T1,T2> implements JoinFunction <T1, T2, T2> {
    @Override
    public T2 join(T1 first, T2 second) throws Exception {
        return second;
    }
}
