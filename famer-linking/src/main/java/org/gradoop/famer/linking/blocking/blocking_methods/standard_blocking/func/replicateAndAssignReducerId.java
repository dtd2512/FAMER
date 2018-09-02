package org.gradoop.famer.linking.blocking.blocking_methods.standard_blocking.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

//input: Tuple6<Vertex, String, Long, Long, Long, Long> ~ Tuple6<Vertex, Key, VIndex, BlkSize, PrevPairs, allPairs>
// output: Tuple5<Vertex, String, Long, Boolean, Integer> ~ Tuple4<Vertex, Key, VertexIndex, IsLast, ReducerId>

//@FunctionAnnotation.ForwardedFields({"f0","f1","f2"})
public class replicateAndAssignReducerId implements FlatMapFunction<Tuple6<Vertex, String, Long, Long, Long, Long>, Tuple5<Vertex, String, Long, Boolean, Integer>> {
    private Integer reducerNo;
    public replicateAndAssignReducerId (Integer ReducerNo){
        reducerNo = ReducerNo;
    }

    public void flatMap(Tuple6<Vertex, String, Long, Long, Long, Long> in, Collector<Tuple5<Vertex, String, Long, Boolean, Integer>> out){
        //p (x,y) = x/2 (2 * blkSize -x-3) + y-1 + prvBlkSize
        long xmin, ymin, xmax, ymax;
        xmin = 0;
        if (in.f2 == 0){
            xmax=0;
            ymin=1;
            ymax=in.f3-1;
        }
        else if (in.f2 == in.f3-1){
            xmax=in.f2-1;
            ymin=in.f3-1;
            ymax=in.f3-1;
        }
        else {
            xmax=in.f2;
            ymin=in.f2;
            ymax=in.f3-1;
        }
        Long PMin, PMax;
        if (xmin%2==0)
            PMin = ((xmin/2)*(2 * in.f3 -xmin-3)) + ymin-1 + in.f4;
        else
            PMin = ((xmin)*((2 * in.f3 -xmin-3)/2)) + ymin-1 + in.f4;
        if (xmax%2==0)
            PMax = ((xmax/2)*(2 * in.f3 -xmax-3)) + ymax-1 + in.f4;
        else
            PMax = ((xmax)*((2 * in.f3 -xmax-3)/2)) + ymax-1 + in.f4;

        Integer MinReducerId = Math.toIntExact(reducerNo * PMin / in.f5);
        Integer MaxReducerId = Math.toIntExact(reducerNo * PMax / in.f5);


//        System.out.println(in.f2+","+in.f3+","+reducerNo * PMin / in.f5+","+reducerNo * PMax / in.f5);
        if (MinReducerId != MaxReducerId) {
            for (int i= MinReducerId; i < MaxReducerId; i++){
                out.collect(Tuple5.of(in.f0, in.f1, in.f2, false, i));
            }        	
        }
        
        // check needed to prevent error when no vertex matching pairs have been found 
        if(in.f5.intValue() != 1) {
        	out.collect(Tuple5.of(in.f0, in.f1, in.f2, true, MaxReducerId));
        }
    }
}
