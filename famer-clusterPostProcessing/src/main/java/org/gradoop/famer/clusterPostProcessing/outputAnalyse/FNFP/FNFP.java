package org.gradoop.famer.clusterPostProcessing.outputAnalyse.FNFP;


/**
 *
 */
public class FNFP  {

    /*public FNFP(LogicalGraph ClusteredGraph){
        super(ClusteredGraph);
    }
    public long getWrongClusterNo () throws Exception {
        return -1;
    }
    public DataSet<String> getClusterIdsWithFP(DataSet<Tuple2<String, String>> GoldenTruth) throws Exception {
        DataSet<Tuple3<Vertex, Vertex, Double>> vertexPairs = new Edges2VertexPairs(clusteredGraphVertices, inputGraphEdges).getPairs();
        DataSet<Tuple2<String, String>> concatedRecIds_clusterIds =vertexPairs.flatMap(new FlatMapFunction<Tuple3<Vertex, Vertex, Double>, Tuple2<String, String>>() {
            @Override
            public void flatMap(Tuple3<Vertex, Vertex, Double> value, Collector<Tuple2<String, String>> out) throws Exception {
                String clusterId0 = value.f0.getPropertyValue("ClusterId").toString();
                String clusterId1 = value.f1.getPropertyValue("ClusterId").toString();
                if (clusterId0.equals(clusterId1)) {
                    String recId0 = value.f0.getPropertyValue("recId").toString();
                    String recId1 = value.f1.getPropertyValue("recId").toString();
                    if (recId0.compareTo(recId1)<0)
                        out.collect(Tuple2.of(recId0+","+recId1,clusterId1));
                    else
                        out.collect(Tuple2.of(recId1+","+recId0,clusterId1));
                }
            }
        }).distinct(0);
        DataSet<Tuple2<String,String>> gt = GoldenTruth.map(new sortAndConcatTuple2()).map(new RichMapFunction<Tuple1<String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple1<String> value) throws Exception {
                return Tuple2.of(value.f0,"gt");
            }
        });
        DataSet<String> clusterIds = concatedRecIds_clusterIds.union(gt).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, String>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> values, Collector<String> out) throws Exception {
                String clusterId = "";
                Boolean isFP = true;
                int cnt = 0;
                for (Tuple2<String, String> value:values){
                    cnt++;
                    if (value.f1.equals("gt"))
                        isFP = false;
                    else
                        clusterId = value.f1;
                }
                if (cnt < 2 && isFP)
                    out.collect(clusterId);
            }
        }).distinct();
        return clusterIds;
    }
    public DataSet<String> getClusterIdsWithFN(DataSet<Tuple2<String, String>> GoldenTruth) throws Exception {
        DataSet<Tuple3<Vertex, Vertex, Double>> vertexPairs = new Edges2VertexPairs(clusteredGraphVertices, inputGraphEdges).getPairs();
        DataSet<Tuple2<String, String>> concatedRecIds_clusterIds =vertexPairs.flatMap(new FlatMapFunction<Tuple3<Vertex, Vertex, Double>, Tuple2<String, String>>() {
            @Override
            public void flatMap(Tuple3<Vertex, Vertex, Double> value, Collector<Tuple2<String, String>> out) throws Exception {
                String clusterId0 = value.f0.getPropertyValue("ClusterId").toString();
                String clusterId1 = value.f1.getPropertyValue("ClusterId").toString();
                if (clusterId0.equals(clusterId1)) {
                    String recId0 = value.f0.getPropertyValue("recId").toString();
                    String recId1 = value.f1.getPropertyValue("recId").toString();
                    if (recId0.compareTo(recId1)<0)
                        out.collect(Tuple2.of(recId0+","+recId1,clusterId1));
                    else
                        out.collect(Tuple2.of(recId1+","+recId0,clusterId1));
                }
            }
        }).distinct(0);
        DataSet<Tuple2<String,String>> gt = GoldenTruth.map(new sortAndConcatTuple2()).map(new RichMapFunction<Tuple1<String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple1<String> value) throws Exception {
                return Tuple2.of(value.f0,"gt");
            }
        });
        DataSet<Tuple1<String>> recIds = concatedRecIds_clusterIds.union(gt).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple1<String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple1<String>> out) throws Exception {
                Boolean isFN = false;
                int cnt = 0;
                String recIds= "";
                for (Tuple2<String, String> value:values){
                    cnt++;
                    if (value.f1.equals("gt")) {
                        isFN = true;
                        recIds = value.f0;
                    }
                }
                if (cnt < 2 && isFN) {
                    out.collect(Tuple1.of(recIds.split(",")[0]));
                    out.collect(Tuple1.of(recIds.split(",")[1]));
                }
            }
        });
        DataSet<Tuple2<String, String>> recId_clusterId = clusteredGraphVertices.map(new RichMapFunction<Vertex, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Vertex value) throws Exception {
                return Tuple2.of(value.getPropertyValue("recId").toString(), value.getPropertyValue("ClusterId").toString());
            }
        });
        DataSet<String> clusterIds = recIds.join(recId_clusterId).where(0).equalTo(0).with(new JoinFunction<Tuple1<String>, Tuple2<String, String>, String>() {
            @Override
            public String join(Tuple1<String> first, Tuple2<String, String> second) throws Exception {
                return second.f1;
            }
        }).distinct();
        return clusterIds;
    }*/
}




























