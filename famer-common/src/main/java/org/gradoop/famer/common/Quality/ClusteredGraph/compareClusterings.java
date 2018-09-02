package org.gradoop.famer.common.Quality.ClusteredGraph;


import org.apache.flink.api.java.DataSet;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 */
public class compareClusterings {
    private Long difference;
    LogicalGraph clusteredGraph1;
    LogicalGraph clusteredGraph2;
    public  compareClusterings(LogicalGraph ClusteredGraph1, LogicalGraph ClusteredGraph2){
        difference = -1l;
        clusteredGraph1 = ClusteredGraph1;
        clusteredGraph2 = ClusteredGraph2;
    }
    public void compare() throws Exception {
        ClusterCollection clusterCollection1 = new ClusterCollection(clusteredGraph1);
        ClusterCollection clusterCollection2 = new ClusterCollection(clusteredGraph2);
        DataSet<Long> resultDataset = clusterCollection1.compareClusterCollections(clusterCollection2);
        difference = resultDataset.collect().get(0);
    }
    public Long getDifference () throws Exception {
        if (difference == -1)
            compare();
        return difference;
    }
}
