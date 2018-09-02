package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.dataStructures;

/**
 */
public class ClusterID {
    private String id;
    private Integer size;
    public ClusterID (String clusterId, Integer clusterSize){
        id = clusterId;
        size = clusterSize;
    }
    public Integer getSize(){return size;}
    public String getId(){return  id;}
}
