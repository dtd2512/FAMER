package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.dataStructures;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class ClusterIDList {
    private Collection<ClusterID> list;
    public ClusterIDList(){list = new ArrayList<>();}
    public void add(ClusterID clusterID){list.add(clusterID);}
}
