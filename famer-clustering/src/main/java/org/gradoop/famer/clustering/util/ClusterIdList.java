package org.gradoop.famer.clustering.util;

import java.util.Collection;

/**
 */
public class ClusterIdList {
    private Collection<String> clusterIdList;
    public Collection<String> getClusterIdList(){return  clusterIdList;}
    public void setClusterIdList(Collection<String> ClusterIdList){clusterIdList=ClusterIdList;}
    public boolean hasClusterId(String ClusterId){
        if (clusterIdList.contains(ClusterId))
                return true;
        return false;
    }
    public boolean removeClusterId (String ClusterId){
        if (clusterIdList.contains(ClusterId)){
            clusterIdList.remove(ClusterId);
            return true;
        }
        return false;
    }
    public void addClusterId(String ClusterId){
        clusterIdList.add(ClusterId);
    }

}
