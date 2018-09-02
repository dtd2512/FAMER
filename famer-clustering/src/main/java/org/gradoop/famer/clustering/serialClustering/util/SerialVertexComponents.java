package org.gradoop.famer.clustering.serialClustering.util;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Vertex structure for serial org.gradoop.famer.clustering.clustering algorithms
 */
public class SerialVertexComponents {
    private Long vertexPrio;
    private Long clusterId;
    private Boolean iscenter;
    private Boolean isAssigned;
    private Integer degree;
    private Double simDegrees;
    private String clusterIds;
    private Collection<String> neighborList;
    private String id;

    public SerialVertexComponents (Long vp, Long cid, Boolean isC){
        vertexPrio = vp;
        clusterId = cid;
        iscenter = isC;
        isAssigned = false;
        degree = 0;
        simDegrees = 0.0;
        clusterIds = "";
        neighborList = new ArrayList<String>();
    }
    public Long getVertexPrio(){return vertexPrio;}
    public Long getClusterId(){return clusterId;}
    public boolean getIsCenter(){return iscenter;}
    public boolean getIsAssigned(){return isAssigned;}
    public void setVertexPrio(Long vrtxprio){ vertexPrio = vrtxprio;}
    public void setClusterId(Long clstrid){clusterId = clstrid;}
    public void setIsCenter(boolean iscntr){ iscenter = iscntr;}
    public void setIsAssigned(boolean isassgnd) {isAssigned = isassgnd;}
    public void AddDegree(Integer in){degree +=in;}
    public void SetDegree(Integer in){degree =in;}
    public Integer getDegree(){return degree;}
    public void addToNeighborList (String in){
        neighborList.add(in);
    }
    public Collection<String> getNeighborList(){
        return neighborList;
    }
    public String getClusterIds(){return clusterIds;}
    public void setClusterIds(String clstrid){
        if (clusterIds.equals(""))
            clusterIds = clstrid;
        else
            clusterIds += ","+clstrid;
    }
    public void setId (String in){ id = in;}
    public String getId(){return id;}
    public void AddSimDegree(Double in){simDegrees +=in;}
    public void SetSimDegree(Double in){simDegrees =in;}
    public Double getSimDegree(){return simDegrees;}

}
