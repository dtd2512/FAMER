package org.gradoop.famer.clustering.serialClustering.util;

/**
 * Edge Structure for serial org.gradoop.famer.clustering.clustering algorithms
 */
public class SerialEdgeComponents {
    private String srcId;
    private String targtId;
    private Double degree;
    public SerialEdgeComponents (String sid, String tid, Double dgr){
        srcId = sid;
        targtId = tid;
        degree = dgr;
    }
    public double getDegree(){return degree;}
    public String getSrcId(){return srcId;}
    public String getTargtId(){return targtId;}
}
