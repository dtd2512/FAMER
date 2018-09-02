package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util;

import org.gradoop.common.model.impl.id.GradoopId;
import scala.Int;

/**
 */
public class Msg2 {
    private String sources;
    private String senderClusterId;
    private String clusterId;
    public Msg2(String finalSources,  String sourceClusterId, String finalClusterId){
        sources = finalSources;
        senderClusterId = sourceClusterId;
        clusterId = finalClusterId;
    }
    public String getSources(){return sources;}
    public String getClusterId(){return clusterId;}
    public String getSenderClusterId(){return senderClusterId;}
}
