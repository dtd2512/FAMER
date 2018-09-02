package org.gradoop.famer.clusterPostProcessing.clusterRepair.MultiEntity1SrcResolve.util;

import org.gradoop.common.model.impl.id.GradoopId;
import scala.Int;

/**
 */
public class Msg {
    private String content;
    private Integer priority;
//    private GradoopId sender;
//    private String clusterId;
//    public Msg(String sourceSources, Integer linkPriority, GradoopId senderId, String senderClusterId){
//        sources = sourceSources;
//        priority = linkPriority;
//        sender = senderId;
//        clusterId = senderClusterId;
//    }
    public Msg(){
        content = "";
        priority = -1;
    }
    public Msg(String msgContent, Integer linkPriority){
        content = msgContent;
        priority = linkPriority;
    }
    public String getContent(){return content;}
    public Integer getPriority(){return priority;}
    public boolean isFake(){
        if (priority == -1)
            return true;
        return false;
    }

//    public String getSources(){return sources;}
//    public Integer getPriority(){return priority;}
//    public GradoopId getSenderId(){return sender;}
//    public String getClusterId(){return clusterId;}
//
//
//    public void setPriority (Integer linkPriority){priority = linkPriority;}
//    public void setSender (GradoopId senderId){sender = senderId;}

    //----------------------------------------------------
//    private String finalSources;
//    private String finalClusterId;
//    private String senderClusterId;
//    public Msg(String sources,  String clusterId, String srcClusterId){
//        finalSources = sources;
//        finalClusterId = clusterId;
//        senderClusterId = srcClusterId;
//    }
//    public String getFinalSources(){return finalSources;}
//    public String getFinalClusterId(){return finalClusterId;}
//    public String getSenderClusterId(){return senderClusterId;}

}
