package org.gradoop.famer.clusterPostProcessing.XML.util;


import org.apache.flink.api.java.tuple.Tuple3;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;


/**
 *
 */
public class Phase {
    public enum PhaseTitle {
        ReadGraph, PrepareForGraphVis, Clustering, Evaluation, Diagnose, OverlapResolve, MultiEntityPerSourceResolve, RemoveIntraClustersLinks, CompareClusters, WriteGraph, FilterOutLinks, WriteInEvalFile, NewLine;
    };
    public enum EvaluationType {
        InputGraph, ClusteredGraph;
    };
    private PhaseTitle phaseName;
    private String graphPath;
    private String graphHeadFileName;
    private String graphVerticesFileName;
    private String graphEdgesFileName;
    private String graphPath2;
    private String graphHeadFileName2;
    private String graphVerticesFileName2;
    private String graphEdgesFileName2;
    private String clusteringMethod;
    private Boolean isEdgeBidirection;
    private String perfectMatchFilePath;
    private String evaluationOutputFilePath;
    private EvaluationType evaluationType;
    private String evaluationRecordComment;
    private Boolean hasOverlap;
    private Integer parallelismDegree;
    private String linkValidationPropertyTitle;
    private Collection<Tuple3<String, Integer, Integer>> vertexVis_LabelPropertyCollection;
    private String diagnoseFileOutputPath;
    private String vertexIdLabel;
    private String diagnoseStatisticsFileOutputPath;
    private String diagnoseType;
    private Double delta;
    private Integer resolvePhase;
    private String clusteringOutputType;
    private Integer edgeType;
    private Integer sourceNo;
    private String content;



    public PhaseTitle getPhaseTitle() {return phaseName;}
    public String getGraphPath(){return graphPath;}
    public String getGraphHeadFileName(){return graphHeadFileName;}
    public String getGraphVerticesFileName(){return graphVerticesFileName;}
    public String getGraphEdgesFileName(){return graphEdgesFileName;}
    public String getGraphPath2(){return graphPath2;}
    public String getGraphHeadFileName2(){return graphHeadFileName2;}
    public String getGraphVerticesFileName2(){return graphVerticesFileName2;}
    public String getGraphEdgesFileName2(){return graphEdgesFileName2;}
    public String getClusteringMethod(){return clusteringMethod;}
    public Boolean getIsEdgeBidirection(){return isEdgeBidirection;}
    public String getPerfectMatchFilePath(){return perfectMatchFilePath;}
    public String getEvaluationOutputFilePath(){return evaluationOutputFilePath;}
    public EvaluationType getEvaluationType(){return evaluationType;}
    public Boolean getHasOverlap(){return hasOverlap;}
    public Integer getParallelismDegree(){return parallelismDegree;}
    public String getEvaluationRecordComment(){return evaluationRecordComment;}
    public String getLinkValidationPropertyTitle(){return linkValidationPropertyTitle;}
    public Collection<Tuple3<String, Integer, Integer>> getVertexVis_LabelPropertCollection (){return vertexVis_LabelPropertyCollection;}
    public String getDiagnoseFileOutputPath(){return diagnoseFileOutputPath;}
    public String getVertexIdLabel(){return vertexIdLabel;}
    public String getDiagnoseStatisticsFileOutputPath () {return diagnoseStatisticsFileOutputPath;}
    public String getDiagnoseType(){return diagnoseType;}
    public Double getDelta(){return delta;}
    public String getClusteringOutputType () {return clusteringOutputType;}
    public Integer getEdgeType (){return  edgeType;}
    public Integer getResolvePhase () {return resolvePhase;}
    public Integer getSourceNo(){return sourceNo;}
    public String getContent(){return content;}




    public Phase parsePhase (Element phaseContent) throws IOException, SAXException, ParserConfigurationException {
        phaseName = PhaseTitle.valueOf(phaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent());
        switch (phaseName){
            case ReadGraph:
                initializeInputGraphProperties(phaseContent);
                break;
            case PrepareForGraphVis:
                initializeInputGraphProperties(phaseContent);
                perfectMatchFilePath = phaseContent.getElementsByTagName("PerfectMatchFilePath").item(0) != null ? phaseContent.getElementsByTagName("PerfectMatchFilePath").item(0).getTextContent() : "";
                linkValidationPropertyTitle = phaseContent.getElementsByTagName("LinkValidationPropertyTitle").item(0).getTextContent();
                vertexVis_LabelPropertyCollection = new ArrayList<>();
                NodeList VertexVis_LabelProperties = phaseContent.getElementsByTagName("VertexVis_LabelProperties");
                NodeList Property = ((Element)VertexVis_LabelProperties.item(0)).getElementsByTagName("Property");
                for (int i=0; i< Property.getLength(); i++) {
                    Element CurrentProperty = ((Element) Property.item(i));
                    String PropertyTitle = CurrentProperty.getElementsByTagName("PropertyTitle").item(0).getTextContent();
                    Integer StartIndex = CurrentProperty.getElementsByTagName("StartIndex").item(0) != null ? Integer.parseInt(CurrentProperty.getElementsByTagName("StartIndex").item(0).getTextContent()) : -1;
                    Integer EndIndex = CurrentProperty.getElementsByTagName("EndIndex").item(0) != null ? Integer.parseInt(CurrentProperty.getElementsByTagName("EndIndex").item(0).getTextContent()) : -1;
                    vertexVis_LabelPropertyCollection.add(Tuple3.of(PropertyTitle, StartIndex, EndIndex));
                }
                delta = phaseContent.getElementsByTagName("Delta").item(0) != null ? Double.parseDouble(phaseContent.getElementsByTagName("Delta").item(0).getTextContent()) : 0;
                break;
            case Clustering:
                initializeInputGraphProperties(phaseContent);
                clusteringMethod = phaseContent.getElementsByTagName("ClusteringMethod").item(0).getTextContent();
                isEdgeBidirection = phaseContent.getElementsByTagName("IsEdgeBidirection").item(0) != null ? Boolean.parseBoolean(phaseContent.getElementsByTagName("IsEdgeBidirection").item(0).getTextContent()) : false;
                clusteringOutputType = phaseContent.getElementsByTagName("ClusteringOutputType").item(0) != null ? phaseContent.getElementsByTagName("ClusteringOutputType").item(0).getTextContent() : "Graph";

                break;
            case Evaluation:
                initializeInputGraphProperties(phaseContent);
                perfectMatchFilePath = phaseContent.getElementsByTagName("PerfectMatchFilePath").item(0) != null ? phaseContent.getElementsByTagName("PerfectMatchFilePath").item(0).getTextContent() : "";
                evaluationType = phaseContent.getElementsByTagName("EvaluationType").item(0) != null ? EvaluationType.valueOf(phaseContent.getElementsByTagName("EvaluationType").item(0).getTextContent()) : EvaluationType.InputGraph;
                evaluationOutputFilePath = phaseContent.getElementsByTagName("EvaluationOutputFilePath").item(0).getTextContent();
                hasOverlap = phaseContent.getElementsByTagName("HasOverlap").item(0) != null ? Boolean.parseBoolean(phaseContent.getElementsByTagName("HasOverlap").item(0).getTextContent()) : false;
                evaluationRecordComment = phaseContent.getElementsByTagName("EvaluationRecordComment").item(0) != null ? phaseContent.getElementsByTagName("EvaluationRecordComment").item(0).getTextContent() : "";
                vertexIdLabel = phaseContent.getElementsByTagName("VertexIdLabel").item(0) != null ? phaseContent.getElementsByTagName("VertexIdLabel").item(0).getTextContent() : "recId";
                sourceNo = phaseContent.getElementsByTagName("SourceNo").item(0) != null ? Integer.parseInt(phaseContent.getElementsByTagName("SourceNo").item(0).getTextContent()): 0;                break;
            case Diagnose:
                initializeInputGraphProperties(phaseContent);
                diagnoseType = phaseContent.getElementsByTagName("DiagnoseType").item(0) != null ? phaseContent.getElementsByTagName("DiagnoseType").item(0).getTextContent() : "2";
                diagnoseFileOutputPath = phaseContent.getElementsByTagName("DiagnoseFileOutputPath").item(0) != null ? phaseContent.getElementsByTagName("DiagnoseFileOutputPath").item(0).getTextContent() : "";
                diagnoseStatisticsFileOutputPath = phaseContent.getElementsByTagName("DiagnoseStatisticsFileOutputPath").item(0) != null ? phaseContent.getElementsByTagName("DiagnoseStatisticsFileOutputPath").item(0).getTextContent() : "";
                break;
            case OverlapResolve:
                initializeInputGraphProperties(phaseContent);
                break;
            case MultiEntityPerSourceResolve:
                initializeInputGraphProperties(phaseContent);
                delta = phaseContent.getElementsByTagName("Delta").item(0) != null ? Double.parseDouble(phaseContent.getElementsByTagName("Delta").item(0).getTextContent()) : 0;
                resolvePhase = phaseContent.getElementsByTagName("ResolvePhase").item(0) != null ? Integer.parseInt(phaseContent.getElementsByTagName("ResolvePhase").item(0).getTextContent()) : 0;
                sourceNo = Integer.parseInt(phaseContent.getElementsByTagName("SourceNo").item(0).getTextContent());
                break;
            case RemoveIntraClustersLinks:
                initializeInputGraphProperties(phaseContent);
                break;
            case WriteGraph:
                initializeInputGraphProperties(phaseContent);
                parallelismDegree = phaseContent.getElementsByTagName("ParallelismDegree").item(0) != null ? Integer.parseInt(phaseContent.getElementsByTagName("ParallelismDegree").item(0).getTextContent()) : 0;
                break;
            case CompareClusters:
                initializeInputGraphProperties(phaseContent);
                initializeSecondInputGraphProperties(phaseContent);
                evaluationOutputFilePath = phaseContent.getElementsByTagName("EvaluationOutputFilePath").item(0).getTextContent();
                break;
            case FilterOutLinks:
                initializeInputGraphProperties(phaseContent);
                edgeType = phaseContent.getElementsByTagName("EdgeType").item(0) != null ? Integer.parseInt(phaseContent.getElementsByTagName("EdgeType").item(0).getTextContent()) : 0;
                delta = phaseContent.getElementsByTagName("Delta").item(0) != null ? Double.parseDouble(phaseContent.getElementsByTagName("Delta").item(0).getTextContent()) : 0;
                break;
            case WriteInEvalFile:
                evaluationOutputFilePath = phaseContent.getElementsByTagName("EvaluationOutputFilePath").item(0).getTextContent();
                content = phaseContent.getElementsByTagName("Content").item(0) != null ? phaseContent.getElementsByTagName("Content").item(0).getTextContent() : "";
                break;
            case NewLine:
                evaluationOutputFilePath = phaseContent.getElementsByTagName("EvaluationOutputFilePath").item(0).getTextContent();
                break;
        }
        return this;
    }
    private void initializeInputGraphProperties (Element phaseContent){
        if (phaseContent.getElementsByTagName("GraphPath").item(0) !=null) {
            graphPath = phaseContent.getElementsByTagName("GraphPath").item(0).getTextContent();
            graphHeadFileName = phaseContent.getElementsByTagName("GraphHeadFileName").item(0) != null ? phaseContent.getElementsByTagName("GraphHeadFileName").item(0).getTextContent() : "graphHeads";
            graphVerticesFileName = phaseContent.getElementsByTagName("GraphVerticesFileName").item(0) != null ? phaseContent.getElementsByTagName("GraphVerticesFileName").item(0).getTextContent() : "vertices";
            graphEdgesFileName = phaseContent.getElementsByTagName("GraphEdgesFileName").item(0) != null ? phaseContent.getElementsByTagName("GraphEdgesFileName").item(0).getTextContent() : "edges";
        }
        else
            graphPath =  graphHeadFileName = graphVerticesFileName = graphEdgesFileName = "";
    }


    private void initializeSecondInputGraphProperties (Element phaseContent){
            graphPath2 = phaseContent.getElementsByTagName("GraphPath2").item(0).getTextContent();
            graphHeadFileName2 = phaseContent.getElementsByTagName("GraphHeadFileName2").item(0) != null ? phaseContent.getElementsByTagName("GraphHeadFileName2").item(0).getTextContent() : "graphHeads";
            graphVerticesFileName2 = phaseContent.getElementsByTagName("GraphVerticesFileName2").item(0) != null ? phaseContent.getElementsByTagName("GraphVerticesFileName2").item(0).getTextContent() : "vertices";
            graphEdgesFileName2 = phaseContent.getElementsByTagName("GraphEdgesFileName2").item(0) != null ? phaseContent.getElementsByTagName("GraphEdgesFileName2").item(0).getTextContent() : "edges";

    }
}
