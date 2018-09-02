
package org.gradoop.famer.XMLInput;




import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Used for reading the XML file of a configuration file and converting all configurations to an object from Matching class
 */
public class XMLtoMatching {
//    private String InputPath;
//    public XMLtoMatching (String inputPath){
//        InputPath = inputPath;
//    }
//    public Collection<Matching> readXML() {
//        Collection<Matching> matchings = new ArrayList<Matching>();
//        try {
//            File fXmlFile = new File(InputPath);
//            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            Document doc = dBuilder.parse(fXmlFile);
//            doc.getDocumentElement().normalize();
//            NodeList runList = doc.getElementsByTagName("run");
//            for (int runNo = 0; runNo < runList.getLength(); runNo++) {
//                Collection<BlockingKeyGenerator> blockingKeyGeneratorList = new ArrayList<BlockingKeyGenerator>();
//                Node runNode = runList.item(runNo);
//                Element run = (Element) runNode;
//                String RunId = run.getElementsByTagName("RunId").item(0) != null ? run.getElementsByTagName("RunId").item(0).getTextContent() : "0";
//                boolean IsMatchedWithinDataset = run.getElementsByTagName("IntraDatasetComparison").item(0) != null ?Boolean.parseBoolean(run.getElementsByTagName("IntraDatasetComparison").item(0).getTextContent()):false;
//                String EdgeLable = run.getElementsByTagName("EdgeLable").item(0) != null ? run.getElementsByTagName("EdgeLable").item(0).getTextContent() : "value";
//                boolean BidirectionalOutput = run.getElementsByTagName("BidirectionalOutput").item(0) != null ? Boolean.parseBoolean(run.getElementsByTagName("BidirectionalOutput").item(0).getTextContent()) : false;
//                String SimilarityStrategy = run.getElementsByTagName("SimilarityStrategy").item(0) != null ? run.getElementsByTagName("SimilarityStrategy").item(0).getTextContent() : "2";
//                Double SimilarityThreshold = run.getElementsByTagName("SimilarityThreshold").item(0) != null ? Double.parseDouble(run.getElementsByTagName("SimilarityThreshold").item(0).getTextContent()) : 0;
//                String SrcType = run.getElementsByTagName("SrcType").item(0) != null ? run.getElementsByTagName("SrcType").item(0).getTextContent() : "";
//                if (run.getElementsByTagName("blockingkeys").item(0) != null) {
//                    Element blockingkeys = (Element) run.getElementsByTagName("blockingkeys").item(0);
//                    NodeList blockingkeyList = blockingkeys.getElementsByTagName("blockingkey");
//                    for (int blockingKeyNo = 0; blockingKeyNo < blockingkeyList.getLength(); blockingKeyNo++) {
//                        Element blockingKey = (Element) blockingkeyList.item(blockingKeyNo);
//                        String blkingMethod = blockingKey.getElementsByTagName("method").item(0).getTextContent();
//                        String blkingAttribute = blockingKey.getElementsByTagName("attribute").item(0).getTextContent();
//                        BlockingKeyGenerator blockingKeyGenerator = new BlockingKeyGenerator(blkingMethod);
//                        blockingKeyGenerator.setAttribute(blkingAttribute);
//                        if (blockingKey.getElementsByTagName("blockingKeyParameters").item(0) != null) {
//                            Element blockingKeyParameters = (Element) blockingKey.getElementsByTagName("blockingKeyParameters").item(0);
//                            boolean ignoreCase = blockingKeyParameters.getElementsByTagName("ignoreCase").item(0) != null ? Boolean.parseBoolean(blockingKeyParameters.getElementsByTagName("ignoreCase").item(0).getTextContent()) : true;
//                            String integerParam = blockingKeyParameters.getElementsByTagName("integerParam").item(0) != null ? blockingKeyParameters.getElementsByTagName("integerParam").item(0).getTextContent() : "1";
//                            String stringParam = blockingKeyParameters.getElementsByTagName("stringParam").item(0) != null ? blockingKeyParameters.getElementsByTagName("stringParam").item(0).getTextContent() : "";
//                            String doubleParam = blockingKeyParameters.getElementsByTagName("doubleParam").item(0) != null ? blockingKeyParameters.getElementsByTagName("doubleParam").item(0).getTextContent() : "0";
//                            blockingKeyGenerator.setIntegerParam(Integer.parseInt(integerParam));
//                            blockingKeyGenerator.setDoubleParam(Double.parseDouble(doubleParam));
//                            blockingKeyGenerator.setStringParam(stringParam);
//                        }
//                        blockingKeyGeneratorList.add(blockingKeyGenerator);
//                    }
//                }
//                Element blockingElement = (Element) run.getElementsByTagName("blocking").item(0);
//                String bMethod = blockingElement.getElementsByTagName("method").item(0) != null ? blockingElement.getElementsByTagName("method").item(0).getTextContent() : "1";
//                boolean IntraDatasetComparison = false;
//                BlockingComponent2 blocking = new BlockingComponent2(bMethod, IsMatchedWithinDataset);
//                if (blockingElement.getElementsByTagName("blockingParameters").item(0)!=null){
//                    Element blockingParams = (Element) blockingElement.getElementsByTagName("blockingParameters").item(0);
//                    String integerParam = blockingParams.getElementsByTagName("integerParam").item(0) != null ? blockingParams.getElementsByTagName("integerParam").item(0).getTextContent() : "0";
////                    IntraDatasetComparison = blockingParams.getElementsByTagName("IsMatchedWithinDataset").item(0) != null ? Boolean.parseBoolean(blockingParams.getElementsByTagName("IsMatchedWithinDataset").item(0).getTextContent()) : false;
//                    String doubleParam = blockingParams.getElementsByTagName("doubleParam").item(0) != null ? blockingParams.getElementsByTagName("doubleParam").item(0).getTextContent() : "0";
//                    String secondMethod = blockingParams.getElementsByTagName("secondMethod").item(0) != null ? blockingParams.getElementsByTagName("SSJSimilarityMethod").item(0).getTextContent() : "1";
//                    Boolean MultiPassBlocking = blockingParams.getElementsByTagName("MultiPassBlocking").item(0) != null ? Boolean.parseBoolean(blockingParams.getElementsByTagName("MultiPassBlocking").item(0).getTextContent()) : false;
////                    RunNo = blockingParams.getElementsByTagName("RunNo").item(0) != null ? Integer.parseInt(blockingParams.getElementsByTagName("RunNo").item(0).getTextContent()) : 1;
//                    blocking.setIntegerParam(Integer.parseInt(integerParam));
//                    blocking.setDoubleParam(Double.parseDouble(doubleParam));
////                    blocking.setIntraDatasetComparison(IntraDatasetComparison);
//                    blocking.setSecondMethod(secondMethod);
////                    blocking.setMultiPassBlocking(MultiPassBlocking);
//                }
//                Element similarities = (Element) run.getElementsByTagName("similarityComponents").item(0);
//                NodeList similarityList = similarities.getElementsByTagName("similarityComponent");
//                Collection<SimilarityComponent> SimilarityComponentList = new ArrayList<SimilarityComponent>();
//                for (int similarityNo = 0; similarityNo < similarityList.getLength(); similarityNo++) {
//                    Element similarityComponentElement = (Element) similarityList.item(similarityNo);
//                    String srcAttribute = similarityComponentElement.getElementsByTagName("srcAttribute").item(0).getTextContent();
//                    String targetAttribute = similarityComponentElement.getElementsByTagName("targetAttribute").item(0).getTextContent();
//
//                    String weight = similarityComponentElement.getElementsByTagName("weight").item(0)!=null?similarityComponentElement.getElementsByTagName("weight").item(0).getTextContent():"1";
//                    String threshold = similarityComponentElement.getElementsByTagName("threshold").item(0)!=null?similarityComponentElement.getElementsByTagName("threshold").item(0).getTextContent():"0";
//                    String method = similarityComponentElement.getElementsByTagName("method").item(0)!=null?similarityComponentElement.getElementsByTagName("method").item(0).getTextContent():"1";
//
//                    SimilarityComponent similarityComponent = new SimilarityComponent(srcAttribute, targetAttribute,Double.parseDouble(weight), Double.parseDouble(threshold), method);
//
//                    if(similarityComponentElement.getElementsByTagName("similarityComponentParameters").item(0)!=null) {
//                        Element similarityComponentParameters = (Element) similarityComponentElement.getElementsByTagName("similarityComponentParameters").item(0);
//                        String methodThreshold = similarityComponentParameters.getElementsByTagName("methodThreshold").item(0) != null ? similarityComponentParameters.getElementsByTagName("methodThreshold").item(0).getTextContent() : "0";
//                        String secondMethod = similarityComponentParameters.getElementsByTagName("secondMethod").item(0) != null ? similarityComponentParameters.getElementsByTagName("secondMethod").item(0).getTextContent() : "2";
//                        String secondMethodThreshold = similarityComponentParameters.getElementsByTagName("secondMethodThreshold").item(0) != null ? similarityComponentParameters.getElementsByTagName("secondMethodThreshold").item(0).getTextContent() : "0";
//                        String booleanParam = similarityComponentParameters.getElementsByTagName("booleanParam").item(0) != null ? similarityComponentParameters.getElementsByTagName("booleanParam").item(0).getTextContent() : "0";
//                        String integerParam = similarityComponentParameters.getElementsByTagName("integerParam").item(0) != null ? similarityComponentParameters.getElementsByTagName("integerParam").item(0).getTextContent() : "0";
//                        String stringParam = similarityComponentParameters.getElementsByTagName("stringParam").item(0) != null ? similarityComponentParameters.getElementsByTagName("stringParam").item(0).getTextContent() : "";
//                        String doubleParam = similarityComponentParameters.getElementsByTagName("doubleParam").item(0) != null ? similarityComponentParameters.getElementsByTagName("doubleParam").item(0).getTextContent() : "0";
//                        similarityComponent.setMethodThreshold(Double.parseDouble(methodThreshold));
//                        similarityComponent.setSecondMethodThreshold(Double.parseDouble(secondMethodThreshold));
//                        similarityComponent.setIntegerParam(Integer.parseInt(integerParam));
//                        similarityComponent.setBooleanParam(Boolean.parseBoolean(booleanParam));
//                        similarityComponent.setSecondMethod(secondMethod);
//                        similarityComponent.setStringParam(stringParam);
//                        similarityComponent.setDoubleParam(Double.parseDouble(doubleParam));
//                    }
//                    SimilarityComponentList.add(similarityComponent);
//                }
//                Matching matching = new Matching(blockingKeyGeneratorList, blocking, SimilarityComponentList, EdgeLable, BidirectionalOutput);
//                matching.setId(Integer.parseInt(RunId));
//                matching.setsimilarityThreshold(SimilarityThreshold);
//                matching.setSimilarityStrategy(SimilarityStrategy);
//                matching.setSrctype(SrcType);
//                matchings.add(matching);
//            }
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//        return matchings;

//    }
}
