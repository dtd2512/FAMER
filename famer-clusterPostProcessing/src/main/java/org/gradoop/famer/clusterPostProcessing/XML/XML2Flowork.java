package org.gradoop.famer.clusterPostProcessing.XML;

import org.gradoop.famer.clusterPostProcessing.XML.util.Phase;
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
 */
public class XML2Flowork {

    private String InputPath;
    public XML2Flowork (String inputPath){
        InputPath = inputPath;
    }
    public Collection<Phase> readXML() {
        Collection<Phase> phases = new ArrayList<>();
        try {
            File fXmlFile = new File(InputPath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
            doc.getDocumentElement().normalize();
            NodeList phaseList = doc.getElementsByTagName("Phase");
            for (int i = 0; i < phaseList.getLength(); i++) {
                Node phaseNode = phaseList.item(i);
                Element phaseContent = (Element) phaseNode;
                Phase phase = new Phase().parsePhase(phaseContent);
                phases.add(phase);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return phases;
    }
}