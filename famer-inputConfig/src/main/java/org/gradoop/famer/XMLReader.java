package org.gradoop.famer;

import org.gradoop.famer.phase.InitialGraphGeneratorPhase;
import org.gradoop.famer.phase.Phase;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class XMLReader {
    private String InputPath;
    public XMLReader (String inputPath){
        InputPath = inputPath;
    }
    public Collection<Phase> read() {
        Collection<Phase> phases = new ArrayList<>();
        try {
            File fXmlFile = new File(InputPath);
            Document doc  = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(fXmlFile);
            doc.getDocumentElement().normalize();
            NodeList phaseList = doc.getElementsByTagName("Phase");
            for (int i = 0; i < phaseList.getLength(); i++) {
                Element phaseContent = (Element) phaseList.item(i);
                phases.add(this.parsePhaseContent(phaseContent));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return phases;
    }


    private Phase parsePhaseContent(Element PhaseContent) {
        PhaseTitles phaseTitle = PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent());
        switch (phaseTitle) {
//            case InitialGraphGeneratorPhase:
//                InitialGraphGeneratorPhase initialGraphGeneratorPhase = new InitialGraphGeneratorPhase(PhaseContent);
//                return initialGraphGeneratorPhase;
        }
        return null;
    }
}
