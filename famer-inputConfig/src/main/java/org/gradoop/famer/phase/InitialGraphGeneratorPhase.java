package org.gradoop.famer.phase;

import org.gradoop.famer.PhaseTitles;
import org.w3c.dom.Element;

/**
 */
public class InitialGraphGeneratorPhase extends Phase{
    private String inputPath;
    private String DSName;
    private String graphLabel;

    public InitialGraphGeneratorPhase (Element PhaseContent) {
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        inputPath = PhaseContent.getElementsByTagName("InputPath").item(0).getTextContent();
        DSName = PhaseContent.getElementsByTagName("DSName").item(0).getTextContent();
        graphLabel = PhaseContent.getElementsByTagName("GraphLabel").item(0).getTextContent();
    }
    public String getInputPath(){return inputPath;}
    public String getDSName(){return DSName;}
    public String getGraphLabel(){return graphLabel;}
}
