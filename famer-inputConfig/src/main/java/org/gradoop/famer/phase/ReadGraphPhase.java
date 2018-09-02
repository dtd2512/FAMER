package org.gradoop.famer.phase;

import org.gradoop.famer.PhaseTitles;
import org.w3c.dom.Element;

/**
 */
public class ReadGraphPhase extends Phase{
    private String inputPath;
    public ReadGraphPhase(Element PhaseContent) {
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        inputPath = PhaseContent.getElementsByTagName("InputPath").item(0).getTextContent();
    }
    public String getInputPath(){return inputPath;}
}
