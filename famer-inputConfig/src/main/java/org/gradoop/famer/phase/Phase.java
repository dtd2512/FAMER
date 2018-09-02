package org.gradoop.famer.phase;

import org.gradoop.famer.PhaseTitles;
import org.w3c.dom.Element;

/**
 */
public class Phase {
    protected PhaseTitles phaseTitle;

    public Phase(PhaseTitles PhaseTitle){
        phaseTitle = PhaseTitle;
    }
    public PhaseTitles getPhaseTitle(){
        return phaseTitle;
    }
}
