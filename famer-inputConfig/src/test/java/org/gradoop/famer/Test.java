package org.gradoop.famer;

import org.gradoop.famer.linking.linking.Linker;
import org.gradoop.famer.phase.InitialGraphGeneratorPhase;
import org.gradoop.famer.phase.Phase;
import org.gradoop.famer.phase.linking.LinkingPhase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Collection;

/**
 */
public class Test {
    public static void main(String args[]) throws Exception {
        Collection<Phase> phases = new XMLReader(args[0]).read();
        new Test().execute(phases);
    }
    public void execute(Collection<Phase> phases) throws Exception {
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment());
        GraphCollection graphCollection = null;
        for (Phase phase :phases) {
//            switch (phase.getPhaseTitle()){
//                case InitialGraphGeneratorPhase:
//                    InitialGraphGeneratorPhase iggp = (InitialGraphGeneratorPhase)phase;
//                    LogicalGraph lg = new InitialGraphGeneration(iggp.getInputPath(), iggp.getDSName(), iggp.getGraphLabel(),  config).generateGraph();
//                    break;
//            }
            switch (phase.getPhaseTitle()){
                case LINKING:
//                    LinkingPhase linkingPhase = (LinkingPhase) phase;
                    graphCollection = graphCollection.callForCollection(new Linker(((LinkingPhase)phase).toLinkerComponet()));

                    break;
            }
        }
    }
}
