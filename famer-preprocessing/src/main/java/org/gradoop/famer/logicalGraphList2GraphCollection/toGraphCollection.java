package org.gradoop.famer.logicalGraphList2GraphCollection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Iterator;

/**
 */
public class toGraphCollection {
    public GraphCollection execute(Collection<LogicalGraph> input)  {
        GraphCollection graphCollection = null;
        for (LogicalGraph logicalGraph : input){
            GraphCollection temp = logicalGraph.getConfig().getGraphCollectionFactory().fromGraph(logicalGraph);
            if (graphCollection == null)
                graphCollection = temp;
            else
                graphCollection = graphCollection.union(temp);
        }
        return graphCollection;
    }
}
