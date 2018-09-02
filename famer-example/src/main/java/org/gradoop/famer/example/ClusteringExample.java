package org.gradoop.famer.example;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clustering.clustering;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.*;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ModifyGraphforClustering;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ClusteringExample {

	public enum ClusteringMethods {
		CONCON, CORRELATION_CLUSTERING, CENTER, MERGE_CENTER, STAR1, STAR2
	};

	public static void main(String args[]) throws Exception {
		ClusteringMethods method = ClusteringMethods.CORRELATION_CLUSTERING;
		new ClusteringExample().execute(method);
	}

	public void execute(ClusteringMethods method) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		//env.setParallelism(1); // should be changed
		GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig((ExecutionEnvironment) env);

		boolean isEdgeBirection = false;
		clustering.ClusteringOutputType clusteringOutputType = clustering.ClusteringOutputType.GraphCollection;
		String srcFolder = "/data/DBLP_GS/SimilarityGraph/default/";
		String outFolder = "/data/DBLP_GS/ClusteredGraph";


		//String srcFolder = "data/X-Y/SimilarityGraph/";
		//String outFolder = "data/X-Y/ClusteredGraph/";

		// String outFolder = "data/DBLP_GS/SimilarityGraph/";
		// String strategyFolder = "data/DBLP_GS/";

		/***************************
		 * Load Similarity Graph
		 ********************************/
		/***************************************************************************/
		/***************************************************************************/

		JSONDataSource dataSource = new JSONDataSource(srcFolder + "graphHeads.json", srcFolder + "vertices.json", srcFolder + "edges.json", grad_config);
		LogicalGraph input = dataSource.getLogicalGraph();
		// System.out.println("\n**************************** Number of Nodes in
		// Inputgraph: " + input.getVertices().count());

		/************************************** Clustering ***************************/
		/***************************************************************************/
		/***************************************************************************/
		LogicalGraph resultGraph = null;

		LogicalGraph test = input;

		System.out.println("\n**************************** Number of Nodes in modified Resultgraph: " + test.getVertices().count());

		switch (method) {
		case CONCON:// concom
			resultGraph = test.callForGraph(new ConnectedComponents());
			break;
		case CORRELATION_CLUSTERING: // CC
			resultGraph = test.callForGraph(new CorrelationClustering(isEdgeBirection, clusteringOutputType));
			break;
		case CENTER: // Center
			resultGraph = test.callForGraph(new Center(1, isEdgeBirection, clusteringOutputType));
			break;
		case MERGE_CENTER: // MC
			resultGraph = test.callForGraph(new MergeCenter(1, 0.0, isEdgeBirection, clusteringOutputType));
			break;
		case STAR1: // Star1
			resultGraph = test.callForGraph(new Star(1, 1, isEdgeBirection, clusteringOutputType));
			break;
		case STAR2: // Star2
			resultGraph = test.callForGraph(new Star(1, 2, isEdgeBirection, clusteringOutputType));
			break;
		}

		DateFormat dateFormat = new SimpleDateFormat("MMdd_HHmm");
		Date date = new Date();
		String timestamp = dateFormat.format(date);
		resultGraph.writeTo(new JSONDataSink(outFolder + timestamp + "/graphHeads.json", outFolder + timestamp + "/vertices.json", outFolder + timestamp + "/edges.json", grad_config));

		DataSet<Tuple1<String>> clusterIds = resultGraph.getVertices().map(new MapFunction<Vertex, Tuple1<String>>() {
			public Tuple1<String> map(Vertex in) {
				return Tuple1.of(in.getPropertyValue("ClusterId").toString());
			}
		});

		DataSet<Tuple1<Long>> cluster_List = clusterIds.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple1<Long>>() {
			@Override
			public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple1<Long>> out) throws Exception {
				long cnt = 0l;
				for (Tuple1<String> i : in)
					cnt++;
				out.collect(Tuple1.of(cnt));
			}
		});
		System.out.println("\n**************************** Number of Clusters in Result: " + cluster_List.count());
	}
}
