package org.gradoop.famer.linking.similarity_measuring;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityComponent;
import org.gradoop.famer.linking.similarity_measuring.data_structures.SimilarityFieldList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 */
public class SimilarityMeasurer implements FlatMapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, SimilarityFieldList>> {
    private Collection<SimilarityComponent> similarityComponents;
    public SimilarityMeasurer (Collection<SimilarityComponent> SimilarityComponents){
        similarityComponents = SimilarityComponents;
    }
    @Override
    public void flatMap (Tuple2<Vertex, Vertex> input, Collector<Tuple3<Vertex, Vertex, SimilarityFieldList>> out) throws Exception {
        SimilarityFieldList similarityFieldList = new SimilarityFieldList();
        for (SimilarityComponent sc: similarityComponents){
            String f0Label = input.f0.getPropertyValue("graphLabel").toString();
            String f1Label = input.f1.getPropertyValue("graphLabel").toString();
            
            String value1 = "";
            String value2 = "";  
            
            // graph label "*" means that we skip check of graphLabel
            // the rest of the hierarchy is checked as usual
            if(sc.getSrcGraphLabel().equals("*") && sc.getTargetGraphLabel().equals("*")) {
            	if(input.f0.getLabel().equals(sc.getSrcLabel()) 
            			&& input.f1.getLabel().equals(sc.getTargetLabel())
                		&& input.f0.hasProperty(sc.getSrcAttribute()) 
                		&& input.f1.hasProperty(sc.getTargetAttribute())) {
            		value1 = input.f0.getPropertyValue(sc.getSrcAttribute()).toString();
            		value2 = input.f1.getPropertyValue(sc.getTargetAttribute()).toString();
            	}
				else if (input.f0.getLabel().equals(sc.getTargetLabel())
						&& input.f1.getLabel().equals(sc.getSrcLabel())
						&& input.f0.hasProperty(sc.getTargetAttribute())
						&& input.f1.hasProperty(sc.getSrcAttribute())){
					value1 = input.f1.getPropertyValue(sc.getSrcAttribute()).toString();
					value2 = input.f0.getPropertyValue(sc.getTargetAttribute()).toString();
				}
            }
            else if(sc.getSrcGraphLabel().equals("*") && !sc.getTargetGraphLabel().equals("*")) {
            	if(f1Label.equals(sc.getTargetGraphLabel())){
                	if(input.f0.getLabel().equals(sc.getSrcLabel()) 
                			&& input.f1.getLabel().equals(sc.getTargetLabel())
                    		&& input.f0.hasProperty(sc.getSrcAttribute()) 
                    		&& input.f1.hasProperty(sc.getTargetAttribute())) {
                		value1 = input.f0.getPropertyValue(sc.getSrcAttribute()).toString();
                		value2 = input.f1.getPropertyValue(sc.getTargetAttribute()).toString();
                	}
    				else if (input.f0.getLabel().equals(sc.getTargetLabel())
    						&& input.f1.getLabel().equals(sc.getSrcLabel())
    						&& input.f0.hasProperty(sc.getTargetAttribute())
    						&& input.f1.hasProperty(sc.getSrcAttribute())){
    					value1 = input.f1.getPropertyValue(sc.getSrcAttribute()).toString();
    					value2 = input.f0.getPropertyValue(sc.getTargetAttribute()).toString();
    				}
            	}
            }
            else if(!sc.getSrcGraphLabel().equals("*") && sc.getTargetGraphLabel().equals("*")) {
            	if (f0Label.equals(sc.getSrcGraphLabel())) {
                	if(input.f0.getLabel().equals(sc.getSrcLabel()) 
                			&& input.f1.getLabel().equals(sc.getTargetLabel())
                    		&& input.f0.hasProperty(sc.getSrcAttribute()) 
                    		&& input.f1.hasProperty(sc.getTargetAttribute())) {
                		value1 = input.f0.getPropertyValue(sc.getSrcAttribute()).toString();
                		value2 = input.f1.getPropertyValue(sc.getTargetAttribute()).toString();
                	}
    				else if (input.f0.getLabel().equals(sc.getTargetLabel())
    						&& input.f1.getLabel().equals(sc.getSrcLabel())
    						&& input.f0.hasProperty(sc.getTargetAttribute())
    						&& input.f1.hasProperty(sc.getSrcAttribute())){
    					value1 = input.f1.getPropertyValue(sc.getSrcAttribute()).toString();
    					value2 = input.f0.getPropertyValue(sc.getTargetAttribute()).toString();
    				}
            	}
            }
            else {
                if (f0Label.equals(sc.getSrcGraphLabel()) 
                		&& f1Label.equals(sc.getTargetGraphLabel())
                		&& input.f0.getLabel().equals(sc.getSrcLabel())
                		&& input.f1.getLabel().equals(sc.getTargetLabel())
                		&& input.f0.hasProperty(sc.getSrcAttribute())
                		&& input.f1.hasProperty(sc.getTargetAttribute())
                	){
                    value1 = input.f0.getPropertyValue(sc.getSrcAttribute()).toString();
                    value2 = input.f1.getPropertyValue(sc.getTargetAttribute()).toString();
                }
                else if (f1Label.equals(sc.getSrcGraphLabel()) 
                		&& f0Label.equals(sc.getTargetGraphLabel())
                		&& input.f0.getLabel().equals(sc.getTargetLabel())
                		&& input.f1.getLabel().equals(sc.getSrcLabel())
                		&& input.f0.hasProperty(sc.getTargetAttribute())
                		&& input.f1.hasProperty(sc.getSrcAttribute())){
                    value1 = input.f1.getPropertyValue(sc.getSrcAttribute()).toString();
                    value2 = input.f0.getPropertyValue(sc.getTargetAttribute()).toString();
                }
            }

            
            if (!value1.equals("") && !value2.equals("")) {
                similarityFieldList.add(new SimilarityComputer(sc).computeSimilarity(value1, value2));
            }
        }
        
        if (!similarityFieldList.isEmpty()) {
        	out.collect(Tuple3.of(input.f0, input.f1, similarityFieldList));
        }
    }
    
    
}
