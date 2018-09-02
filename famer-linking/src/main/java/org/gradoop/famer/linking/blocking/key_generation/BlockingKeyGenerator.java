package org.gradoop.famer.linking.blocking.key_generation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.FullAttribute;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.PrefixLength;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.QGrams;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.WordTokenizer;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.*;


import java.util.ArrayList;
import java.util.Collection;

/**
 * Generate blocking key for each vertex of the input graph using one method of key generation.
 * The generated blocking key is stored as a property value with property name "key" inside the vertex.
 *
 *
 */
public class BlockingKeyGenerator implements FlatMapFunction<Vertex, Tuple2<Vertex, String>> {
    private KeyGenerationComponent keyGenerationComponent;



    public BlockingKeyGenerator (KeyGenerationComponent KeyGenerationComponent){
        keyGenerationComponent = KeyGenerationComponent;
    }


    private String cleanAttribueValue(String Attribute){
        return Attribute.toString().toLowerCase().replaceAll("\\s+", " ").trim();
    }


    @Override
    public void flatMap(Vertex vertex, Collector<Tuple2<Vertex, String>> output) throws Exception {
        
    	String attributeValue = "";
    	if(vertex.hasProperty(keyGenerationComponent.getAttribute())) {
    		attributeValue = vertex.getPropertyValue(keyGenerationComponent.getAttribute()).toString();
        }
        attributeValue = cleanAttribueValue(attributeValue);
        keyGenerationComponent.setAttributeValue(attributeValue);
        KeyGenerationMethod method = keyGenerationComponent.getMethod();
        Collection<String> keys = new ArrayList<>();
        switch (method){
            case FULL_ATTRIBUTE:
                keys.add(new FullAttribute(keyGenerationComponent).execute());
                break;
            case PREFIX_LENGTH:
                keys.add(new PrefixLength((PrefixLengthComponent) keyGenerationComponent).execute());
                break;
            case QGRAMS:
                keys.addAll(new QGrams((QGramsComponent) keyGenerationComponent).execute());
                break;
            case WORD_TOKENIZER:
                keys.addAll(new WordTokenizer((WordTokenizerComponent)keyGenerationComponent).execute());
                break;
        }
        if (keys.size() > 0) {
            for (String key : keys)
                output.collect(Tuple2.of(vertex, key));
        }
    }
}
