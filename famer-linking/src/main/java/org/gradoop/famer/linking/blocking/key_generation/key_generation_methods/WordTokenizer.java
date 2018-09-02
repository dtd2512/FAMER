package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods;

import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.WordTokenizerComponent;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The flatMap transformation to generate keys from the value of an attribute of the vertex using tokenizing method.
 * The generated key is stored as a property inside the vertex.
 */
public class WordTokenizer  {
    private WordTokenizerComponent wordTokenizerComponent;
    public WordTokenizer(WordTokenizerComponent WordTokenizerComponent){
        wordTokenizerComponent = WordTokenizerComponent;
    }
    public Collection<String> execute() {
        Collection<String> output = new ArrayList<>();
        String value = wordTokenizerComponent.getAttributeValue();
        String[] attArray = value.split(wordTokenizerComponent.getTokenizer());
        if (attArray.length == 0) {
            output.add("");
            return output;
        }
        for (int i=0; i<attArray.length;i++){
            output.add(attArray[i]);
        }
        return output;
    }
}
