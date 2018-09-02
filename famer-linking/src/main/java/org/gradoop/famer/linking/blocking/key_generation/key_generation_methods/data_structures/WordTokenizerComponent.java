package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures;

import java.io.Serializable;

/**
 */
public class WordTokenizerComponent extends KeyGenerationComponent implements Serializable {
    private String tokenizer;
    public WordTokenizerComponent(KeyGenerationMethod Method, String Attribute, String Tokenizer) {
        super(Method, Attribute);
        tokenizer = Tokenizer;
    }
    public String getTokenizer(){return tokenizer;}
}
