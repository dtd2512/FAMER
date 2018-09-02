package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures;

import java.io.Serializable;

/**
 */
public class PrefixLengthComponent extends KeyGenerationComponent implements Serializable {
    private Integer prefixLength;
    public PrefixLengthComponent(KeyGenerationMethod Method, String Attribute, Integer PrefixLength) {
        super(Method, Attribute);
        prefixLength = PrefixLength;
    }
    public Integer getPrefixLength(){return prefixLength;}
}
