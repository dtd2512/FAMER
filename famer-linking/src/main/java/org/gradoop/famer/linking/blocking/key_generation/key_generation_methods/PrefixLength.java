package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods;

import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.PrefixLengthComponent;

/**
 * The map transformation to generate a key from n initial letters of the value of an attribute of the vertex.
 * The generated key is stored as a property inside the vertex.
 */
public class PrefixLength {
    private PrefixLengthComponent prefixLengthComponent;

    public PrefixLength(PrefixLengthComponent PrefixLengthComponent){
        prefixLengthComponent = PrefixLengthComponent;
    }
    public String execute() {
        String value = prefixLengthComponent.getAttributeValue();
//
        String key="";
        if(value.length() <= prefixLengthComponent.getPrefixLength())
            key = value;
        else
            key = value.substring(0,prefixLengthComponent.getPrefixLength());
        return key;

    }
}
