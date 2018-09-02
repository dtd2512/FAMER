package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods;

import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.KeyGenerationComponent;

/**
 * The flatMap transformation to generate a key from the value of an attribute of the vertex.
 * The generated key is stored as a property inside the vertex.
 */
public class FullAttribute {
    private KeyGenerationComponent keyGenerationComponent;
    public FullAttribute(KeyGenerationComponent KeyGenerationComponent){
        keyGenerationComponent = KeyGenerationComponent;
    }
    public String execute() {
        return keyGenerationComponent.getAttributeValue();
    }
}

