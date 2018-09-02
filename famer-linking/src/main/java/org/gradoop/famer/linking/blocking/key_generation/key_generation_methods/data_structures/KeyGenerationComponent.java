package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures;

import java.io.Serializable;

/**
 */
public class KeyGenerationComponent implements Serializable {
    private KeyGenerationMethod method;
    private String attribute;
    private String attributeValue;
    public KeyGenerationComponent(KeyGenerationMethod Method, String Attribute){
        method = Method;
        attribute = Attribute;
    }
    public KeyGenerationMethod getMethod(){return method;}
    public String getAttribute(){return attribute;}
    public void setAttributeValue(String Value){attributeValue = Value;}
    public String getAttributeValue(){return attributeValue;}
}
