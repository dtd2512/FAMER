package org.gradoop.famer.phase.linking.blocking;

import org.gradoop.famer.PhaseTitles;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.PrefixLength;
import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.*;
import org.gradoop.famer.phase.Phase;
import org.w3c.dom.Element;

/**
 */
public class BlockingKeyGenerationPhase extends Phase{
    private KeyGenerationMethod method;
    private String attribute;
    private Integer integerParam;
    private Double doubleParam;
    private String stringParam;

    public BlockingKeyGenerationPhase(Element PhaseContent) {
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        attribute = PhaseContent.getElementsByTagName("Attribute").item(0).getTextContent();
        method = KeyGenerationMethod.valueOf(PhaseContent.getElementsByTagName("Method").item(0).getTextContent());        
        switch (method){
            case PREFIX_LENGTH:
                integerParam = Integer.parseInt(PhaseContent.getElementsByTagName("IntegerParam").item(0).getTextContent());
                break;
            case QGRAMS:
                integerParam = Integer.parseInt(PhaseContent.getElementsByTagName("IntegerParam").item(0).getTextContent());
                doubleParam = Double.parseDouble(PhaseContent.getElementsByTagName("DoubleParam").item(0).getTextContent());
                break;
            case WORD_TOKENIZER:
                stringParam = PhaseContent.getElementsByTagName("StringParam").item(0).getTextContent();
                break;
        }
    }
    public KeyGenerationComponent toKeyGenerationComponent(){
        switch (method){
            case FULL_ATTRIBUTE:
                return new KeyGenerationComponent(method, attribute);
            case PREFIX_LENGTH:
                return new PrefixLengthComponent(method, attribute, integerParam);
            case QGRAMS:
                return new QGramsComponent(method, attribute, integerParam, doubleParam);
            case WORD_TOKENIZER:
                return new WordTokenizerComponent(method, attribute, stringParam);
        }
        return null;
    }
}
