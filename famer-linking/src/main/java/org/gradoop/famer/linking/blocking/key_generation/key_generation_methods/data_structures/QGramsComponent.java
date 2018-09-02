package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures;

import java.io.Serializable;

/**
 */
public class QGramsComponent extends KeyGenerationComponent implements Serializable {
    private Integer qgramNo;
    private Double qgramThreshold;
    public QGramsComponent(KeyGenerationMethod Method, String Attribute,
                           Integer QGramNo, Double QGramThreshold) {
        super(Method, Attribute);
        qgramNo = QGramNo;
        qgramThreshold = QGramThreshold;
    }
    public Integer getQGramNo(){return qgramNo;}
    public Double getQGramThreshold(){return qgramThreshold;}
}
