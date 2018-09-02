package org.gradoop.famer.linking.blocking.key_generation.key_generation_methods;

import org.gradoop.famer.linking.blocking.key_generation.key_generation_methods.data_structures.QGramsComponent;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The flatMap transformation to generate keys from the value of an attribute of the vertex using qgram method.
 * The generated key is stored as a property inside the vertex.
 */
public class QGrams  {
    private QGramsComponent qgramsComponent;
    public QGrams(QGramsComponent QGramsComponent){
        qgramsComponent = QGramsComponent;
    }

    public Collection<String> execute() {
        Collection<String> output = new ArrayList<>();
        String value = qgramsComponent.getAttributeValue();
        Integer QGramNo = qgramsComponent.getQGramNo();
        if (value.equals("") || value.length()< QGramNo){
            output.add("");
            return output;
        }
        Collection<String> qrams = new ArrayList<String>();
        for (int i = 0; i< value.length();i++){
            if (i+QGramNo-1 < value.length()) {
                qrams.add(value.substring(i, i + QGramNo));
            }
        }
        String qramsArray[] = qrams.toArray(new String[qrams.size()]);
        String qgram = "";
        for (int t=0;t<qramsArray.length;t++) {
            qgram += qramsArray[t];
        }
        output.add(qgram);


        String omittedIndices = "";
        int qramsLength = qrams.size();
        int omitedGrams = qramsLength - Math.max(1,(int)Math.floor(qgramsComponent.getQGramThreshold() * qramsLength));
        if (omitedGrams >= 1){
            for (int i=0; i< qramsLength ;i++)
                omittedIndices += (";"+i);
        }
        else
            return output;
        omittedIndices = omittedIndices.substring(1, omittedIndices.length());
        for (int i = 2; i <= omitedGrams; i++) {
            String[] omittedIndicesArray = omittedIndices.split(";");
            for (int j = 0; j < omittedIndicesArray.length; j++) {
                String curOmitedIndices = omittedIndicesArray[j];
                if (curOmitedIndices.split(",").length == i - 1) {
                    int last = Integer.parseInt(curOmitedIndices.charAt(curOmitedIndices.length() - 1) + "");
                    String newOmitedIndices = curOmitedIndices;
                    for (int k = last + 1; k < qramsLength; k++) {
                        newOmitedIndices += ("," + k);
                        omittedIndices += ";" + newOmitedIndices;
                        newOmitedIndices = curOmitedIndices;
                    }
                }
            }
        }
        String[] omittedIndicesArray = omittedIndices.split(";");

        for (int i = 0; i< omittedIndicesArray.length; i++){
            String[] omittedIndicesArrayElement = omittedIndicesArray[i].split(",");
            String remaingIndices = "";
            for (int t=0;t<qramsArray.length;t++) {
                boolean isTOK=true;
                for (int j = 0; j < omittedIndicesArrayElement.length; j++) {
                    if (t == Integer.parseInt(omittedIndicesArrayElement[j])) {
                        isTOK = false;
                        break;
                    }
                }
                if (isTOK)
                    remaingIndices += (","+t);
            }
            String[] remaingIndicesArray = remaingIndices.substring(1).split(",");

            qgram = "";
            for (int k=0;k<remaingIndicesArray.length;k++){
                qgram += qramsArray[Integer.parseInt(remaingIndicesArray[k])];
            }
            output.add(qgram);
        }
        return output;
    }
}
