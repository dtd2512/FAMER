package org.gradoop.famer.linking.blocking.blocking_methods.data_structures;

import java.io.Serializable;

/**
 */
public class CartesianProductComponent extends BlockingComponent implements Serializable {
    public CartesianProductComponent(Boolean IntraGraphComparison, BlockingMethod BlockingMethod) {
        super(IntraGraphComparison, BlockingMethod);
    }
}
