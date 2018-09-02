package org.gradoop.famer.linking.similarity_measuring.data_structures;

/**
 */
public enum SimilarityComputationMethod {
    JAROWINKLER,
    TRUNCATE_BEGIN,
    TRUNCATE_END,
    EDIT_DISTANCE,
    QGRAMS,
    MONGE_ELKAN,
    EXTENDED_JACCARD,
    LONGEST_COMMON_SUBSTRING,
    NUMERICAL_SIMILARITY_MAXDISTANCE,
    NUMERICAL_SIMILARITY_MAXPERCENTAGE,
    OVERLAP,
    JACARD,
    DICE
}
