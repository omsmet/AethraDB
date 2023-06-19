package evaluation.codegen.infrastructure.context;

/**
 * Enum for keeping track of the type of a variable in a generated query.
 * Required since the types in the logical plan don't always correspond to the actual type
 * of query variables, due to the fact that we need to upgrade types in aggregations.
 */
public enum QueryVariableType {

    // Primitive types
    P_BOOLEAN,
    P_DOUBLE,
    P_FLOAT,
    P_INT,
    P_LONG,

    // Primitive array types
    P_A_BOOLEAN,
    P_A_DOUBLE,
    P_A_FLOAT,
    P_A_INT,
    P_A_LONG,

    // Arrow vector types
    ARROW_DOUBLE_VECTOR,
    ARROW_FLOAT_VECTOR,
    ARROW_INT_VECTOR,
    ARROW_LONG_VECTOR,

    // SIMD related types
    VECTOR_SPECIES_INT,

    VECTOR_MASK_INT,

    VECTOR_INT_MASKED,

    // Memory segment types
    MEMORY_SEGMENT_INT,

    // Complex types
    MAP_INT_LONG_SIMPLE,

}
