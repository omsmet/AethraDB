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
    P_INT_DATE, // regular int to be interpreted as a UNIX day
    P_LONG,

    // Primitive array types
    P_A_BOOLEAN,
    P_A_DOUBLE,
    P_A_FLOAT,
    P_A_INT,
    P_A_INT_DATE,
    P_A_LONG,

    // Special types
    S_FL_BIN,
    S_VARCHAR,

    // Special array types
    S_A_FL_BIN,
    S_A_VARCHAR,

    // Arrow vector types
    ARROW_DATE_VECTOR,
    ARROW_DOUBLE_VECTOR,
    ARROW_FIXED_LENGTH_BINARY_VECTOR,
    ARROW_FLOAT_VECTOR,
    ARROW_INT_VECTOR,
    ARROW_LONG_VECTOR,
    ARROW_VARCHAR_VECTOR,

    // Arrow vector with selection/validity mask types
    ARROW_DATE_VECTOR_W_SELECTION_VECTOR,
    ARROW_DATE_VECTOR_W_VALIDITY_MASK,

    ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR,
    ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK,

    ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR,
    ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK,

    ARROW_INT_VECTOR_W_SELECTION_VECTOR,
    ARROW_INT_VECTOR_W_VALIDITY_MASK,

    ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR,
    ARROW_VARCHAR_VECTOR_W_VALIDITY_MASK,

    // Array vector types,
    ARRAY_DOUBLE_VECTOR,
    ARRAY_FIXED_LENGTH_BINARY_VECTOR,
    ARRAY_FLOAT_VECTOR,
    ARRAY_INT_VECTOR,
    ARRAY_INT_DATE_VECTOR,
    ARRAY_LONG_VECTOR,
    ARRAY_VARCHAR_VECTOR,

    // Array vector with selection/validity mask types
    ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR,
    ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK,

    // SIMD related types
    VECTOR_SPECIES_DOUBLE,
    VECTOR_SPECIES_INT,

    VECTOR_MASK_DOUBLE,
    VECTOR_MASK_INT,

    VECTOR_DOUBLE_MASKED,
    VECTOR_INT_MASKED,

    VECTOR_LONG,

    // Memory segment types
    MEMORY_SEGMENT_DOUBLE,
    MEMORY_SEGMENT_INT,

    // Complex types
    MAP_GENERATED,

}
