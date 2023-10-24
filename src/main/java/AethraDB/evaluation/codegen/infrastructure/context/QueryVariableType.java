package AethraDB.evaluation.codegen.infrastructure.context;

/**
 * Class for keeping track of the type of a variable in a generated query.
 * Required since the types in the logical plan don't always correspond to the actual type
 * of query variables, due to the fact that we need to upgrade types in aggregations.
 */
public final class QueryVariableType {

    /**
     * Enum indicating the logical type of the QueryVariableType.
     */
    public enum LogicalType {

        // Primitive types
        P_BOOLEAN,
        P_DOUBLE,
        P_INT,
        P_INT_DATE, // regular int to be interpreted as a UNIX day
        P_LONG,

        // Primitive array types
        P_A_BOOLEAN,
        P_A_DOUBLE,
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
        ARROW_INT_VECTOR,
        ARROW_LONG_VECTOR,
        ARROW_VARCHAR_VECTOR,

        // Arrow vector with selection vector types
        ARROW_DATE_VECTOR_W_SELECTION_VECTOR,
        ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR,
        ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR,
        ARROW_INT_VECTOR_W_SELECTION_VECTOR,
        ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR,

        // Array vector types
        ARRAY_DOUBLE_VECTOR,
        ARRAY_FIXED_LENGTH_BINARY_VECTOR,
        ARRAY_INT_VECTOR,
        ARRAY_INT_DATE_VECTOR,
        ARRAY_LONG_VECTOR,
        ARRAY_VARCHAR_VECTOR,

        // Array vector with selection vector types
        ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR,

        // Complex types
        MAP_GENERATED,

    }

    /**
     * Field keeping track of the logical type.
     */
    public final LogicalType logicalType;

    /**
     * Field keeping track of the byte-width for fixed-length binary types only.
     */
    public final int byteWidth;

    /**
     * Create a new {@link QueryVariableType} instance.
     * @param logicalType The logical type of the new type.
     * @param byteWidth The byte-width of the new type.
     */
    public QueryVariableType(LogicalType logicalType, int byteWidth) {
        this.logicalType = logicalType;
        this.byteWidth = byteWidth;
    }

    /**
     * Instances for common types with no need for a byte-width value.
     */
    // Primitive types
    public static final QueryVariableType P_BOOLEAN = new QueryVariableType(LogicalType.P_BOOLEAN, -1);
    public static final QueryVariableType P_DOUBLE = new QueryVariableType(LogicalType.P_DOUBLE, -1);
    public static final QueryVariableType P_INT = new QueryVariableType(LogicalType.P_INT, -1);
    public static final QueryVariableType P_INT_DATE = new QueryVariableType(LogicalType.P_INT_DATE, -1); // regular int to be interpreted as a UNIX day
    public static final QueryVariableType P_LONG = new QueryVariableType(LogicalType.P_LONG, -1);

    // Primitive array types
    public static final QueryVariableType P_A_BOOLEAN = new QueryVariableType(LogicalType.P_A_BOOLEAN, -1);
    public static final QueryVariableType P_A_DOUBLE = new QueryVariableType(LogicalType.P_A_DOUBLE, -1);
    public static final QueryVariableType P_A_INT = new QueryVariableType(LogicalType.P_A_INT, -1);
    public static final QueryVariableType P_A_INT_DATE = new QueryVariableType(LogicalType.P_A_INT_DATE, -1);
    public static final QueryVariableType P_A_LONG = new QueryVariableType(LogicalType.P_A_LONG, -1);

    // Special types
    public static final QueryVariableType S_VARCHAR = new QueryVariableType(LogicalType.S_VARCHAR, -1);

    // Special array types
    public static final QueryVariableType S_A_VARCHAR = new QueryVariableType(LogicalType.S_A_VARCHAR, -1);

    // Arrow vector types
    public static final QueryVariableType ARROW_DATE_VECTOR = new QueryVariableType(LogicalType.ARROW_DATE_VECTOR, -1);
    public static final QueryVariableType ARROW_DOUBLE_VECTOR = new QueryVariableType(LogicalType.ARROW_DOUBLE_VECTOR, -1);
    public static final QueryVariableType ARROW_INT_VECTOR = new QueryVariableType(LogicalType.ARROW_INT_VECTOR, -1);
    public static final QueryVariableType ARROW_LONG_VECTOR = new QueryVariableType(LogicalType.ARROW_LONG_VECTOR, -1);
    public static final QueryVariableType ARROW_VARCHAR_VECTOR = new QueryVariableType(LogicalType.ARROW_VARCHAR_VECTOR, -1);

    // Arrow vector with selection vector types
    public static final QueryVariableType ARROW_DATE_VECTOR_W_SELECTION_VECTOR = new QueryVariableType(LogicalType.ARROW_DATE_VECTOR_W_SELECTION_VECTOR, -1);
    public static final QueryVariableType ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR = new QueryVariableType(LogicalType.ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR, -1);
    public static final QueryVariableType ARROW_INT_VECTOR_W_SELECTION_VECTOR = new QueryVariableType(LogicalType.ARROW_INT_VECTOR_W_SELECTION_VECTOR, -1);
    public static final QueryVariableType ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR = new QueryVariableType(LogicalType.ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR, -1);

    // Array vector types
    public static final QueryVariableType ARRAY_DOUBLE_VECTOR = new QueryVariableType(LogicalType.ARRAY_DOUBLE_VECTOR, -1);
    public static final QueryVariableType ARRAY_INT_VECTOR = new QueryVariableType(LogicalType.ARRAY_INT_VECTOR, -1);
    public static final QueryVariableType ARRAY_INT_DATE_VECTOR = new QueryVariableType(LogicalType.ARRAY_INT_DATE_VECTOR, -1);
    public static final QueryVariableType ARRAY_LONG_VECTOR = new QueryVariableType(LogicalType.ARRAY_LONG_VECTOR, -1);
    public static final QueryVariableType ARRAY_VARCHAR_VECTOR = new QueryVariableType(LogicalType.ARRAY_VARCHAR_VECTOR, -1);

    // Array vector with selection vector types
    public static final QueryVariableType ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR = new QueryVariableType(LogicalType.ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR, -1);

    // Complex types
    public static final QueryVariableType MAP_GENERATED = new QueryVariableType(LogicalType.MAP_GENERATED, -1);

}

