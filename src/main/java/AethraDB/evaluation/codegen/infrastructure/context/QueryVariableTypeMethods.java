package AethraDB.evaluation.codegen.infrastructure.context;

import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;

/**
 * Class containing helper methods to operate on {@link QueryVariableType} values.
 */
public final class QueryVariableTypeMethods {

    /**
     * Method to check if a type is a primitive type.
     */
    public static boolean isPrimitive(QueryVariableType type) {
        return switch (type) {
            case P_BOOLEAN, P_DOUBLE, P_FLOAT, P_INT, P_INT_DATE, P_LONG -> true;
            default -> false;
        };
    }

    /**
     * Method to translate a SQL type to a primitive type.
     */
    public static QueryVariableType sqlTypeToPrimitiveType(SqlTypeName sqlType) {
        return switch (sqlType) {
            case DOUBLE -> QueryVariableType.P_DOUBLE;
            case FLOAT -> QueryVariableType.P_FLOAT;
            case INTEGER -> QueryVariableType.P_INT;
            case BIGINT -> QueryVariableType.P_LONG;
            default ->
                    throw new IllegalArgumentException("sqlTypeToPrimitiveType does not support type " + sqlType.getName());
        };
    }

    /**
     * Method to get the primitive scalar type for a given type.
     */
    public static QueryVariableType primitiveType(QueryVariableType type) {
        return switch (type) {
            case P_BOOLEAN -> QueryVariableType.P_BOOLEAN;
            case P_DOUBLE -> QueryVariableType.P_DOUBLE;
            case P_FLOAT -> QueryVariableType.P_FLOAT;
            case P_INT -> QueryVariableType.P_INT;
            case P_INT_DATE -> QueryVariableType.P_INT_DATE;
            case P_LONG -> QueryVariableType.P_LONG;

            case P_A_BOOLEAN -> QueryVariableType.P_BOOLEAN;
            case P_A_DOUBLE -> QueryVariableType.P_DOUBLE;
            case P_A_FLOAT -> QueryVariableType.P_FLOAT;
            case P_A_INT -> QueryVariableType.P_INT;
            case P_A_LONG -> QueryVariableType.P_LONG;

            case ARROW_DOUBLE_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARROW_FLOAT_VECTOR -> QueryVariableType.P_FLOAT;
            case ARROW_INT_VECTOR -> QueryVariableType.P_INT;
            case ARRAY_INT_DATE_VECTOR -> QueryVariableType.P_INT_DATE;
            case ARROW_LONG_VECTOR -> QueryVariableType.P_LONG;

            case ARROW_DATE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_INT_DATE;
            case ARROW_DATE_VECTOR_W_VALIDITY_MASK -> QueryVariableType.P_INT_DATE;

            case ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK -> QueryVariableType.P_DOUBLE;

            case ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK -> QueryVariableType.P_DOUBLE;

            case ARROW_INT_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_INT;
            case ARROW_INT_VECTOR_W_VALIDITY_MASK -> QueryVariableType.P_INT;

            case ARRAY_DOUBLE_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARRAY_FLOAT_VECTOR -> QueryVariableType.P_FLOAT;
            case ARRAY_INT_VECTOR -> QueryVariableType.P_INT;
            case ARRAY_LONG_VECTOR -> QueryVariableType.P_LONG;

            case VECTOR_DOUBLE_MASKED -> QueryVariableType.P_DOUBLE;
            case VECTOR_INT_MASKED -> QueryVariableType.P_INT;

            default -> throw new IllegalArgumentException(
                    "primitiveType cannot determine the primitive scalar type for " + type);
        };
    }

    /**
     * Method to get the primitive array type for a primitive type.
     */
    public static QueryVariableType primitiveArrayTypeForPrimitive(QueryVariableType primitive) {
        return switch (primitive) {
            case P_BOOLEAN -> QueryVariableType.P_A_BOOLEAN;
            case P_DOUBLE -> QueryVariableType.P_A_DOUBLE;
            case P_FLOAT -> QueryVariableType.P_A_FLOAT;
            case P_INT -> QueryVariableType.P_A_INT;
            case P_INT_DATE -> QueryVariableType.P_A_INT_DATE;
            case P_LONG -> QueryVariableType.P_A_LONG;
            default -> throw new IllegalArgumentException(
                    "primitiveArrayTypeForPrimitive cannot determine the primitive array type for " + primitive);
        };
    }

    /**
     * Method to get the primitive member type for a primitive array type.
     */
    public static QueryVariableType primitiveMemberTypeForArray(QueryVariableType arrayType) {
        return switch (arrayType) {
            case P_A_DOUBLE -> QueryVariableType.P_DOUBLE;
            case P_A_FLOAT -> QueryVariableType.P_FLOAT;
            case P_A_INT -> QueryVariableType.P_INT;
            case P_A_LONG -> QueryVariableType.P_LONG;
            default ->
                    throw new IllegalArgumentException("primitiveMemberTypeForArray expects a primitive array type");
        };
    }

    /**
     * Method to get the array vector type for a primitive array type.
     */
    public static QueryVariableType vectorTypeForPrimitiveArrayType(QueryVariableType arrayType) {
        return switch (arrayType) {
            case P_A_DOUBLE -> QueryVariableType.ARRAY_DOUBLE_VECTOR;
            case P_A_FLOAT -> QueryVariableType.ARRAY_FLOAT_VECTOR;
            case P_A_INT -> QueryVariableType.ARRAY_INT_VECTOR;
            case P_A_INT_DATE -> QueryVariableType.ARRAY_INT_DATE_VECTOR;
            case P_A_LONG -> QueryVariableType.ARRAY_LONG_VECTOR;
            case S_A_FL_BIN -> QueryVariableType.ARRAY_FIXED_LENGTH_BINARY_VECTOR;
            case S_A_VARCHAR -> QueryVariableType.ARRAY_VARCHAR_VECTOR;
            default ->
                throw new IllegalArgumentException("vectorTypeForPrimitiveArrayType expects a primitive array type");
        };
    }

    /**
     * Method to get an Arrow vector type for a SQL type.
     */
    public static QueryVariableType sqlTypeToArrowVectorType(SqlTypeName sqlType) {
        return switch (sqlType) {
            case BIGINT -> QueryVariableType.ARROW_LONG_VECTOR;
            case CHAR -> QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR;
            case DATE -> QueryVariableType.ARROW_DATE_VECTOR;
            case DOUBLE -> QueryVariableType.ARROW_DOUBLE_VECTOR;
            case FLOAT -> QueryVariableType.ARROW_FLOAT_VECTOR;
            case INTEGER -> QueryVariableType.ARROW_INT_VECTOR;
            case VARCHAR -> QueryVariableType.ARROW_VARCHAR_VECTOR;
            default ->
                    throw new IllegalArgumentException("sqlTypeToArrowVectorType does not support type " + sqlType.getName());
        };
    }

    /**
     * Method to get the member type of Arrow vector.
     */
    public static QueryVariableType memberTypeForArrowVector(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_DATE_VECTOR -> QueryVariableType.P_INT_DATE;
            case ARROW_DOUBLE_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARROW_FIXED_LENGTH_BINARY_VECTOR -> QueryVariableType.S_FL_BIN;
            case ARROW_FLOAT_VECTOR -> QueryVariableType.P_FLOAT;
            case ARROW_INT_VECTOR -> QueryVariableType.P_INT;
            case ARROW_LONG_VECTOR -> QueryVariableType.P_LONG;
            case ARROW_VARCHAR_VECTOR -> QueryVariableType.S_VARCHAR;
            default ->
                    throw new IllegalArgumentException("memberTypeForArrowVector expects an arrow type");
        };
    }

    /**
     * Method to get the MemorySegment type for a given Arrow vector type.
     */
    public static QueryVariableType memorySegmentTypeForArrowVector(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_DOUBLE_VECTOR -> QueryVariableType.MEMORY_SEGMENT_DOUBLE;
            case ARROW_INT_VECTOR -> QueryVariableType.MEMORY_SEGMENT_INT;
            default ->
                    throw new IllegalArgumentException("memorySegmentTypeForArrowVector expects an arrow type");
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a selection vector for an arrow vector type.
     */
    public static QueryVariableType arrowVectorWithSelectionVectorType(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_DATE_VECTOR, ARROW_DATE_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_DATE_VECTOR_W_SELECTION_VECTOR;
            case ARROW_DOUBLE_VECTOR, ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR;
            case ARROW_FIXED_LENGTH_BINARY_VECTOR, ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR;
            case ARROW_INT_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
            case ARROW_VARCHAR_VECTOR, ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR;
            default ->
                    throw new IllegalArgumentException("arrowVectorWithSelectionVectorType cannot handle this type " + arrowType);
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a validity mask for an arrow vector type.
     */
    public static QueryVariableType arrowVectorWithValidityMaskType(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_DATE_VECTOR, ARROW_DATE_VECTOR_W_VALIDITY_MASK
                    -> QueryVariableType.ARROW_DATE_VECTOR_W_VALIDITY_MASK;
            case ARROW_DOUBLE_VECTOR, ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK
                    -> QueryVariableType.ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK;
            case ARROW_FIXED_LENGTH_BINARY_VECTOR, ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK
                    -> QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK;
            case ARROW_INT_VECTOR, ARROW_INT_VECTOR_W_VALIDITY_MASK
                    -> QueryVariableType.ARROW_INT_VECTOR_W_VALIDITY_MASK;
            case ARROW_VARCHAR_VECTOR, ARROW_VARCHAR_VECTOR_W_VALIDITY_MASK
                    -> QueryVariableType.ARROW_VARCHAR_VECTOR_W_VALIDITY_MASK;
            default ->
                    throw new IllegalArgumentException("arrowVectorWithValidityMaskType cannot handle this type " + arrowType);
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a selection vector for an array vector type.
     */
    public static QueryVariableType arrayVectorWithSelectionVectorType(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARRAY_DOUBLE_VECTOR, ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR;
            default ->
                    throw new IllegalArgumentException("arrayVectorWithSelectionVectorType cannot handle this type" + arrowType);
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a validity mask for an array vector type.
     */
    public static QueryVariableType arrayVectorWithValidityMaskType(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARRAY_DOUBLE_VECTOR, ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK -> QueryVariableType.ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK;
            default ->
                    throw new IllegalArgumentException("arrayVectorWithValidityMaskType cannot handle this type" + arrowType);
        };
    }

    /**
     * Method for converting a {@link QueryVariableType} to a {@link Java.Type} instance.
     * @param location The location where the type is requested for generation.
     * @param type The type to convert.
     * @return The {@link Java.Type} instance corresponding to the given type.
     */
    public static Java.Type toJavaType(Location location, QueryVariableType type) {
        return switch (type) {
            case P_BOOLEAN -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.BOOLEAN);
            case P_DOUBLE -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.DOUBLE);
            case P_FLOAT -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.FLOAT);
            case P_INT -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.INT);
            case P_INT_DATE -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.INT);
            case P_LONG -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.LONG);

            case P_A_BOOLEAN -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.BOOLEAN);
            case P_A_DOUBLE -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.DOUBLE);
            case P_A_FLOAT -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.FLOAT);
            case P_A_INT -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.INT);
            case P_A_INT_DATE -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.INT);
            case P_A_LONG -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.LONG);

            case S_FL_BIN -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.BYTE);
            case S_VARCHAR -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.BYTE);

            case S_A_FL_BIN -> JaninoGeneralGen.createNestedPrimitiveArrayType(location, Java.Primitive.BYTE);
            case S_A_VARCHAR -> JaninoGeneralGen.createNestedPrimitiveArrayType(location, Java.Primitive.BYTE);

            case ARROW_DATE_VECTOR -> JaninoGeneralGen.createReferenceType(JaninoGeneralGen.getLocation(), "org.apache.arrow.vector.DateDayVector");
            case ARROW_DOUBLE_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.Float8Vector");
            case ARROW_FIXED_LENGTH_BINARY_VECTOR -> JaninoGeneralGen.createReferenceType(JaninoGeneralGen.getLocation(), "org.apache.arrow.vector.FixedSizeBinaryVector");
            case ARROW_FLOAT_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.Float4Vector");
            case ARROW_INT_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.IntVector");
            case ARROW_LONG_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.BigIntVector");
            case ARROW_VARCHAR_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.VarCharVector");

            case VECTOR_SPECIES_DOUBLE -> JaninoGeneralGen.createReferenceType(location, "jdk.incubator.vector.VectorSpecies", "Double");
            case VECTOR_SPECIES_INT -> JaninoGeneralGen.createReferenceType(location, "jdk.incubator.vector.VectorSpecies", "Integer");

            case VECTOR_MASK_INT -> JaninoGeneralGen.createReferenceType(location, "jdk.incubator.vector.VectorMask", "Integer");

            case VECTOR_INT_MASKED -> JaninoGeneralGen.createReferenceType(location, "jdk.incubator.vector.IntVector");

            default -> throw new UnsupportedOperationException(
                    "toJavaType does not currently support this type " + type);
        };
    }

    /**
     * Method to obtain a {@link Java.Primitive} for a primitive type.
     */
    public static Java.Primitive toJavaPrimitive(QueryVariableType type) {
        return switch (type) {
            case P_BOOLEAN -> Java.Primitive.BOOLEAN;
            case P_DOUBLE -> Java.Primitive.DOUBLE;
            case P_FLOAT -> Java.Primitive.FLOAT;
            case P_INT -> Java.Primitive.INT;
            case P_INT_DATE -> Java.Primitive.INT;
            case P_LONG -> Java.Primitive.LONG;

            default -> throw new UnsupportedOperationException(
                    "toJavaPrimitive expects a primitive type");
        };
    }

    /**
     * Method to get a {@link QueryVariableType} for a given {@link Java.Type}.
     * @param type The type to find the corresponding {@link QueryVariableType} for.
     * @return The {@link QueryVariableType} corresponding to {@code type}.
     */
    public static QueryVariableType queryVariableTypeFromJavaType(Java.Type type) {
        if (type instanceof Java.PrimitiveType primitiveType) {
            return switch (primitiveType.primitive) {
                case INT -> QueryVariableType.P_INT;
                case LONG -> QueryVariableType.P_LONG;
                case FLOAT -> QueryVariableType.P_FLOAT;
                case DOUBLE -> QueryVariableType.P_DOUBLE;
                case BOOLEAN -> QueryVariableType.P_BOOLEAN;
                default ->
                        throw new UnsupportedOperationException(
                                "QueryVariableTypeMethods.queryVariableTypeFromJavaType does not support this primitive type: "
                                        + primitiveType.primitive.toString());
            };
        }

        throw new UnsupportedOperationException(
                "QueryVariableTypeMethods.queryVariableTypeFromJavaType does not support this java type: " + type.toString());
    }

}