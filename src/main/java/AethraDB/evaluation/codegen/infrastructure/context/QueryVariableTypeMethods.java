package AethraDB.evaluation.codegen.infrastructure.context;

import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

/**
 * Class containing helper methods to operate on {@link QueryVariableType} values.
 */
public final class QueryVariableTypeMethods {

    /**
     * Method to check if a type is a primitive type.
     */
    public static boolean isPrimitive(QueryVariableType type) {
        return switch (type.logicalType) {
            case P_BOOLEAN, P_DOUBLE, P_INT, P_INT_DATE, P_LONG -> true;
            default -> false;
        };
    }

    /**
     * Method to get the primitive scalar type for a given type.
     */
    public static QueryVariableType primitiveType(QueryVariableType type) {
        return switch (type.logicalType) {
            case P_BOOLEAN -> QueryVariableType.P_BOOLEAN;
            case P_DOUBLE -> QueryVariableType.P_DOUBLE;
            case P_INT -> QueryVariableType.P_INT;
            case P_INT_DATE -> QueryVariableType.P_INT_DATE;
            case P_LONG -> QueryVariableType.P_LONG;

            case P_A_BOOLEAN -> QueryVariableType.P_BOOLEAN;
            case P_A_DOUBLE -> QueryVariableType.P_DOUBLE;
            case P_A_INT -> QueryVariableType.P_INT;
            case P_A_LONG -> QueryVariableType.P_LONG;

            case ARROW_DOUBLE_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARROW_INT_VECTOR -> QueryVariableType.P_INT;
            case ARRAY_INT_DATE_VECTOR -> QueryVariableType.P_INT_DATE;
            case ARROW_LONG_VECTOR -> QueryVariableType.P_LONG;

            case ARROW_DATE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_INT_DATE;

            case ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_DOUBLE;

            case ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_DOUBLE;

            case ARROW_INT_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_INT;

            case ARRAY_INT_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.P_INT;

            case ARRAY_DOUBLE_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARRAY_INT_VECTOR -> QueryVariableType.P_INT;
            case ARRAY_LONG_VECTOR -> QueryVariableType.P_LONG;

            default -> throw new IllegalArgumentException(
                    "primitiveType cannot determine the primitive scalar type for " + type);
        };
    }

    /**
     * Method to get the primitive array type for a primitive type.
     */
    public static QueryVariableType primitiveArrayTypeForPrimitive(QueryVariableType primitive) {
        return switch (primitive.logicalType) {
            case P_BOOLEAN -> QueryVariableType.P_A_BOOLEAN;
            case P_DOUBLE -> QueryVariableType.P_A_DOUBLE;
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
        return switch (arrayType.logicalType) {
            case P_A_DOUBLE -> QueryVariableType.P_DOUBLE;
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
        return switch (arrayType.logicalType) {
            case P_A_DOUBLE -> QueryVariableType.ARRAY_DOUBLE_VECTOR;
            case P_A_INT -> QueryVariableType.ARRAY_INT_VECTOR;
            case P_A_INT_DATE -> QueryVariableType.ARRAY_INT_DATE_VECTOR;
            case P_A_LONG -> QueryVariableType.ARRAY_LONG_VECTOR;
            case S_A_FL_BIN -> new QueryVariableType(QueryVariableType.LogicalType.ARRAY_FIXED_LENGTH_BINARY_VECTOR, arrayType.byteWidth);
            case S_A_VARCHAR -> QueryVariableType.ARRAY_VARCHAR_VECTOR;
            default ->
                throw new IllegalArgumentException("vectorTypeForPrimitiveArrayType expects a primitive array type");
        };
    }

    /**
     * Method to get an Arrow vector type from an {@link ArrowType}.
     */
    public static QueryVariableType arrowTypeToArrowVectorType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.FixedSizeBinary fsb)
            return new QueryVariableType(QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR, fsb.getByteWidth());
        else if (arrowType instanceof ArrowType.Int intat && intat.getBitWidth() == 32)
            return QueryVariableType.ARROW_INT_VECTOR;
        else if (arrowType instanceof ArrowType.Int intat && intat.getBitWidth() == 64)
            return QueryVariableType.ARROW_LONG_VECTOR;
        else if (arrowType instanceof ArrowType.Utf8)
            return QueryVariableType.ARROW_VARCHAR_VECTOR;
        else if (arrowType instanceof ArrowType.Date)
            return QueryVariableType.ARROW_DATE_VECTOR;
        else if (arrowType instanceof ArrowType.FloatingPoint fpat && fpat.getPrecision() == FloatingPointPrecision.DOUBLE)
            return QueryVariableType.ARROW_DOUBLE_VECTOR;
        else
            throw new UnsupportedOperationException("QueryVariableTypeMethods.arrowTypeToArrowVectorType does not support " + arrowType);
    }

    /**
     * Method to get the member type of Arrow vector.
     */
    public static QueryVariableType memberTypeForArrowVector(QueryVariableType arrowType) {
        return switch (arrowType.logicalType) {
            case ARROW_DATE_VECTOR -> QueryVariableType.P_INT_DATE;
            case ARROW_DOUBLE_VECTOR -> QueryVariableType.P_DOUBLE;
            case ARROW_FIXED_LENGTH_BINARY_VECTOR -> new QueryVariableType(QueryVariableType.LogicalType.S_FL_BIN, arrowType.byteWidth);
            case ARROW_INT_VECTOR -> QueryVariableType.P_INT;
            case ARROW_LONG_VECTOR -> QueryVariableType.P_LONG;
            case ARROW_VARCHAR_VECTOR -> QueryVariableType.S_VARCHAR;
            default ->
                    throw new IllegalArgumentException("memberTypeForArrowVector expects an arrow type");
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a selection vector for an arrow vector type.
     */
    public static QueryVariableType arrowVectorWithSelectionVectorType(QueryVariableType arrowType) {
        return switch (arrowType.logicalType) {
            case ARROW_DATE_VECTOR, ARROW_DATE_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_DATE_VECTOR_W_SELECTION_VECTOR;
            case ARROW_DOUBLE_VECTOR, ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR;
            case ARROW_FIXED_LENGTH_BINARY_VECTOR -> new QueryVariableType(QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR, arrowType.byteWidth);
            case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR -> arrowType;
            case ARROW_INT_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
            case ARROW_VARCHAR_VECTOR, ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR
                    -> QueryVariableType.ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR;
            default ->
                    throw new IllegalArgumentException("arrowVectorWithSelectionVectorType cannot handle this type " + arrowType);
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a selection vector for an array vector type.
     */
    public static QueryVariableType arrayVectorWithSelectionVectorType(QueryVariableType arrowType) {
        return switch (arrowType.logicalType) {
            case ARRAY_DOUBLE_VECTOR, ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR;
            case ARRAY_INT_VECTOR, ARRAY_INT_VECTOR_W_SELECTION_VECTOR -> QueryVariableType.ARRAY_INT_VECTOR_W_SELECTION_VECTOR;
            default ->
                    throw new IllegalArgumentException("arrayVectorWithSelectionVectorType cannot handle this type" + arrowType);
        };
    }

    /**
     * Method for converting a {@link QueryVariableType} to a {@link Java.Type} instance.
     * @param location The location where the type is requested for generation.
     * @param type The type to convert.
     * @return The {@link Java.Type} instance corresponding to the given type.
     */
    public static Java.Type toJavaType(Location location, QueryVariableType type) {
        return switch (type.logicalType) {
            case P_BOOLEAN -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.BOOLEAN);
            case P_DOUBLE -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.DOUBLE);
            case P_INT -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.INT);
            case P_INT_DATE -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.INT);
            case P_LONG -> JaninoGeneralGen.createPrimitiveType(location, Java.Primitive.LONG);

            case P_A_BOOLEAN -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.BOOLEAN);
            case P_A_DOUBLE -> JaninoGeneralGen.createPrimitiveArrayType(location, Java.Primitive.DOUBLE);
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
            case ARROW_INT_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.IntVector");
            case ARROW_LONG_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.BigIntVector");
            case ARROW_VARCHAR_VECTOR -> JaninoGeneralGen.createReferenceType(location, "org.apache.arrow.vector.VarCharVector");

            default -> throw new UnsupportedOperationException(
                    "toJavaType does not currently support this type " + type);
        };
    }

    /**
     * Method to obtain a {@link Java.Primitive} for a primitive type.
     */
    public static Java.Primitive toJavaPrimitive(QueryVariableType type) {
        return switch (type.logicalType) {
            case P_BOOLEAN -> Java.Primitive.BOOLEAN;
            case P_DOUBLE -> Java.Primitive.DOUBLE;
            case P_INT -> Java.Primitive.INT;
            case P_INT_DATE -> Java.Primitive.INT;
            case P_LONG -> Java.Primitive.LONG;

            default -> throw new UnsupportedOperationException(
                    "toJavaPrimitive expects a primitive type");
        };
    }

}
