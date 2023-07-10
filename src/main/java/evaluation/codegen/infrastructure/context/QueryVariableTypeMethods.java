package evaluation.codegen.infrastructure.context;

import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_FLOAT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_LONG_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FLOAT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_VALIDITY_MASK;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_LONG_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.MEMORY_SEGMENT_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_BOOLEAN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_DOUBLE;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_FLOAT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_BOOLEAN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_DOUBLE;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_FLOAT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;

/**
 * Class containing helper methods to operate on {@link QueryVariableType} values.
 */
public final class QueryVariableTypeMethods {

    /**
     * Method to check if a type is a primitive type.
     */
    public static boolean isPrimitive(QueryVariableType type) {
        return switch (type) {
            case P_BOOLEAN, P_DOUBLE, P_FLOAT, P_INT, P_LONG -> true;
            default -> false;
        };
    }

    /**
     * Method to get the primitive scalar type for a given type.
     */
    public static QueryVariableType primitiveType(QueryVariableType type) {
        return switch (type) {
            case P_BOOLEAN -> P_BOOLEAN;
            case P_DOUBLE -> P_DOUBLE;
            case P_FLOAT -> P_FLOAT;
            case P_INT -> P_INT;
            case P_LONG -> P_LONG;

            case P_A_BOOLEAN -> P_BOOLEAN;
            case P_A_DOUBLE -> P_DOUBLE;
            case P_A_FLOAT -> P_FLOAT;
            case P_A_INT -> P_INT;
            case P_A_LONG -> P_LONG;

            case ARROW_DOUBLE_VECTOR -> P_DOUBLE;
            case ARROW_FLOAT_VECTOR -> P_FLOAT;
            case ARROW_INT_VECTOR -> P_INT;
            case ARROW_LONG_VECTOR -> P_LONG;

            case ARRAY_DOUBLE_VECTOR -> P_DOUBLE;
            case ARRAY_FLOAT_VECTOR -> P_FLOAT;
            case ARRAY_INT_VECTOR -> P_INT;
            case ARRAY_LONG_VECTOR -> P_LONG;

            case VECTOR_INT_MASKED -> P_INT;

            default -> throw new IllegalArgumentException(
                    "primitiveType cannot determine the primitive scalar type for " + type);
        };
    }

    /**
     * Method to get the primitive array type for a primitive type.
     */
    public static QueryVariableType primitiveArrayTypeForPrimitive(QueryVariableType primitive) {
        return switch (primitive) {
            case P_BOOLEAN -> P_A_BOOLEAN;
            case P_DOUBLE -> P_A_DOUBLE;
            case P_FLOAT -> P_A_FLOAT;
            case P_INT -> P_A_INT;
            case P_LONG -> P_A_LONG;
            default -> throw new IllegalArgumentException(
                    "primitiveArrayTypeForPrimitive cannot determine the primitive array type for " + primitive);
        };
    }

    /**
     * Method to get the primitive member type for a primitive array type.
     */
    public static QueryVariableType primitiveMemberTypeForArray(QueryVariableType arrayType) {
        return switch (arrayType) {
            case P_A_DOUBLE -> P_DOUBLE;
            case P_A_FLOAT -> P_FLOAT;
            case P_A_INT -> P_INT;
            case P_A_LONG -> P_LONG;
            default ->
                    throw new IllegalArgumentException("primitiveMemberTypeForArray expects a primitive array type");
        };
    }

    /**
     * Method to get the array vector type for a primitive array type.
     */
    public static QueryVariableType vectorTypeForPrimitiveArrayType(QueryVariableType arrayType) {
        return switch (arrayType) {
            case P_A_DOUBLE -> ARRAY_DOUBLE_VECTOR;
            case P_A_FLOAT -> ARRAY_FLOAT_VECTOR;
            case P_A_INT -> ARRAY_INT_VECTOR;
            case P_A_LONG -> ARRAY_LONG_VECTOR;
            default ->
                throw new IllegalArgumentException("vectorTypeForPrimitiveArrayType expects a primitive array type");
        };
    }

    /**
     * Method to get an Arrow vector type for a SQL type.
     */
    public static QueryVariableType sqlTypeToArrowVectorType(SqlTypeName sqlType) {
        return switch (sqlType) {
            case DOUBLE -> ARROW_DOUBLE_VECTOR;
            case FLOAT -> ARROW_FLOAT_VECTOR;
            case INTEGER -> ARROW_INT_VECTOR;
            case BIGINT -> ARROW_LONG_VECTOR;
            default ->
                    throw new IllegalArgumentException("sqlTypeToArrowVectorType does not support type " + sqlType.getName());
        };
    }

    /**
     * Method to get the primitive member type of Arrow vector.
     */
    public static QueryVariableType primitiveMemberTypeForArrowVector(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_DOUBLE_VECTOR -> P_DOUBLE;
            case ARROW_FLOAT_VECTOR -> P_FLOAT;
            case ARROW_INT_VECTOR -> P_INT;
            case ARROW_LONG_VECTOR -> P_LONG;
            default ->
                    throw new IllegalArgumentException("primitiveMemberForArrow expects an arrow type");
        };
    }

    /**
     * Method to get the MemorySegment type for a given Arrow vector type.
     */
    public static QueryVariableType memorySegmentTypeForArrowVector(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_INT_VECTOR -> MEMORY_SEGMENT_INT;
            default ->
                    throw new IllegalArgumentException("memorySegmentTypeForArrowVector expects an arrow type");
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a selection vector for an arrow vector type.
     */
    public static QueryVariableType arrowVectorWithSelectionVectorType(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_INT_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR -> ARROW_INT_VECTOR_W_SELECTION_VECTOR;
            default ->
                    throw new IllegalArgumentException("arrowVectorWithSelectionVectorType cannot handle this type" + arrowType);
        };
    }

    /**
     * Method to get the {@link QueryVariableType} with a validity mask for an arrow vector type.
     */
    public static QueryVariableType arrowVectorWithValidityMaskType(QueryVariableType arrowType) {
        return switch (arrowType) {
            case ARROW_INT_VECTOR, ARROW_INT_VECTOR_W_VALIDITY_MASK -> ARROW_INT_VECTOR_W_VALIDITY_MASK;
            default ->
                    throw new IllegalArgumentException("arrowVectorWithValidityMaskType cannot handle this type" + arrowType);
        };
    }

    /**
     * Method to get the key type for a given map type.
     */
    public static QueryVariableType keyTypeForMap(QueryVariableType mapType) {
        return switch (mapType) {
            case MAP_INT_LONG_SIMPLE -> P_INT;
            default ->
                    throw new IllegalArgumentException("keyTypeForMap expects a map type");
        };
    }

    /**
     * Method to get the value type for a given map type.
     */
    public static QueryVariableType valueTypeForMap(QueryVariableType mapType) {
        return switch (mapType) {
            case MAP_INT_LONG_SIMPLE -> P_LONG;
            default ->
                    throw new IllegalArgumentException("valueTypeForMap expects a map type");
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
            case P_BOOLEAN -> createPrimitiveType(location, Java.Primitive.BOOLEAN);
            case P_DOUBLE -> createPrimitiveType(location, Java.Primitive.DOUBLE);
            case P_FLOAT -> createPrimitiveType(location, Java.Primitive.FLOAT);
            case P_INT -> createPrimitiveType(location, Java.Primitive.INT);
            case P_LONG -> createPrimitiveType(location, Java.Primitive.LONG);

            case P_A_BOOLEAN -> createPrimitiveArrayType(location, Java.Primitive.BOOLEAN);
            case P_A_DOUBLE -> createPrimitiveArrayType(location, Java.Primitive.DOUBLE);
            case P_A_FLOAT -> createPrimitiveArrayType(location, Java.Primitive.FLOAT);
            case P_A_INT -> createPrimitiveArrayType(location, Java.Primitive.INT);
            case P_A_LONG -> createPrimitiveArrayType(location, Java.Primitive.LONG);

            case ARROW_DOUBLE_VECTOR -> createReferenceType(location, "org.apache.arrow.vector.DoubleVector");
            case ARROW_FLOAT_VECTOR -> createReferenceType(location, "org.apache.arrow.vector.FloatVector");
            case ARROW_INT_VECTOR -> createReferenceType(location, "org.apache.arrow.vector.IntVector");
            case ARROW_LONG_VECTOR -> createReferenceType(location, "org.apache.arrow.vector.BigIntVector");

            case VECTOR_SPECIES_INT -> createReferenceType(location, "jdk.incubator.vector.VectorSpecies", "Integer");

            case VECTOR_MASK_INT -> createReferenceType(location, "jdk.incubator.vector.VectorMask", "Integer");

            case VECTOR_INT_MASKED -> createReferenceType(location, "jdk.incubator.vector.IntVector");

            case MAP_INT_LONG_SIMPLE -> createReferenceType(location, "Simple_Int_Long_Map");

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
                case INT -> P_INT;
                case LONG -> P_LONG;
                case FLOAT -> P_FLOAT;
                case DOUBLE -> P_DOUBLE;
                case BOOLEAN -> P_BOOLEAN;
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
