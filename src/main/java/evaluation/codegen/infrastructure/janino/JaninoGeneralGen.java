package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

/**
 * Class containing general helper methods for generating code with Janino.
 */
public class JaninoGeneralGen {

    /**
     * Prevent the class from being instantiated.
     */
    private JaninoGeneralGen() {

    }

    /**
     * A "Clever" method to get a location from a stack trace.
     */
    public static Location getLocation() {
        Exception         e   = new Exception();
        StackTraceElement ste = e.getStackTrace()[1]; // we only care about our caller
        return new Location(ste.getFileName(), ste.getLineNumber(), 0);
    }

    /**
     * Method for creating an {@link Java.Type} instance for a primitive java type.
     * @param location The location at which the created primitive type is requested for generation.
     * @param type The type of the primitive that should be created.
     * @return an {@link Java.Type} instance for a specific java primitive type.
     */
    public static Java.Type createPrimitiveType(Location location, Java.Primitive type) {
        return new Java.PrimitiveType(location, type);
    }

    /**
     * Method for creating an {@link Java.Type} instance for an array of java primitives.
     * @param location The location at which the created primitive array type is requested for generation.
     * @param type The type of the primitive that should be generated for.
     * @return A {@link Java.Type} instance for a specific java primitive array type.
     */
    public static Java.ArrayType createPrimitiveArrayType(Location location, Java.Primitive type) {
        return new Java.ArrayType(createPrimitiveType(location, type));
    }

    /**
     * Method for creating an {@link Java.Type} instance for an nested array of a java primitive.
     * @param location The location at which the created primitive array type is requested for generation.
     * @param type The type of the primitive that should be generated for.
     * @return A {@link Java.Type} instance for a specific java nested primitive array type.
     */
    public static Java.ArrayType createNestedPrimitiveArrayType(Location location, Java.Primitive type) {
        return new Java.ArrayType(createPrimitiveArrayType(location, type));
    }

    /**
     * Method for creating an {@link Java.ReferenceType} instance.
     * @param location The location at which the type reference requested for generation.
     * @param identifier The identifier of the referenced type.
     * @return The {@link Java.ReferenceType} corresponding to {@code identifier}.
     */
    public static Java.ReferenceType createReferenceType(Location location, String identifier) {
        return createReferenceType(location, identifier, null);
    }

    /**
     * Method for creating an {@link Java.ReferenceType} instance.
     * @param location The location at which the type reference requested for generation.
     * @param identifier The identifier of the referenced type.
     * @param typeArgumentIdentifier An optional type argument for the referenced type.
     * @return The {@link Java.ReferenceType} corresponding to {@code identifier<typeArgumentIdentifier>}.
     */
    public static Java.ReferenceType createReferenceType(
            Location location,
            String identifier,
            String typeArgumentIdentifier
    ) {
        return new Java.ReferenceType(
                location,
                new Java.Annotation[0],
                new String[] { identifier },
                typeArgumentIdentifier == null ? null : new Java.TypeArgument[]{
                        new Java.ReferenceType(
                                getLocation(),
                                new Java.Annotation[0],
                                new String[] { typeArgumentIdentifier },
                                null
                        )
                });
    }

    /**
     * Method to create a cast statement.
     * @param location The location at which the cast statement is requested.
     * @param targetType The type to which the value should be cast.
     * @param value The value to be cast.
     * @return The cast statement corresponding to the provided arguments.
     */
    public static Java.Cast createCast(Location location, Java.Type targetType, Java.Rvalue value) {
        return new Java.Cast(
                location,
                targetType,
                value
        );
    }

    /**
     * Method to create an {@link Java.AmbiguousName} to reference a class, variable etc..
     * @param location The location from which the reference is requested for generation.
     * @param name The name of the object being referred to.
     * @return The {@link Java.AmbiguousName} corresponding to {@code object}.
     */
    public static Java.AmbiguousName createAmbiguousNameRef(Location location, String name) {
        return new Java.AmbiguousName(
                location,
                name.split("\\.")
        );
    }

    /**
     * Method to create a {@link Java.FieldAccessExpression} for a member field on a {@code this}
     * reference.
     * @param location The location from which the access expression is requested for generation.
     * @param name The name of the field being referred to.
     * @return The {@link Java.FieldAccessExpression} corresponding to the provided parameters
     */
    public static Java.FieldAccessExpression createThisFieldAccess(Location location, String name) {
        return new Java.FieldAccessExpression(
                location,
                new Java.ThisReference(location),
                name
        );
    }

    /**
     * Method for creating an int literal.
     * @param location The location at which the created primitive is requested for generation.
     * @param value The value of the int literal.
     * @return an {@link Java.IntegerLiteral} with the specified value.
     */
    public static Java.IntegerLiteral createIntegerLiteral(Location location, int value) {
        return new Java.IntegerLiteral(location, Integer.toString(value));
    }

    /**
     * Method for creating an int literal.
     * @param location The location at which the created primitive is requested for generation.
     * @param value The value of the int literal.
     * @return an {@link Java.IntegerLiteral} with the specified value.
     */
    public static Java.IntegerLiteral createIntegerLiteral(Location location, String value) {
        return new Java.IntegerLiteral(location, value);
    }

    /**
     * Method for creating a float/double literal.
     * @param location The location at which the created primitive is requested for generation.
     * @param value The value of the float/double literal.
     * @return a {@link Java.FloatingPointLiteral} with the specified value.
     */
    public static Java.FloatingPointLiteral createFloatingPointLiteral(Location location, double value) {
        return new Java.FloatingPointLiteral(location, Double.toString(value));
    }

    /**
     * Method for creating a float/double literal.
     * @param location The location at which the created primitive is requested for generation.
     * @param value The value of the float/double literal.
     * @return a {@link Java.FloatingPointLiteral} with the specified value.
     */
    public static Java.FloatingPointLiteral createFloatingPointLiteral(Location location, float value) {
        return new Java.FloatingPointLiteral(location, Float.toString(value));
    }

    /**
     * Method for creating a float/double literal.
     * @param location The location at which the created primitive is requested for generation.
     * @param value The value of the float/double literal.
     * @return a {@link Java.FloatingPointLiteral} with the specified value.
     */
    public static Java.FloatingPointLiteral createFloatingPointLiteral(Location location, String value) {
        return new Java.FloatingPointLiteral(location, value);
    }

    /**
     * Method for creating a String literal.
     * @param location The location at which the created primitive is requested for generation.
     * @param value The value of the String literal.
     * @return a {@link Java.StringLiteral} with the specified value.
     */
    public static Java.StringLiteral createStringLiteral(Location location, String value) {
        return new Java.StringLiteral(location, value);
    }

    /**
     * Method for generating an initialised array of java primitives.
     * @param location The location at which the array is requested for generation.
     * @param primitiveType The type of the array elements.
     * @param initialValues The initial values to be contained in the array.
     * @return The initialised array.
     */
    public static Java.NewInitializedArray createInitialisedPrimitiveArray(
            Location location,
            Java.Primitive primitiveType,
            String[] initialValues
    ) {
        return new Java.NewInitializedArray(
                location,
                createPrimitiveArrayType(location, primitiveType),
                createPrimitiveArrayInitialiser(location, primitiveType, initialValues)
        );
    }

    /**
     * Method for generating the array initialiser of an array of java primitives.
     * @param location The location at which the initialiser is requested for generation.
     * @param type The type of the array elements.
     * @param values The list of values that should be contained in the initialised array.
     * @return The initialiser for the provided values.
     */
    public static Java.ArrayInitializer createPrimitiveArrayInitialiser(
            Location location,
            Java.Primitive type,
            String[] values
    ) {
        Java.Rvalue[] initialRValues = new Java.Rvalue[values.length];

        for (int i = 0; i < values.length; i++) {
            initialRValues[i] = switch (type) {
                case INT -> JaninoGeneralGen.createIntegerLiteral(location, values[i]);
                case FLOAT, DOUBLE -> JaninoGeneralGen.createFloatingPointLiteral(location, values[i]);
                default -> throw new UnsupportedOperationException("The current primitive is not supported for array initialiser generation");
            };
        }

        return new Java.ArrayInitializer(location, initialRValues);
    }

    /**
     * Method to create a new array of a primitive java type.
     * @param location The location where the new array is requested for generation.
     * @param type The primitive type that the array should get.
     * @param length The length to initialise the array with.
     * @return A {@link Java.NewArray} corresponding to the provided parameters.
     */
    public static Java.NewArray createNewPrimitiveArray(
            Location location,
            Java.Primitive type,
            int length
    ) {
        return createNewPrimitiveArray(
                location,
                createPrimitiveType(getLocation(), type),
                length
        );
    }

    /**
     * Method to create a new array of a primitive java type.
     * @param location The location where the new array is requested for generation.
     * @param primitiveType The primitive type that the array should get.
     * @param length The length to initialise the array with.
     * @return A {@link Java.NewArray} corresponding to the provided parameters.
     */
    public static Java.NewArray createNewPrimitiveArray(
            Location location,
            Java.Type primitiveType,
            int length
    ) {
        return new Java.NewArray(
                location,
                primitiveType,
                new Java.Rvalue[] { createIntegerLiteral(getLocation(), length) },
                0
        );
    }

    /**
     * Method to create a new array of a primitive java type.
     * @param location The location where the new array is requested for generation.
     * @param primitiveType The primitive type that the array should get.
     * @param length The length to initialise the array with.
     * @return A {@link Java.NewArray} corresponding to the provided parameters.
     */
    public static Java.NewArray createNewPrimitiveArray(
            Location location,
            Java.Primitive primitiveType,
            Java.Rvalue length
    ) {
        return new Java.NewArray(
                location,
                createPrimitiveType(getLocation(), primitiveType),
                new Java.Rvalue[] { length },
                0
        );
    }

    /**
     * Method to create a new 2 dimensional array of a primitive java type.
     * @param location The location where the new array is requested for generation.
     * @param primitiveType The primitive type that the array should get.
     * @param lengthD1 The length to initialise the first array dimension with.
     * @param lengthD2 The length to initialise the second array dimension with.
     * @return A {@link Java.NewArray} corresponding to the provided parameters.
     */
    public static Java.NewArray createNew2DPrimitiveArray(
            Location location,
            Java.Primitive primitiveType,
            Java.Rvalue lengthD1,
            Java.Rvalue lengthD2
    ) {
        return new Java.NewArray(
                location,
                createPrimitiveType(getLocation(), primitiveType),
                new Java.Rvalue[] { lengthD1, lengthD2 },
                0
        );
    }

    /**
     * Method for creating an expression that accesses an element of an array.
     * @param location The location at which the array access expression is requested for generation.
     * @param array The array to access an element from.
     * @param elementIndex The index of the element to access.
     * @return The expression for accessing an array element.
     */
    public static Java.ArrayAccessExpression createArrayElementAccessExpr(
            Location location,
            Java.Rvalue array,
            Java.Rvalue elementIndex
    ) {
        return new Java.ArrayAccessExpression(
                location,
                array,
                elementIndex
        );
    }

}
