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
     * @param location The location at which the created primitive is requested for generation.
     * @param type The type of the primitive that should be created.
     * @return an {@link Java.Type} instance for a specific java primitive type.
     */
    public static Java.Type createPrimitiveType(Location location, Java.Primitive type) {
        return new Java.PrimitiveType(location, type);
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

}
