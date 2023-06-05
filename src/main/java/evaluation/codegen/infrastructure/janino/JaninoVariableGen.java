package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

/**
 * Class containing helper methods for generating variable-related code with Janino.
 */
public class JaninoVariableGen {

    /**
     * Prevent the class from being instantiated.
     */
    private JaninoVariableGen() {

    }

    /**
     * Method for creating a local variable with a primitive java type.
     * @param location The location from which the generation of the variable is requested.
     * @param primitiveType The type that the variable should receive.
     * @param variableName The name of the variable.
     * @return The generated variable.
     */
    public static Java.LocalVariableDeclarationStatement createPrimitiveLocalVar(
            Location location,
            Java.Primitive primitiveType,
            String variableName
    ) {
        return createPrimitiveLocalVar(location, primitiveType, variableName, null);
    }

    /**
     * Method for creating a local variable with a primitive java type with an initial value.
     * @param location The location from which the generation of the variable is requested.
     * @param primitiveType The type that the variable should receive.
     * @param variableName The name of the variable.
     * @param initialValue The initial value that the variable should get.
     * @return The generated variable.
     */
    public static Java.LocalVariableDeclarationStatement createPrimitiveLocalVar(
            Location location,
            Java.Primitive primitiveType,
            String variableName,
            String initialValue
    ) {
        Java.Literal initialValueLiteral = null;
        if (initialValue != null) { // Allow declaration of uninitialised variables
            initialValueLiteral = switch (primitiveType) {
                case INT -> JaninoGeneralGen.createIntegerLiteral(location, initialValue);
                case DOUBLE, FLOAT -> JaninoGeneralGen.createFloatingPointLiteral(location, initialValue);
                default ->
                        throw new UnsupportedOperationException("This primitive type is not yet supported for local variable declarations");
            };
        }

        return new Java.LocalVariableDeclarationStatement(
                location,
                new Java.Modifier[0], // Local variables do not need an access modifier ? todo: are these only used in methods or also in classes?
                JaninoGeneralGen.createPrimitiveType(location, primitiveType),
                new Java.VariableDeclarator[] {
                        new Java.VariableDeclarator(
                                location,
                                variableName,
                                0,
                                initialValueLiteral
                        )
                }
        );
    }

    /**
     * Method to create a reference to a variable.
     * @param location The location from which the reference is requested for generation.
     * @param varName The name of the variable to be referenced.
     * @return The reference to the variable identified by {@code varName}.
     */
    public static Java.AmbiguousName createVariableRef(Location location, String varName) {
        return new Java.AmbiguousName(
                location,
                new String[] { varName }
        );
    }

    /**
     * Method to generate a statement that assigns a value to a variable.
     * @param location The location from which the assignment statement is requested for generation.
     * @param lhs The variable to which the assignment should be done.
     * @param rhs The value to assign.
     * @return The assignment statement.
     * @throws CompileException when the variable assignment is not valid.
     */
    public static Java.ExpressionStatement createVariableAssignmentExpr(Location location, Java.Lvalue lhs, Java.Rvalue rhs) throws CompileException {
        return new Java.ExpressionStatement(
                createVariableAssignment(location, lhs, rhs)
        );
    }

    /**
     * Method to generate a variable assignment.
     * @param location The location from which the assignment expression is requested for generation.
     * @param lhs The variable to which the assignment should be done.
     * @param rhs The value to assign.
     * @return The variable assignment.
     */
    private static Java.Assignment createVariableAssignment(Location location, Java.Lvalue lhs, Java.Rvalue rhs) {
        return new Java.Assignment(
                location,
                lhs,
                "=",
                rhs
        );
    }

}
