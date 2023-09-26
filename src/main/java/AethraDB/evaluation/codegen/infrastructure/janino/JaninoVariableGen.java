package AethraDB.evaluation.codegen.infrastructure.janino;

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
     * Method to create a simple, uninitialised variable with a given name.
     * @param location The location at which the variable declaration is requested for generation.
     * @param name The name that the variable should get.
     * @return A {@link Java.VariableDeclarator} instance matching the provided parameters.
     */
    public static Java.VariableDeclarator createSimpleVariableDeclaration(Location location, String name) {
        return new Java.VariableDeclarator(
                location,
                name,
                0,
                null
        );
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
                case DOUBLE, FLOAT -> JaninoGeneralGen.createFloatingPointLiteral(location, initialValue);
                case INT, LONG -> JaninoGeneralGen.createIntegerLiteral(location, initialValue);
                default ->
                        throw new UnsupportedOperationException("This primitive type is not yet supported for local variable declarations");
            };
        }

        return createLocalVariable(
                location,
                JaninoGeneralGen.createPrimitiveType(location, primitiveType),
                variableName,
                initialValueLiteral);
    }

    /**
     * Method for generating a local variable declaration.
     * @param location The location at which the variable is requested for generation.
     * @param type The type of the variable to generate.
     * @param variableName The name of the variable to generate.
     * @param initialValue The initial value of the variable.
     * @return The generated variable.
     */
    public static Java.LocalVariableDeclarationStatement createLocalVariable(
            Location location,
            Java.Type type,
            String variableName,
            Java.ArrayInitializerOrRvalue initialValue
    ) {
        return new Java.LocalVariableDeclarationStatement(
                location,
                new Java.Modifier[0], // Local variables do not need an access modifier ? todo: are these only used in methods or also in classes?
                type,
                new Java.VariableDeclarator[]{
                        new Java.VariableDeclarator(
                                location,
                                variableName,
                                0,
                                initialValue
                        )
                }
        );
    }

    /**
     * Method to generate a statement that assigns a value to a variable.
     * @param location The location from which the assignment statement is requested for generation.
     * @param lhs The variable to which the assignment should be done.
     * @param rhs The value to assign.
     * @return The assignment statement.
     */
    public static Java.ExpressionStatement createVariableAssignmentStm(Location location, Java.Lvalue lhs, Java.Rvalue rhs) {
        try {
            return new Java.ExpressionStatement(createVariableAssignment(location, lhs, rhs));
        } catch (CompileException e) {
            throw new RuntimeException("Exception occurred while creating a variable assignment statement", e);
        }
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

    /**
     * Method to generate a statement that performs an addition assignment to a variable.
     * @param location The location from which the addition assignment statement is requested for generation.
     * @param lhs The variable to which the addition assignment should be done.
     * @param rhs The value to add to the variable assign.
     * @return The addition assignment statement.
     */
    public static Java.ExpressionStatement createVariableAdditionAssignmentStm(Location location, Java.Lvalue lhs, Java.Rvalue rhs) {
        try {
            return new Java.ExpressionStatement(createVariableAdditionAssignment(location, lhs, rhs));
        } catch (CompileException e) {
            throw new RuntimeException("Exception occurred while creating a variable addition assignment statement", e);
        }
    }

    /**
     * Method to generate a variable addition assignment (+=).
     * @param location The location from which the addition assignment expression is requested for generation.
     * @param lhs The variable to which the addition assignment should be done.
     * @param rhs The value to add to the variable.
     * @return The variable addition assignment.
     */
    public static Java.Assignment createVariableAdditionAssignment(Location location, Java.Lvalue lhs, Java.Rvalue rhs) {
        return new Java.Assignment(
                location,
                lhs,
                "+=",
                rhs
        );
    }

    /**
     * Method to generate a statement that performs an XOR assignment to a variable.
     * @param location The location from which the XOR assignment statement is requested for generation.
     * @param lhs The variable to which the XOR assignment should be done.
     * @param rhs The value to XOR to the variable assign.
     * @return The XOR assignment statement.
     */
    public static Java.ExpressionStatement createVariableXorAssignmentStm(Location location, Java.Lvalue lhs, Java.Rvalue rhs) {
        try {
            return new Java.ExpressionStatement(createVariableXorAssignment(location, lhs, rhs));
        } catch (CompileException e) {
            throw new RuntimeException("Exception occurred while creating a variable XOR assignment statement", e);
        }
    }

    /**
     * Method to generate a variable XOR assignment (^=).
     * @param location The location from which the XOR assignment expression is requested for generation.
     * @param lhs The variable to which the XOR assignment should be done.
     * @param rhs The value to XOR to the variable.
     * @return The variable XOR assignment.
     */
    public static Java.Assignment createVariableXorAssignment(Location location, Java.Lvalue lhs, Java.Rvalue rhs) {
        return new Java.Assignment(
                location,
                lhs,
                "^=",
                rhs
        );
    }

}
