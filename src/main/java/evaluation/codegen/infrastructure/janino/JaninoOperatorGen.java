package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

/**
 * Class containing helper methods for generating operator-related code with Janino.
 */
public class JaninoOperatorGen {

    /**
     * Prevent this class from being instantiated.
     */
    private JaninoOperatorGen() {

    }

    /**
     * Generate an addition/string concatenation operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the addition/concatenation.
     * @param rhs The right-hand side of the addition/concatenation.
     * @return The generated addition/concatenation operator.
     */
    public static Java.BinaryOperation plus(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "+", rhs);
    }

    /**
     * Generate a post-increment operator.
     * @param location The location at which the operator is requested for generation.
     * @param var The variable to increment.
     * @return the post-increment operator.
     */
    public static Java.Crement postIncrement(Location location, Java.Lvalue var) {
        return new Java.Crement(location, var, "++");
    }

    /**
     * Generate a multiplication operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the multiplication.
     * @param rhs The right-hand side of the multiplication.
     * @return The generated multiplication operator.
     */
    public static Java.BinaryOperation mul(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "*", rhs);
    }

    /**
     * Generate a modulo operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the modulo.
     * @param rhs The right-hand side of the modulo.
     * @return The generated modulo operator.
     */
    public static Java.BinaryOperation mod(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "%", rhs);
    }

    /**
     * Generate a post-increment operator statement.
     * @param location The location at which the operator is requested for generation.
     * @param var The variable to increment.
     * @return the post-increment operator.
     */
    public static Java.Statement postIncrementStm(Location location, Java.Lvalue var) {
        try {
            return new Java.ExpressionStatement(postIncrement(location, var));
        } catch (CompileException e) {
            throw new RuntimeException("Exception occurred during post increment statement creation", e);
        }
    }

    /**
     * Generate a logical not operator.
     * @param location The location at which the operator is requested for generation.
     * @param boolValue The value to invert.
     * @return The generated operator.
     */
    public static Java.UnaryOperation not(Location location, Java.Rvalue boolValue) {
        return new Java.UnaryOperation(location, "!", boolValue);
    }

    /**
     * Generate a less-than comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation lt(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "<", rhs);
    }

}
