package evaluation.codegen.infrastructure.janino;

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
     * Generate a less-than comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation lt(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(
                location,
                lhs,
                "<",
                rhs
        );
    }

    /**
     * Generate a post-increment operator.
     * @param location The location at which the operator is requested for generation.
     * @param var The variable to increment.
     * @return the post-increment operator.
     */
    public static Java.Crement postIncrement(Location location, Java.Lvalue var) {
        return new Java.Crement(
                location,
                var,
                "++"
        );
    }

}
