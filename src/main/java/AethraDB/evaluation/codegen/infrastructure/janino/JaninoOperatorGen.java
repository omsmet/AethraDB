package AethraDB.evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;

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
     * Generate an subtraction operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the subtraction.
     * @param rhs The right-hand side of the subtraction.
     * @return The generated subtraction operator.
     */
    public static Java.BinaryOperation sub(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "-", rhs);
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
     * Generate a division operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the division.
     * @param rhs The right-hand side of the division.
     * @return The generated division operator.
     */
    public static Java.BinaryOperation div(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "/", rhs);
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
     * Generate a left-shift operator statement.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the left-shift.
     * @param rhs The right-hand side of the left-shift.
     * @return The generated left-shift operator.
     */
    public static Java.BinaryOperation lShift(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "<<", rhs);
    }

    /**
     * Generate a binary-and operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the binary-and.
     * @param rhs The right-hand side of the binary-and.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation binAnd(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "&", rhs);
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
     * Generate an and operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the and.
     * @param rhs The right-hand side of the and.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation and(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "&&", rhs);
    }

    /**
     * Generate an or operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the or.
     * @param rhs The right-hand side of the or.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation or(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "||", rhs);
    }

    /**
     * Generate an equality comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.Rvalue eq(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        // Need to handle some types with more care
        if (lhs instanceof Java.NewInitializedArray || rhs instanceof Java.NewInitializedArray) {
            return createMethodInvocation(
                    location,
                    createAmbiguousNameRef(location, "Arrays"),
                    "equals",
                    new Java.Rvalue[] {
                            lhs,
                            rhs
                    }
            );
        }

        return new Java.BinaryOperation(location, lhs, "==", rhs);
    }

    /**
     * Generate an inequality comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation neq(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "!=", rhs);
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

    /**
     * Generate a less-than or equal comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation le(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, "<=", rhs);
    }

    /**
     * Generate a greater-than comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation gt(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, ">", rhs);
    }

    /**
     * Generate a greater-than or equal comparison operator.
     * @param location The location at which the operator is requested for generation.
     * @param lhs The left-hand side of the comparison.
     * @param rhs The right-hand side of the comparison.
     * @return The comparison operator.
     */
    public static Java.BinaryOperation ge(Location location, Java.Rvalue lhs, Java.Rvalue rhs) {
        return new Java.BinaryOperation(location, lhs, ">=", rhs);
    }

    /**
     * Generate a ternary operator expression.
     * @param location The location at which the expression is requested for generation.
     * @param condition The condition to be evaluated.
     * @param trueValue The value in case the condition evaluates to true.
     * @param falseValue The value in case the condition evaluates to false.
     * @return The ternary operator.
     */
    public static Java.Rvalue ternary(Location location, Java.Rvalue condition, Java.Rvalue trueValue, Java.Rvalue falseValue) {
        return new Java.ConditionalExpression(location, condition, trueValue, falseValue);
    }

}