package AethraDB.evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * Class containing helper methods for generating control flow related code with Janino.
 */
public class JaninoControlGen {

    /**
     * Prevent this class from being instantiated.
     */
    private JaninoControlGen() {

    }

    /**
     * Method for creating an "if (!condition) continue;" statement.
     * @param location The location at which the statement is requested for generation.
     * @param condition The condition in the above code fragment.
     * @return The generated statement.
     */
    public static Java.IfStatement createIfNotContinue(Location location, Java.Rvalue condition) {
        return createIf(
                location,
                JaninoOperatorGen.not(getLocation(), condition),
                new Java.ContinueStatement(getLocation(), null)
        );
    }

    /**
     * Method for creating an if-statement.
     * @param location The location at which the if-statement is requested for generation.
     * @param condition The guard of the if-statement.
     * @param thenBody The code to execute if the guard holds.
     * @return The generated if-statement.
     */
    public static Java.IfStatement createIf(
            Location location,
            Java.Rvalue condition,
            Java.BlockStatement thenBody
    ) {
        return createIf(location, condition, thenBody, null);
    }

    /**
     * Method for creating an if-statement.
     * @param location The location at which the if-statement is requested for generation.
     * @param condition The guard of the if-statement.
     * @param thenBody The code to execute if the guard holds.
     * @param elseBody The code to exeucte if the guard does not hold.
     * @return The generated if-statement.
     */
    public static Java.IfStatement createIf(
            Location location,
            Java.Rvalue condition,
            Java.BlockStatement thenBody,
            Java.BlockStatement elseBody
    ) {
        return new Java.IfStatement(location, condition, thenBody, elseBody);
    }

    /**
     * Method for creating a standard for-loop.
     * @param location The location at which the for-loop is requested for generation.
     * @param indexVariable The variable to use as the for-loop index.
     * @param condition The condition during which the for-loop should execute.
     * @param update The update to perform at the end of a for-loop iteration.
     * @param body The body of the for-loop.
     * @return The for-loop corresponding to the supplied arguments.
     */
    public static Java.ForStatement createForLoop(
            Location location,
            Java.LocalVariableDeclarationStatement indexVariable,
            Java.Rvalue condition,
            Java.Rvalue update,
            Java.BlockStatement body
    ) {
        return new Java.ForStatement(
                location,
                indexVariable,
                condition,
                new Java.Rvalue[] { update },
                body
        );
    }

    /**
     * Method for creating a standard while-loop.
     * @param location The location at which the while loop is requested for generation.
     * @param condition The condition during which the while-loop should execute.
     * @param body The body of the while-loop.
     * @return The while-loop corresponding to the provided arguments.
     */
    public static Java.WhileStatement createWhileLoop(
            Location location,
            Java.Rvalue condition,
            Java.BlockStatement body
    ) {
        return new Java.WhileStatement(
                location,
                condition,
                body
        );
    }

}
