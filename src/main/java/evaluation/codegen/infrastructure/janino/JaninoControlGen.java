package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;

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
