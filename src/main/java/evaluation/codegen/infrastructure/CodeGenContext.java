package evaluation.codegen.infrastructure;

/**
 * Class for keeping track of the context during code generation, as well as plugging in specific
 * objects during execution.
 * That is: an {@link CodeGenContext} may keep track of instances of helper classes like
 * {@link evaluation.codegen.infrastructure.data.ArrowTableReader}s that are created during code
 * generation and plugged in during execution.
 */
public class CodeGenContext {

    /**
     * Test variable to ensure that a {@link CodeGenContext} can be used during both generation
     * and execution time.
     */
    private final int test;

    /**
     * Creates a new {@link CodeGenContext} instance.
     * @param test an integer used for testing.
     */
    public CodeGenContext(int test) {
        this.test = test;
    }

    /**
     * Obtain the value of {@code this.test}.
     * @return the value of {@code this.test}.
     */
    public int getTest() {
        return this.test;
    }

}
