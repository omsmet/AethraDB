package evaluation.codegen.infrastructure.data;

/**
 * Class for wrapping allocation related infrastructure so that optimisations can be applied
 * irrespective of code generation.
 */
public abstract class AllocationManager {

    /**
     * Default constructor for any {@link AllocationManager} descendant.
     */
    public AllocationManager() {

    }

    /**
     * Method for obtaining an int vector to use in query processing.
     * @return An int vector of length {@code evaluation.vector_support.VectorisedOperators.VECTOR_LENGTH}.
     */
    public abstract int[] getIntVector();

    /**
     * Method to mark an int vector used in query processing as unused. ("free a vector")
     * @param vector The vector to mark as unused.
     */
    public abstract void release(int[] vector);

    /**
     * Method for obtaining a long vector to use in query processing.
     * @return A long vector of length {@code evaluation.vector_support.VectorisedOperators.VECTOR_LENGTH}.
     */
    public abstract long[] getLongVector();

    /**
     * Method to mark a long vector used in query processing as unused. ("free a vector")
     * @param vector The vector to mark as unused.
     */
    public abstract void release(long[] vector);

    /**
     * Method for obtaining a boolean vector to use in query processing.
     * @return A boolean vector of length {@code evaluation.vector_support.VectorisedOperators.VECTOR_LENGTH}.
     */
    public abstract boolean[] getBooleanVector();

    /**
     * Method to mark a boolean vector used in query processing as unused. ("free a vector")
     * @param vector The vector to mark as unused.
     */
    public abstract void release(boolean[] vector);

}
