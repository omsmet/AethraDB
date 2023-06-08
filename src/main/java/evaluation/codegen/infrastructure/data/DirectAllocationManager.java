package evaluation.codegen.infrastructure.data;

import evaluation.vector_support.VectorisedOperators;

/**
 * An {@link AllocationManager} specialisation which performs allocations without any optimisations.
 */
public class DirectAllocationManager extends AllocationManager {

    /**
     * Create a new {@link DirectAllocationManager} instance.
     */
    public DirectAllocationManager() {
        super();
    }

    @Override
    public int[] getIntVector() {
        // Simply return a new vector
        return new int[VectorisedOperators.VECTOR_LENGTH];
    }

    @Override
    public void release(int[] vector) {
        // Do nothing since we do not keep track of vectors which are in circulation.
    }

    @Override
    public boolean[] getBooleanVector() {
        // Simply return a new vector
        return new boolean[VectorisedOperators.VECTOR_LENGTH];
    }

    @Override
    public void release(boolean[] vector) {
        // Do nothing since we do not keep track of vectors which are in circulation.
    }

}
