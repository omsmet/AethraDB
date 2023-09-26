package AethraDB.evaluation.codegen.infrastructure.data;

import AethraDB.evaluation.vector_support.VectorisedOperators;

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
    public void performMaintenance() {
        // Do nothing since we do not keep track of instances which have been allocated
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
    public long[] getLongVector() {
        return new long[VectorisedOperators.VECTOR_LENGTH];
    }

    @Override
    public void release(long[] vector) {
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

    @Override
    public double[] getDoubleVector() {
        // Simply return a new vector
        return new double[VectorisedOperators.VECTOR_LENGTH];
    }

    @Override
    public void release(double[] vector) {
        // Do nothing since we do not keep track of vectors which are in circulation.
    }

    @Override
    public byte[][] getNestedByteVector() {
        return new byte[VectorisedOperators.VECTOR_LENGTH][];
    }

    @Override
    public void release(byte[][] vector) {
        // Do nothing since we do not keep track of vectors which are in circulation.
    }

}
