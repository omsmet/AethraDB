package AethraDB.evaluation.codegen.infrastructure.data;

import AethraDB.evaluation.vector_support.VectorisedOperators;

/**
 * An {@link AllocationManager} specialisation which returns allocation instances to consumers by
 * returning them from a buffer of available instances. Any query using this
 * {@link BufferPoolAllocationManager} should call the {@code performMaintenance} method exactly
 * at the end of the query to return all buffers to a clean state.
 */
public class BufferPoolAllocationManager extends AllocationManager {

    /**
     * Buffer of int vectors.
     */
    private int[][] intVectors;

    /**
     * The index of the next int vector to return on request.
     */
    private int nextIntVectorIndex;

    /**
     * Buffer of long vectors.
     */
    private long[][] longVectors;

    /**
     * The index of the next long vector to return on request.
     */
    private int nextLongVectorIndex;

    /**
     * Buffer of boolean vectors.
     */
    private boolean[][] booleanVectors;

    /**
     * The index of the next boolean vector to return on request.
     */
    private int nextBooleanVectorIndex;

    /**
     * Buffer of double vectors.
     */
    private double[][] doubleVectors;

    /**
     * The index of the next double vector to return on request.
     */
    private int nextDoubleVectorIndex;

    /**
     * Buffer of nested byte vectors.
     */
    private byte[][][] nestedByteVectors;

    /**
     * The index of the next nested byte vector to return on request.
     */
    private int nextNestedByteVectorIndex;

    /**
     * Create a {@link BufferPoolAllocationManager} instance where all buffers have a given initial capacity.
     * @param initialBufferCapacity The initial capacity that all buffers should have.
     */
    public BufferPoolAllocationManager(int initialBufferCapacity) {
        // Setup int vectors
        this.intVectors = new int[initialBufferCapacity][];
        this.nextIntVectorIndex = 0;

        // Setup long vectors
        this.longVectors = new long[initialBufferCapacity][];
        this.nextLongVectorIndex = 0;

        // Setup boolean vectors
        this.booleanVectors = new boolean[initialBufferCapacity][];
        this.nextBooleanVectorIndex = 0;

        // Setup double vectors
        this.doubleVectors = new double[initialBufferCapacity][];
        this.nextDoubleVectorIndex = 0;

        // Setup nested byte vectors
        this.nestedByteVectors = new byte[initialBufferCapacity][][];
        this.nextNestedByteVectorIndex = 0;

        // Create all buffer elements cleanly
        this.performMaintenance();
    }

    @Override
    public void performMaintenance() {
        // Maintain int vectors
        for (int i = 0; i < this.intVectors.length; i++) {
                this.intVectors[i] = new int[VectorisedOperators.VECTOR_LENGTH];
        }
        this.nextIntVectorIndex = 0;

        // Maintain long vectors
        for (int i = 0; i < this.longVectors.length; i++) {
            this.longVectors[i] = new long[VectorisedOperators.VECTOR_LENGTH];
        }
        this.nextLongVectorIndex = 0;

        // Maintain boolean vectors
        for (int i = 0; i < this.booleanVectors.length; i++) {
            this.booleanVectors[i] = new boolean[VectorisedOperators.VECTOR_LENGTH];
        }
        this.nextBooleanVectorIndex = 0;

        // Maintain double vectors
        for (int i = 0; i < this.doubleVectors.length; i++) {
            this.doubleVectors[i] = new double[VectorisedOperators.VECTOR_LENGTH];
        }
        this.nextDoubleVectorIndex = 0;

        // Maintain nested byte vectors
        for (int i = 0; i < this.nestedByteVectors.length; i++) {
            this.nestedByteVectors[i] = new byte[VectorisedOperators.VECTOR_LENGTH][];
        }
        this.nextNestedByteVectorIndex = 0;
    }

    @Override
    public int[] getIntVector() {
        // Check if we need to grow the buffer of int vectors
        if (this.nextIntVectorIndex >= this.intVectors.length) {
            int[][] newIntVectors = new int[this.intVectors.length * 2][];
            System.arraycopy(this.intVectors, 0, newIntVectors, 0, this.intVectors.length);
            for (int i = this.intVectors.length; i < newIntVectors.length; i++)
                newIntVectors[i] = new int[VectorisedOperators.VECTOR_LENGTH];
            this.intVectors = newIntVectors;
        }

        return this.intVectors[this.nextIntVectorIndex++];
    }

    @Override
    public void release(int[] vector) {
        // Do nothing as all int vectors will be cleaned at the end of the query via the call to
        // performMaintenance()
    }

    @Override
    public long[] getLongVector() {
        // Check if we need to grow the buffer of long vectors
        if (this.nextLongVectorIndex >= this.longVectors.length) {
            long[][] newLongVectors = new long[this.longVectors.length * 2][];
            System.arraycopy(this.longVectors, 0, newLongVectors, 0, this.longVectors.length);
            for (int i = this.intVectors.length; i < newLongVectors.length; i++)
                newLongVectors[i] = new long[VectorisedOperators.VECTOR_LENGTH];
            this.longVectors = newLongVectors;
        }

        return this.longVectors[this.nextLongVectorIndex++];
    }

    @Override
    public void release(long[] vector) {
        // Do nothing as all long vectors will be cleaned at the end of the query via the call to
        // performMaintenance()
    }

    @Override
    public boolean[] getBooleanVector() {
        // Check if we need to grow the buffer of boolean vectors
        if (this.nextBooleanVectorIndex >= this.booleanVectors.length) {
            boolean[][] newBooleanVectors = new boolean[this.booleanVectors.length * 2][];
            System.arraycopy(this.booleanVectors, 0, newBooleanVectors, 0, this.booleanVectors.length);
            for (int i = this.booleanVectors.length; i < newBooleanVectors.length; i++)
                newBooleanVectors[i] = new boolean[VectorisedOperators.VECTOR_LENGTH];
            this.booleanVectors = newBooleanVectors;
        }

        return this.booleanVectors[this.nextBooleanVectorIndex++];
    }

    @Override
    public void release(boolean[] vector) {
        // Do nothing as all boolean vectors will be cleaned at the end of the query via the call to
        // performMaintenance()
    }

    @Override
    public double[] getDoubleVector() {
        // Check if we need to grow the buffer of boolean vectors
        if (this.nextDoubleVectorIndex >= this.doubleVectors.length) {
            double[][] newDoubleVectors = new double[this.doubleVectors.length * 2][];
            System.arraycopy(this.doubleVectors, 0, newDoubleVectors, 0, this.doubleVectors.length);
            for (int i = this.doubleVectors.length; i < newDoubleVectors.length; i++)
                newDoubleVectors[i] = new double[VectorisedOperators.VECTOR_LENGTH];
            this.doubleVectors = newDoubleVectors;
        }

        return this.doubleVectors[this.nextDoubleVectorIndex++];
    }

    @Override
    public void release(double[] vector) {
        // Do nothing as all double vectors will be cleaned at the end of the query via the call to
        // performMaintenance()
    }

    @Override
    public byte[][] getNestedByteVector() {
        // Check if we need to grow the buffer of boolean vectors
        if (this.nextNestedByteVectorIndex >= this.nestedByteVectors.length) {
            byte[][][] newNestedByteVectors = new byte[this.nestedByteVectors.length * 2][][];
            System.arraycopy(this.nestedByteVectors, 0, newNestedByteVectors, 0, this.nestedByteVectors.length);
            for (int i = this.nestedByteVectors.length; i < newNestedByteVectors.length; i++)
                newNestedByteVectors[i] = new byte[VectorisedOperators.VECTOR_LENGTH][];
            this.nestedByteVectors = newNestedByteVectors;
        }

        return this.nestedByteVectors[this.nextNestedByteVectorIndex++];
    }

    @Override
    public void release(byte[][] vector) {
        // Do nothing as all nested byte vectors will be cleaned at the end of the query via the call
        // to performMaintenance()
    }

}
