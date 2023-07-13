package evaluation.codegen.infrastructure.data;

import evaluation.general_support.hashmaps.Simple_Int_Long_Map;

/**
 * Class for wrapping allocation related infrastructure so that optimisations can be applied
 * irrespective of code generation.
 */
public abstract class AllocationManager {

    /**
     * The default size at which {@link Simple_Int_Long_Map} instances are initialised by {@code this}.
     */
    protected int defaultSimpleIntLongMapCapacity = 4;

    /**
     * Default constructor for any {@link AllocationManager} descendant.
     */
    public AllocationManager() {

    }

    /**
     * Method to invoke maintenance procedures on an {@link AllocationManager} instance.
     */
    public abstract void performMaintenance();

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

    /**
     * Method for obtaining a {@link Simple_Int_Long_Map} instance for use in query processing.
     * @return An empty {@link Simple_Int_Long_Map} instance.
     */
    public abstract Simple_Int_Long_Map getSimpleIntLongMap();

    /**
     * Method to mark a {@link Simple_Int_Long_Map} instance as unused. ("free a map")
     * @param map The map to mark as unused.
     */
    public abstract void release(Simple_Int_Long_Map map);

    /**
     * Method to change the default capacity at which {@link Simple_Int_Long_Map} instances are
     * created. Note that the new default capacity is only enforced after a call to
     * {@code performMaintenance()}.
     * @param newCapacity The new default capacity to set for {@link Simple_Int_Long_Map} instances.
     */
    public void updateDefaultSimpleIntLongMapCapacity(int newCapacity) {
        this.defaultSimpleIntLongMapCapacity = newCapacity;
    }

}
