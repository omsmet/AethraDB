package evaluation.codegen.infrastructure.context;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * Class for storing information that can be used for performing optimisations at code generation time
 * as well as at runtime.
 */
public class OptimisationContext {

    /**
     * Construct a new {@link OptimisationContext}.
     */
    public OptimisationContext() {

    }

    /**
     * The SIMD {@link VectorSpecies} to use for double vectors.
     */
    private static VectorSpecies<Double> MAX_LEN_DOUBLE_VECTOR_SPECIES;

    /**
     * The SIMD {@link VectorSpecies} to use for float vectors.
     */
    private static VectorSpecies<Float> MAX_LEN_FLOAT_VECTOR_SPECIES;

    /**
     * The SIMD {@link VectorSpecies} to use for integer vectors.
     */
    private static VectorSpecies<Integer> MAX_LEN_INT_VECTOR_SPECIES;

    /**
     * The SIMD {@link VectorSpecies} to use for long-integer vectors.
     */
    private static VectorSpecies<Long> MAX_LEN_LONG_VECTOR_SPECIES;

    /**
     * Method to initialise the vector species in this class. All species are initialised such that
     * all vectors (even of different types) will contain an equal amount of elements. Therefor
     * the vector lengths are determined by the {@link LongVector}.
     */
    private static void initialiseVectorSpecies() {
        MAX_LEN_LONG_VECTOR_SPECIES = LongVector.SPECIES_PREFERRED;
        int vectorElementLength = MAX_LEN_LONG_VECTOR_SPECIES.length();

        if (vectorElementLength == IntVector.SPECIES_64.length())
            MAX_LEN_INT_VECTOR_SPECIES = IntVector.SPECIES_64;
        else if (vectorElementLength == IntVector.SPECIES_128.length())
            MAX_LEN_INT_VECTOR_SPECIES = IntVector.SPECIES_128;
        else if (vectorElementLength == IntVector.SPECIES_256.length())
            MAX_LEN_INT_VECTOR_SPECIES = IntVector.SPECIES_256;
        else if (vectorElementLength == IntVector.SPECIES_512.length())
            MAX_LEN_INT_VECTOR_SPECIES = IntVector.SPECIES_512;
        else
            throw new IllegalArgumentException("Cannot determine Integer VectorSpecies for provided length");

        if (vectorElementLength == FloatVector.SPECIES_64.length())
            MAX_LEN_FLOAT_VECTOR_SPECIES = FloatVector.SPECIES_64;
        else if (vectorElementLength == FloatVector.SPECIES_128.length())
            MAX_LEN_FLOAT_VECTOR_SPECIES = FloatVector.SPECIES_128;
        else if (vectorElementLength == FloatVector.SPECIES_256.length())
            MAX_LEN_FLOAT_VECTOR_SPECIES = FloatVector.SPECIES_256;
        else if (vectorElementLength == FloatVector.SPECIES_512.length())
            MAX_LEN_FLOAT_VECTOR_SPECIES = FloatVector.SPECIES_512;
        else
            throw new IllegalArgumentException("Cannot determine Float VectorSpecies for provided length");

        if (vectorElementLength == DoubleVector.SPECIES_64.length())
            MAX_LEN_DOUBLE_VECTOR_SPECIES = DoubleVector.SPECIES_64;
        else if (vectorElementLength == DoubleVector.SPECIES_128.length())
            MAX_LEN_DOUBLE_VECTOR_SPECIES = DoubleVector.SPECIES_128;
        else if (vectorElementLength == DoubleVector.SPECIES_256.length())
            MAX_LEN_DOUBLE_VECTOR_SPECIES = DoubleVector.SPECIES_256;
        else if (vectorElementLength == DoubleVector.SPECIES_512.length())
            MAX_LEN_DOUBLE_VECTOR_SPECIES = DoubleVector.SPECIES_512;
        else
            throw new IllegalArgumentException("Cannot determine Double VectorSpecies for provided length");
    }

    /**
     * Method to obtain the vector species for doubles.
     */
    public static VectorSpecies<Double> getVectorSpeciesDouble() {
        if (MAX_LEN_DOUBLE_VECTOR_SPECIES == null)
            initialiseVectorSpecies();
        return MAX_LEN_DOUBLE_VECTOR_SPECIES;
    }

    /**
     * Method to obtain the vector species for floats.
     */
    public static VectorSpecies<Float> getVectorSpeciesFloat() {
        if (MAX_LEN_FLOAT_VECTOR_SPECIES == null)
            initialiseVectorSpecies();
        return MAX_LEN_FLOAT_VECTOR_SPECIES;
    }

    /**
     * Method to obtain the vector species for integers.
     */
    public static VectorSpecies<Integer> getVectorSpeciesInt() {
        if (MAX_LEN_INT_VECTOR_SPECIES == null)
            initialiseVectorSpecies();
        return MAX_LEN_INT_VECTOR_SPECIES;
    }

    /**
     * Method to obtain the vector species for long integers.
     */
    public static VectorSpecies<Long> getVectorSpeciesLong() {
        if (MAX_LEN_LONG_VECTOR_SPECIES == null)
            initialiseVectorSpecies();
        return MAX_LEN_LONG_VECTOR_SPECIES;
    }

    /**
     * Wrapper method to be able to create {@link MemorySegment}s in Janino generated code.
     * @param address The address in memory that should be wrapped by the {@link MemorySegment}.
     * @param byteSize The byte size of the {@link MemorySegment}.
     * @return The generated {@link MemorySegment}.
     */
    public static MemorySegment createMemorySegmentForAddress(long address, long byteSize) {
        return MemorySegment.ofAddress(address, byteSize);
    }

    /**
     * Wrapper method to be able to create {@link IntVector}s in Janino generated code.
     * @param species The species of the vector to create.
     * @param memorySegment The memory segment from which to create the vector.
     * @param offset The offset into the memory segment to start reading data from.
     * @param byteOrder The byte order of the memory segment used.
     * @param mask A mask indicating which elements of the vector are valid.
     * @return The {@link IntVector} corresponding to the provided arguments.
     */
    public static IntVector createIntVector(
            VectorSpecies<Integer> species,
            MemorySegment memorySegment,
            long offset,
            ByteOrder byteOrder,
            VectorMask<Integer> mask
    ) {
        return IntVector.fromMemorySegment(species, memorySegment, offset, byteOrder, mask);
    }

    /**
     * Wrapper method to be able to create {@link DoubleVector}s in Janino generated code.
     * @param species The species of the vector to create.
     * @param memorySegment The memory segment from which to create the vector.
     * @param offset The offset into the memory segment to start reading data from.
     * @param byteOrder The byte order of the memory segment used.
     * @param mask A mask indicating which elements of the vector are valid.
     * @return The {@link DoubleVector} corresponding to the provided arguments.
     */
    public static DoubleVector createDoubleVector(
            VectorSpecies<Double> species,
            MemorySegment memorySegment,
            long offset,
            ByteOrder byteOrder,
            VectorMask<Double> mask
    ) {
        return DoubleVector.fromMemorySegment(species, memorySegment, offset, byteOrder, mask);
    }

}
