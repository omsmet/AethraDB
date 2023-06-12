package evaluation.codegen.infrastructure.context;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Class for storing information that can be used for performing optimisations at code generation time
 * as well as at runtime.
 */
public class OptimisationContext {

    /**
     * The SIMD {@link VectorSpecies} to use for byte vectors.
     */
    public static final VectorSpecies<Byte> MAX_LEN_BYTE_VECTOR_SPECIES = ByteVector.SPECIES_PREFERRED;

    /**
     * The SIMD {@link VectorSpecies} to use for double vectors.
     */
    public static final VectorSpecies<Double> MAX_LEN_DOUBLE_VECTOR_SPECIES = DoubleVector.SPECIES_PREFERRED;

    /**
     * The SIMD {@link VectorSpecies} to use for float vectors.
     */
    public static final VectorSpecies<Float> MAX_LEN_FLOAT_VECTOR_SPECIES = FloatVector.SPECIES_PREFERRED;

    /**
     * The SIMD {@link VectorSpecies} to use for integer vectors.
     */
    public static final VectorSpecies<Integer> MAX_LEN_INT_VECTOR_SPECIES = IntVector.SPECIES_PREFERRED;

    /**
     * The SIMD {@link VectorSpecies} to use for long-integer vectors.
     */
    public static final VectorSpecies<Long> MAX_LEN_LONG_VECTOR_SPECIES = LongVector.SPECIES_PREFERRED;

    /**
     * Construct a new {@link OptimisationContext}.
     */
    public OptimisationContext() {

    }

    /**
     * Method to compute the vector length which is appropriate for all element types of a row.
     * @param fieldList The list of fields for which a common SIMD vector length should be computed.
     * @return The common vector length computed.
     */
    public static int computeCommonSIMDVectorLength(List<RelDataTypeField> fieldList) {
        // Compute the maximum length that any vector could ever have
        int commonLength = MAX_LEN_BYTE_VECTOR_SPECIES.length();

        // Check if we need to use a lower vector length due to any field
        for (RelDataTypeField field : fieldList) {
            commonLength = Math.min(
                    commonLength,
                    switch (field.getType().getSqlTypeName()) {
                        case DOUBLE -> MAX_LEN_DOUBLE_VECTOR_SPECIES.length();
                        case FLOAT -> MAX_LEN_FLOAT_VECTOR_SPECIES.length();
                        case INTEGER -> MAX_LEN_INT_VECTOR_SPECIES.length();
                        case BIGINT -> MAX_LEN_LONG_VECTOR_SPECIES.length();
                        default -> throw new UnsupportedOperationException("We do not have SIMD support for the current type");
                    }
            );
        }

        return commonLength;
    }

    /**
     * Obtain the correct {@link VectorSpecies<Integer>} for a given vector length.
     * @param length The length the vector should have.
     * @return The {@link VectorSpecies<Integer>} belonging to the given length.
     */
    public static VectorSpecies<Integer> getIntVectorSpecies(int length) {
        if (length == IntVector.SPECIES_64.length())
            return IntVector.SPECIES_64;
        else if (length == IntVector.SPECIES_128.length())
            return IntVector.SPECIES_128;
        else if (length == IntVector.SPECIES_256.length())
            return IntVector.SPECIES_256;
        else if (length == IntVector.SPECIES_512.length())
            return IntVector.SPECIES_512;
        else
            throw new IllegalArgumentException("Cannot return Integer VectorSpecies for provided length");
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

}
