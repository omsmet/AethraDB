package evaluation.codegen.infrastructure.context;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Class for storing information that can be used for performing optimisations at code generation time
 * as well as at runtime.
 */
public class OptimisationContext {

    /**
     * The SIMD {@link VectorSpecies} to use for byte vectors.
     */
    public static final VectorSpecies<Byte> BYTE_VECTOR_SPECIES = ByteVector.SPECIES_PREFERRED;

    /**
     * The SIMD {@link VectorSpecies} to use for integer vectors.
     */
    public static final VectorSpecies<Integer> INT_VECTOR_SPECIES = IntVector.SPECIES_PREFERRED;

    /**
     * Construct a new {@link OptimisationContext}.
     */
    public OptimisationContext() {

    }

}
