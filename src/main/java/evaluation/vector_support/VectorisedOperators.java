package evaluation.vector_support;

/**
 * Class containing general definitions for the vectorised processing support "library".
 */
public class VectorisedOperators {

    /**
     * Length of vectors in the system.
     * Current value is determined as the maximum vector length of used datasets so vectors always fit.
     * TODO: maybe make less magic.
     */
    public static final int VECTOR_LENGTH = 16384;

    /**
     * Prevent this class from being instantiated.
     */
    protected VectorisedOperators() {

    }

}
