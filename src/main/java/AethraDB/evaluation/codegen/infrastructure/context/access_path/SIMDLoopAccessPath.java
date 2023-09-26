package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import jdk.incubator.vector.VectorMask;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for indicating an Arrow vector which has been wrapped in a for-loop
 * that is compatible with creating SIMD vectors around the row values.
 */
public class SIMDLoopAccessPath extends AccessPath {

    /**
     * The Arrow vector variable to access elements from.
     */
    private final ArrowVectorAccessPath arrowVectorVariable;

    /**
     * The variable representing the length of the arrow vector.
     */
    private final ScalarVariableAccessPath arrowVectorLengthVariable;

    /**
     * The variable representing the current offset into the arrow vector variable.
     */
    private final ScalarVariableAccessPath currentArrowVectorOffsetVariable;

    /**
     * The variable representing the length of the SIMD vectors to be used.
     */
    private final ScalarVariableAccessPath SIMDVectorLengthVariable;

    /**
     * The variable representing the SIMD {@link VectorMask<Integer>} that indicates whether
     * entries in {@code arrowVectorVariable[currentArrowVectorOffsetVariable,
     * currentArrowVectorOffsetVariable + SIMDVectorLengthVariable)} are valid and should be part
     * of the result.
     */
    private final SIMDVectorMaskAccessPath SIMDValidityMaskVariable;

    /**
     * The variable representing the {@link java.lang.foreign.MemorySegment} that represents the
     * {@code arrowVectorVariable}.
     */
    private final SIMDMemorySegmentAccessPath memorySegmentVariable;

    /**
     * The variable representing the {@link jdk.incubator.vector.VectorSpecies} that should be used
     * for this {@coe arrowVectorVariable}'s row values.
     */
    private final SIMDVectorSpeciesAccessPath vectorSpeciesVariable;

    /**
     * Create an {@link IndexedArrowVectorElementAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to access.
     * @param arrowVectorLengthVariable The Arrow vector length variable to use.
     * @param currentArrowVectorOffsetVariable The offset variable to use for {@code arrowVectorVariable}.
     * @param SIMDVectorLengthVariable  The SIMD length variable to use.
     * @param SIMDValidityMaskVariable The SIMD validity mask variable to use.
     * @param memorySegmentVariable The memory segment variable to use.
     * @param vectorSpeciesVariable The vector species variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public SIMDLoopAccessPath(
            ArrowVectorAccessPath arrowVectorVariable,
            ScalarVariableAccessPath arrowVectorLengthVariable,
            ScalarVariableAccessPath currentArrowVectorOffsetVariable,
            ScalarVariableAccessPath SIMDVectorLengthVariable,
            SIMDVectorMaskAccessPath SIMDValidityMaskVariable,
            SIMDMemorySegmentAccessPath memorySegmentVariable,
            SIMDVectorSpeciesAccessPath vectorSpeciesVariable,
            QueryVariableType type
    ) {
        super(type);
        this.arrowVectorVariable = arrowVectorVariable;
        this.arrowVectorLengthVariable = arrowVectorLengthVariable;
        this.currentArrowVectorOffsetVariable = currentArrowVectorOffsetVariable;
        this.SIMDVectorLengthVariable = SIMDVectorLengthVariable;
        this.SIMDValidityMaskVariable = SIMDValidityMaskVariable;
        this.memorySegmentVariable = memorySegmentVariable;
        this.vectorSpeciesVariable = vectorSpeciesVariable;
    }


    /**
     * Method performing code generation to read the value of the arrow vector represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the value of the arrow vector represented by {@code this}.
     */
    public Java.Rvalue readArrowVector() {
        return this.arrowVectorVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the Arrow vector variable.
     * @return The requested {@link AccessPath}.
     */
    public ArrowVectorAccessPath getArrowVectorAccessPath() {
        return this.arrowVectorVariable;
    }

    /**
     * Method performing code generation to read the length of the arrow vector represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the length of the arrow vector represented by {@code this}.
     */
    public Java.Rvalue readArrowVectorLength() {
        return this.arrowVectorLengthVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the Arrow vector length variable.
     * @return The requested {@link AccessPath}.
     */
    public ScalarVariableAccessPath getArrowVectorLengthAccessPath() {
        return this.arrowVectorLengthVariable;
    }

    /**
     * Method performing code generation to read the offset into the arrow vector represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the offset into the arrow vector represented by {@code this}.
     */
    public Java.Rvalue readArrowVectorOffset() {
        return this.currentArrowVectorOffsetVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the Arrow vector offset variable.
     * @return The requested {@link AccessPath}.
     */
    public ScalarVariableAccessPath getCurrentArrowVectorOffsetAccessPath() {
        return this.currentArrowVectorOffsetVariable;
    }

    /**
     * Method performing code generation to read the length of SIMD vectors possible with the representation in {@code this}.
     * @return A {@link Java.Rvalue} to read the length of SIMD vectors possible with the representation in {@code this}.
     */
    public Java.Rvalue readSIMDVectorLengthVariable() {
        return this.SIMDVectorLengthVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the SIMD vector length variable.
     * @return The requested {@link AccessPath}.
     */
    public ScalarVariableAccessPath getSIMDVectorLengthAccessPath() {
        return this.SIMDVectorLengthVariable;
    }

    /**
     * Method performing code generation to read the SIMD validity mask represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the SIMD validity mask represented by {@code this}.
     */
    public Java.Rvalue readSIMDMask() {
        return this.SIMDValidityMaskVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the SIMD validity mask variable.
     * @return The requested {@link AccessPath}.
     */
    public SIMDVectorMaskAccessPath getSIMDValidityMaskAccessPath() {
        return this.SIMDValidityMaskVariable;
    }

    /**
     * Method performing code generation to read the memory segment represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the memory segment represented by {@code this}.
     */
    public Java.Rvalue readMemorySegment() {
        return this.memorySegmentVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the memory segment variable.
     * @return The requested {@link AccessPath}.
     */
    public SIMDMemorySegmentAccessPath getMemorySegmentAccessPath() {
        return this.memorySegmentVariable;
    }

    /**
     * Method performing code generation to read the vector species required for {@code this}.
     * @return A {@link Java.Rvalue} to read the vector species variable represented by {@code this}.
     */
    public Java.Rvalue readVectorSpecies() {
        return this.vectorSpeciesVariable.read();
    }

    /**
     * Method to obtain the {@link AccessPath} to the vector species variable.
     * @return The requested {@link AccessPath}.
     */
    public SIMDVectorSpeciesAccessPath getVectorSpeciesAccessPath() {
        return this.vectorSpeciesVariable;
    }

}

