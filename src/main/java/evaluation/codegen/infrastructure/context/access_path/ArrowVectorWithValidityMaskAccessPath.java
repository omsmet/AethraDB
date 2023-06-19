package evaluation.codegen.infrastructure.context.access_path;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for accessing an Arrow vector where only some values are valid as
 * indicated by a validity mask of type boolean[].
 */
public class ArrowVectorWithValidityMaskAccessPath extends AccessPath {

    /**
     * The name of the Arrow vector variable accessible through {@code this}.
     */
    private final ArrowVectorAccessPath arrowVectorVariable;

    /**
     * The name of the validity mask variable indicating the validity of records.
     */
    private final ArrayAccessPath validityMaskVariable;

    /**
     * The name of the variable representing the length of the valid portion of the validity mask.
     */
    private final ScalarVariableAccessPath validityMaskLengthVariable;

    /**
     * Construct an {@link ArrowVectorWithValidityMaskAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to use.
     * @param validityMaskVariable The validity mask variable to use.
     * @param validityMaskLengthVariable The validity mask length variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public ArrowVectorWithValidityMaskAccessPath(
            ArrowVectorAccessPath arrowVectorVariable,
            ArrayAccessPath validityMaskVariable,
            ScalarVariableAccessPath validityMaskLengthVariable,
            QueryVariableType type
    ) {
        super(type);
        this.arrowVectorVariable = arrowVectorVariable;
        this.validityMaskVariable = validityMaskVariable;
        this.validityMaskLengthVariable = validityMaskLengthVariable;
    }

    /**
     * Method to get the name of the Arrow vector variable wrapped by {@code this}.
     * @return The name of the Arrow vector variable wrapped by {@code this}.
     */
    public ArrowVectorAccessPath getArrowVectorVariable() {
        return this.arrowVectorVariable;
    }

    /**
     * Create an access path to the Arrow vector variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the Arrow vector represented by {@code this}.
     */
    public Java.Rvalue readArrowVector() {
        return this.arrowVectorVariable.read();
    }

    /**
     * Create an access path to the validity mask variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the validity mask represented by {@code this}.
     */
    public Java.Rvalue readValidityMask() {
        return this.validityMaskVariable.read();
    }

    /**
     * Create an access path to the validity mask length variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the validity mask length represented by {@code this}.
     */
    public Java.Rvalue readValidityMaskLength() {
        return this.validityMaskLengthVariable.read();
    }

}
