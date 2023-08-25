package evaluation.codegen.infrastructure.context.access_path;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for accessing an array vector where only some values are valid as
 * indicated by a validity mask of type boolean[].
 */
public class ArrayVectorWithValidityMaskAccessPath extends AccessPath {

    /**
     * The name of the array vector variable accessible through {@code this}.
     */
    private final ArrayVectorAccessPath arrayVectorVariable;

    /**
     * The name of the validity mask variable indicating the validity of records.
     */
    private final ArrayAccessPath validityMaskVariable;

    /**
     * The name of the variable representing the length of the valid portion of the validity mask.
     */
    private final ScalarVariableAccessPath validityMaskLengthVariable;

    /**
     * Construct an {@link ArrayVectorWithValidityMaskAccessPath} instance.
     * @param arrayVectorVariable The array vector variable to use.
     * @param validityMaskVariable The validity mask variable to use.
     * @param validityMaskLengthVariable The validity mask length variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public ArrayVectorWithValidityMaskAccessPath(
            ArrayVectorAccessPath arrayVectorVariable,
            ArrayAccessPath validityMaskVariable,
            ScalarVariableAccessPath validityMaskLengthVariable,
            QueryVariableType type
    ) {
        super(type);
        this.arrayVectorVariable = arrayVectorVariable;
        this.validityMaskVariable = validityMaskVariable;
        this.validityMaskLengthVariable = validityMaskLengthVariable;
    }

    /**
     * Method to get the array vector variable wrapped by {@code this}.
     * @return The array vector variable wrapped by {@code this}.
     */
    public ArrayVectorAccessPath getArrayVectorVariable() {
        return this.arrayVectorVariable;
    }

    /**
     * Method to get the validity mask variable wrapped by {@code this}.
     * @return The validity mask variable wrapped by {@code this}.
     */
    public ArrayAccessPath getValidityMaskVariable() {
        return this.validityMaskVariable;
    }

    /**
     * Create an access path to the validity mask variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the validity mask represented by {@code this}.
     */
    public Java.Rvalue readValidityMask() {
        return this.validityMaskVariable.read();
    }

    /**
     * Method to get the validity mask length variable wrapped by {@code this}.
     * @return The validity mask length variable wrapped by {@code this}.
     */
    public ScalarVariableAccessPath getValidityMaskLengthVariable() {
        return this.validityMaskLengthVariable;
    }

    /**
     * Create an access path to the validity mask length variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the validity mask length represented by {@code this}.
     */
    public Java.Rvalue readValidityMaskLength() {
        return this.validityMaskLengthVariable.read();
    }

}
