package evaluation.codegen.infrastructure.context.access_path;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for accessing an array vector where only some values are valid as
 * indicated by a selection vector of type int[].
 */
public class ArrayVectorWithSelectionVectorAccessPath extends AccessPath {

    /**
     * The name of the array vector variable accessible through {@code this}.
     */
    private final ArrayVectorAccessPath arrayVectorVariable;

    /**
     * The name of the selection vector variable indicating the validity of records.
     */
    private final ArrayAccessPath selectionVectorVariable;

    /**
     * The name of the variable representing the length of the selection vector.
     */
    private final ScalarVariableAccessPath selectionVectorLengthVariable;

    /**
     * Construct an {@link ArrayVectorWithSelectionVectorAccessPath} instance.
     * @param arrayVectorVariable The array vector variable to use.
     * @param selectionVectorVariable The selection vector variable to use.
     * @param selectionVectorLengthVariable The selection vector length variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public ArrayVectorWithSelectionVectorAccessPath(
            ArrayVectorAccessPath arrayVectorVariable,
            ArrayAccessPath selectionVectorVariable,
            ScalarVariableAccessPath selectionVectorLengthVariable,
            QueryVariableType type
    ) {
        super(type);
        this.arrayVectorVariable = arrayVectorVariable;
        this.selectionVectorVariable = selectionVectorVariable;
        this.selectionVectorLengthVariable = selectionVectorLengthVariable;
    }

    /**
     * Method to get the array vector variable wrapped by {@code this}.
     * @return The array vector variable wrapped by {@code this}.
     */
    public ArrayVectorAccessPath getArrayVectorVariable() {
        return this.arrayVectorVariable;
    }

    /**
     * Method to get the selection vector variable wrapped by {@code this}.
     * @return The selection vector variable wrapped by {@code this}.
     */
    public ArrayAccessPath getSelectionVectorVariable() {
        return this.selectionVectorVariable;
    }

    /**
     * Create an access path to the selection vector variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the selection vector represented by {@code this}.
     */
    public Java.Rvalue readSelectionVector() {
        return this.selectionVectorVariable.read();
    }

    /**
     * Method to get the selection vector length variable wrapped by {@code this}.
     * @return The selection vector length variable wrapped by {@code this}.
     */
    public ScalarVariableAccessPath getSelectionVectorLengthVariable() {
        return this.selectionVectorLengthVariable;
    }

    /**
     * Create an access path to the selection vector length variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the selection vector length represented by {@code this}.
     */
    public Java.Rvalue readSelectionVectorLength() {
        return this.selectionVectorLengthVariable.read();
    }

}
