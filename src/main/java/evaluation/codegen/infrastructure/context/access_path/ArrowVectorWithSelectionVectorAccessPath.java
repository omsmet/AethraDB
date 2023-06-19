package evaluation.codegen.infrastructure.context.access_path;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for accessing an Arrow vector where only some values are valid as
 * indicated by a selection vector of type int[].
 */
public class ArrowVectorWithSelectionVectorAccessPath extends AccessPath {

    /**
     * The name of the Arrow vector variable accessible through {@code this}.
     */
    private final ArrowVectorAccessPath arrowVectorVariable;

    /**
     * The name of the selection vector variable indicating the validity of records.
     */
    private final ArrayAccessPath selectionVectorVariable;

    /**
     * The name of the variable representing the length of the selection vector.
     */
    private final ScalarVariableAccessPath selectionVectorLengthVariable;

    /**
     * Construct an {@link ArrowVectorWithSelectionVectorAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to use.
     * @param selectionVectorVariable The selection vector variable to use.
     * @param selectionVectorLengthVariable The selection vector length variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public ArrowVectorWithSelectionVectorAccessPath(
            ArrowVectorAccessPath arrowVectorVariable,
            ArrayAccessPath selectionVectorVariable,
            ScalarVariableAccessPath selectionVectorLengthVariable,
            QueryVariableType type
    ) {
        super(type);
        this.arrowVectorVariable = arrowVectorVariable;
        this.selectionVectorVariable = selectionVectorVariable;
        this.selectionVectorLengthVariable = selectionVectorLengthVariable;
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
     * Create an access path to the selection vector variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the selection vector represented by {@code this}.
     */
    public Java.Rvalue readSelectionVector() {
        return this.selectionVectorVariable.read();
    }

    /**
     * Create an access path to the selection vector length variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to the selection vector length represented by {@code this}.
     */
    public Java.Rvalue readSelectionVectorLength() {
        return this.selectionVectorLengthVariable.read();
    }

}
