package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;

/**
 * {@link AccessPath} type for accessing a variable representing a vector that is contained in an array.
 */
public class ArrayVectorAccessPath extends AccessPath {

    /**
     * The variable that contains the actual vector accessible through {@code this}.
     */
    private final ArrayAccessPath vectorVariable;

    /**
     * The variable that indicates the length of the valid part of the vector variable.
     */
    private final ScalarVariableAccessPath vectorLengthVariable;

    /**
     * Construct an {@link ArrayVectorAccessPath} instance for a specific array vector.
     * @param vectorVariable The vector variable that should be accessible through {@code this}.
     * @param vectorLengthVariable The variable indicating the length of the vector.
     * @param type The type of the variable accessible through {@code this}.
     */
    public ArrayVectorAccessPath(
            ArrayAccessPath vectorVariable,
            ScalarVariableAccessPath vectorLengthVariable,
            QueryVariableType type) {
        super(type);
        this.vectorVariable = vectorVariable;
        this.vectorLengthVariable = vectorLengthVariable;
    }

    /**
     * Method to obtain the vector variable wrapped by {@code this}.
     */
    public ArrayAccessPath getVectorVariable() {
        return this.vectorVariable;
    }

    /**
     * Method to obtain the vector length variable wrapped by {@code this}.
     */
    public ScalarVariableAccessPath getVectorLengthVariable() {
        return this.vectorLengthVariable;
    }

}
