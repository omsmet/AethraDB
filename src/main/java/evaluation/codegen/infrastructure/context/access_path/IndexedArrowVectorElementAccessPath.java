package evaluation.codegen.infrastructure.context.access_path;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;

/**
 * {@link AccessPath} type for accessing a specific element of an Arrow vector variable as specified
 * by an index variable.
 */
public class IndexedArrowVectorElementAccessPath extends AccessPath {

    /**
     * The Arrow vector variable to access elements from.
     */
    private final ArrowVectorAccessPath arrowVectorVariable;

    /**
     * The variable representing the index of the element in the Arrow vector to access.
     */
    private final ScalarVariableAccessPath indexVariable;

    /**
     * Create an {@link IndexedArrowVectorElementAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to access.
     * @param indexVariable The index variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public IndexedArrowVectorElementAccessPath(
            ArrowVectorAccessPath arrowVectorVariable,
            ScalarVariableAccessPath indexVariable,
            QueryVariableType type
    ) {
        super(type);
        this.arrowVectorVariable = arrowVectorVariable;
        this.indexVariable = indexVariable;
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the value of the variable represented by {@code this}.
     */
    public Java.Rvalue read() {
        return createMethodInvocation(
                getLocation(),
                this.arrowVectorVariable.read(),
                "get",
                new Java.Rvalue[] {
                        this.indexVariable.read()
                }
        );
    }

}
