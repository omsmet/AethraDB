package evaluation.codegen.infrastructure.context.access_path;

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
    private final SimpleAccessPath arrowVectorVariable;

    /**
     * The variable representing the index of the element in the Arrow vector to access.
     */
    private final SimpleAccessPath indexVariable;

    /**
     * Create an {@link IndexedArrowVectorElementAccessPath} instance.
     * @param arrowVectorVariable The Arrow vector variable to access.
     * @param indexVariable The index variable to use.
     */
    public IndexedArrowVectorElementAccessPath(SimpleAccessPath arrowVectorVariable, SimpleAccessPath indexVariable) {
        this.arrowVectorVariable = arrowVectorVariable;
        this.indexVariable = indexVariable;
    }

    @Override
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
