package evaluation.codegen.infrastructure.context.access_path;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;

/**
 * {@link AccessPath} type for accessing a specific element of a HashMap variable as specified
 * by an index variable. The requirement is that the "map" variable is an instance of a class that
 * exposes a "get" method with an argument type that matches the "key" variable.
 */
public class IndexedMapAccessPath extends AccessPath {

    /**
     * The map whose elements should be accessible through {@code this}.
     */
    private final MapAccessPath hashmapVariable;

    /**
     * The index variable indicating which element of the map is currently being accessed.
     */
    private final ScalarVariableAccessPath indexVariable;

    /**
     * Create an {@link IndexedMapAccessPath} instance.
     * @param hashmapVariable The hashmap variable to access.
     * @param indexVariable The index variable to use.
     * @param type The type of the variable accessible through {@code this}.
     */
    public IndexedMapAccessPath(
            MapAccessPath hashmapVariable,
            ScalarVariableAccessPath indexVariable,
            QueryVariableType type
    ) {
        super(type);
        this.hashmapVariable = hashmapVariable;
        this.indexVariable = indexVariable;
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the value of the variable represented by {@code this}.
     */
    public Java.Rvalue read() {
        return createMethodInvocation(
                getLocation(),
                this.hashmapVariable.read(),
                "get",
                new Java.Rvalue[] {
                        this.indexVariable.read()
                }
        );
    }

}
