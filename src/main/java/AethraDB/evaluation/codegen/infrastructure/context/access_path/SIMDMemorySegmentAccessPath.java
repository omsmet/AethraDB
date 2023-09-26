package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import org.codehaus.janino.Java;

/**
 * {@link AccessPath} type for accessing a variable with a given name containing a
 * {@link java.lang.foreign.MemorySegment}.
 */
public class SIMDMemorySegmentAccessPath extends AccessPath {

    /**
     * The variable that is accessible through {@code this}.
     */
    private final String variableToAccess;

    /**
     * Construct an {@link SIMDMemorySegmentAccessPath} instance for a specific variable name.
     * @param variableToAccess The variable that should be accessible through {@code this}.
     * @param type The type of the variable accessible through {@code this}.
     */
    public SIMDMemorySegmentAccessPath(String variableToAccess, QueryVariableType type) {
        super(type);
        this.variableToAccess = variableToAccess;
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}.
     * @return A {@link Java.Rvalue} to read the value of the variable represented by {@code this}.
     */
    public Java.Rvalue read() {
        return JaninoGeneralGen.createAmbiguousNameRef(
                JaninoGeneralGen.getLocation(),
                this.variableToAccess
        );
    }

}
