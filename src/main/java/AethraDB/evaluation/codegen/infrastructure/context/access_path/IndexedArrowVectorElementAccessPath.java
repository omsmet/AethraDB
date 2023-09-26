package AethraDB.evaluation.codegen.infrastructure.context.access_path;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import org.codehaus.janino.Java;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;

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
     * Method performing code generation to read the value of the variable represented by {@code this}
     * without performing any kind of optimisations.
     */
    public Java.Rvalue readGeneric() {
        return createMethodInvocation(
                getLocation(),
                this.arrowVectorVariable.read(),
                "get",
                new Java.Rvalue[] {
                        this.indexVariable.read()
                }
        );
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}
     * while performing an optimised read for a fixed length binary value. It is the callers
     * responsibility to only invoke this method for the appropriate vector types.
     * @param byteArrayCacheName The name of the byte array cache variable to use for the read.
     */
    public Java.Rvalue readFixedLengthOptimised(String byteArrayCacheName) {
        return createMethodInvocation(
                getLocation(),
                createAmbiguousNameRef(getLocation(), "ArrowOptimisations"),
                "getFixedSizeBinaryValue",
                new Java.Rvalue[] {
                        this.arrowVectorVariable.read(),
                        this.indexVariable.read(),
                        createAmbiguousNameRef(getLocation(), byteArrayCacheName)
                }
        );
    }

    /**
     * Method performing code generation to read the value of the variable represented by {@code this}
     * while performing an optimised read for a varchar binary value. It is the callers
     * responsibility to only invoke this method for the appropriate vector types.
     * @param byteArrayCachesName The name of the variable with the byte array caches to use for the read.
     */
    public Java.Rvalue readVarCharOptimised(String byteArrayCachesName) {
        return createMethodInvocation(
                getLocation(),
                createAmbiguousNameRef(getLocation(), "ArrowOptimisations"),
                "getVarCharBinaryValue",
                new Java.Rvalue[] {
                        this.arrowVectorVariable.read(),
                        this.indexVariable.read(),
                        createAmbiguousNameRef(getLocation(), byteArrayCachesName)
                }
        );
    }

}
