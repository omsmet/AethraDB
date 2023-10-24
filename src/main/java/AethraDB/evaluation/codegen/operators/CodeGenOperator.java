package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen;
import AethraDB.util.language.value.literal.AethraDateDayLiteral;
import AethraDB.util.language.value.literal.AethraDoubleLiteral;
import AethraDB.util.language.value.literal.AethraIntegerLiteral;
import AethraDB.util.language.value.literal.AethraLiteral;
import AethraDB.util.language.value.literal.AethraStringLiteral;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.janino.Java;

import java.util.List;

/**
 * Class that is extended by all code generator operators.
 */
public abstract class CodeGenOperator {

    /**
     * The {@link CodeGenOperator} that consumes the result from this operator.
     */
    protected CodeGenOperator parent = null;

    /**
     * Whether a {@link CodeGenOperator} is allowed to use SIMD for processing.
     */
    private final boolean simdEnabled = false;

    /**
     * Method to set the parent of this {@link CodeGenOperator}.
     * @param parent The parent of this operator.
     */
    public void setParent(CodeGenOperator parent) {
        this.parent = parent;
    }

    /**
     * Method to indicate whether the current operator can produce code for non-vectorised execution.
     */
    public boolean canProduceNonVectorised() {
        return false;
    };

    /**
     * Method to indicate whether the current operator can produce code for vectorised execution.
     */
    public boolean canProduceVectorised() {
        return false;
    };

    /**
     * Method for generating non-vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this production style");
    };

    /**
     * Method for generating non-vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this consumption style");
    }

    /**
     * Method to invoke the parent's consumption method in the non-vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated consumption code.
     */
    protected List<Java.Statement> nonVecParentConsume(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Push the ordinal mapping and CodeGenContext
        cCtx.pushOrdinalMapping();
        cCtx.pushCodeGenContext();

        // Have the parent operator consume the result within the for loop
        List<Java.Statement> parentCode = this.parent.consumeNonVec(cCtx, oCtx);

        // Pop the CodeGenContext and ordinal to access path mappings again
        cCtx.popCodeGenContext();
        cCtx.popOrdinalMapping();

        return parentCode;
    }

    /**
     * Method for generating vectorised code during a "forward" pass.
     * This can e.g. be for building scan infrastructure.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this production style");
    }

    /**
     * Method for generating vectorised code during a "backward" pass.
     * This can e.g. be to handle consumption of a scan operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        throw new UnsupportedOperationException("This CodeGenOperator does not support this consumption style");
    }

    /**
     * Method to invoke the parent's consumption method in the vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated consumption code.
     */
    protected List<Java.Statement> vecParentConsume(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Push the ordinal mapping and CodeGenContext
        cCtx.pushOrdinalMapping();
        cCtx.pushCodeGenContext();

        // Have the parent operator consume the result within the for loop
        List<Java.Statement> parentCode = this.parent.consumeVec(cCtx, oCtx);

        // Pop the CodeGenContext and ordinal to access path mappings again
        cCtx.popCodeGenContext();
        cCtx.popOrdinalMapping();

        return parentCode;
    }

    /**
     * Method to retrieve a {@link Java.Rvalue} for a non-vector {@link AccessPath} which is an
     * ordinal of the current {@link CodeGenContext} ordinal mapping.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param ordinalIndex The ordinal index in {@code cCtx.getCurrentOrdinalMapping()} of the
     *                     {@link AccessPath} to generate the {@link Java.Rvalue} for.
     * @param codegenTarget The current list of statements being generated to perform allocations of
     *                      variables if necessary for creating the {@link Java.Rvalue}.
     * @return The {@link Java.Rvalue} corresponding to {@code accessPath}.
     */
    protected Java.Rvalue getRValueFromOrdinalAccessPathNonVec(
            CodeGenContext cCtx,
            int ordinalIndex,
            List<Java.Statement> codegenTarget
    ) {
        // Get the access path from the ordinal mapping
        AccessPath accessPath = cCtx.getCurrentOrdinalMapping().get(ordinalIndex);

        // Convert it by possibly allocating a local variable
        Pair<Java.Rvalue, ScalarVariableAccessPath> accessPathConversionResult =
                getRValueFromAccessPathNonVec(cCtx, accessPath, codegenTarget);

        // Update the ordinal mapping with the possibly new local variable
        cCtx.getCurrentOrdinalMapping().set(ordinalIndex, accessPathConversionResult.getRight());

        // Return the r-value corresponding to the requested ordinal (via the potentially new variable)
        return accessPathConversionResult.getLeft();
    }

    /**
     * Method to retrieve a {@link Java.Rvalue} for a non-vector {@link AccessPath}.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param accessPath The {@link AccessPath} to generate the {@link Java.Rvalue} for.
     * @param codegenTarget The current list of statements being generated to perform allocations of
     *                      variables if necessary for creating the {@link Java.Rvalue}.
     * @return The {@link Java.Rvalue} corresponding to {@code accessPath}.
     */
    protected Pair<Java.Rvalue, ScalarVariableAccessPath> getRValueFromAccessPathNonVec(
            CodeGenContext cCtx,
            AccessPath accessPath,
            List<Java.Statement> codegenTarget
    ) {
        // Expose the ordinal position value based on the supported type. When the access path is
        // not a simple scalar variable, transform it to reduce method calls where possible.
        if (accessPath instanceof ScalarVariableAccessPath svap) {
            return Pair.of(svap.read(), svap);

        } else if (accessPath instanceof IndexedArrowVectorElementAccessPath iaveap) {
            QueryVariableType ordinalType = iaveap.getType();

            // We allocate a new variable and initialise its value based on whether the read needs
            // to be optimised or not
            String operandVariableName;
            if (ordinalType.logicalType == QueryVariableType.LogicalType.S_FL_BIN) {
                // Allocate a global byte array cache variable to avoid allocations
                operandVariableName = cCtx.defineQueryGlobalVariable(
                        "byte_array_cache",
                        JaninoGeneralGen.createPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                        new Java.NewArray(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                                new Java.Rvalue[] { JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), ordinalType.byteWidth) },
                                0
                        ),
                        false);

                // Perform an optimised read and assign the returned byte array to the variable
                // to ensure proper initialisation
                Java.Rvalue optimisedReadValue = iaveap.readFixedLengthOptimised(operandVariableName);
                codegenTarget.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), operandVariableName),
                                optimisedReadValue
                        )
                );

            } else if (ordinalType == QueryVariableType.S_VARCHAR) {
                // Allocate an array of global byte array caches to avoid allocations as much as possible
                // while keeping the var-charity. The array has space for one byte array cache upto the
                // maximum var char length in the database
                int maximumVarCharLength = 200; // TODO: find way to not hard-code this
                String arrayOfCachesName = cCtx.defineQueryGlobalVariable(
                        "byte_array_caches",
                        JaninoGeneralGen.createNestedPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                        JaninoGeneralGen.createNew2DPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                Java.Primitive.BYTE,
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), maximumVarCharLength)
                        ),
                        false
                );

                // Perform an optimised read and assign the returned byte array to a local reference variable
                operandVariableName = cCtx.defineVariable("ordinal_value");
                codegenTarget.add(
                        JaninoVariableGen.createLocalVariable(
                                JaninoGeneralGen.getLocation(),
                                QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), ordinalType),
                                operandVariableName,
                                iaveap.readVarCharOptimised(arrayOfCachesName)
                        )
                );


            } else {
                // Allocate a local variable and assign it the value which is obtained via a generic read
                operandVariableName = cCtx.defineVariable("ordinal_value");
                codegenTarget.add(
                        JaninoVariableGen.createLocalVariable(
                                JaninoGeneralGen.getLocation(),
                                QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), ordinalType),
                                operandVariableName,
                                iaveap.readGeneric()
                        )
                );

            }

            // Update the access path to reflect the allocation of the variable
            var newAccessPath = new ScalarVariableAccessPath(operandVariableName, ordinalType);

            // Return the access path code
            return Pair.of(newAccessPath.read(), newAccessPath);

        } else {
            throw new IllegalStateException(
                    "CodeGenOperator.getRValueFromAccessPathNonVec does not support the provided access path");
        }
    }

    /**
     * Method to convert a {@link AethraLiteral} to an appropriate {@link Java.Rvalue}.
     * @param literal The {@link AethraLiteral} to convert.
     * @return The {@link Java.Rvalue} corresponding to {@code literal}.
     */
    protected Java.Rvalue aethraLiteralToRvalue(AethraLiteral literal) {
        return switch (literal) {
            case AethraDateDayLiteral addl ->
                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), addl.unixDay);
            case AethraDoubleLiteral adl ->
                    JaninoGeneralGen.createFloatingPointLiteral(JaninoGeneralGen.getLocation(), adl.value);
            case AethraIntegerLiteral ail ->
                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), ail.value);
            case AethraStringLiteral asl ->
                    JaninoGeneralGen.createInitialisedByteArray(JaninoGeneralGen.getLocation(), asl.value);
            case null, default -> throw new UnsupportedOperationException(
                    "FilterOperator.aethraLiteralToRvalue does not support this literal type: " + literal.getClass());
        };
    }

    /**
     * Method to determine if SIMD processing should be used at the current point in the codebase
     * when using non-vectorised processing.
     * @param cCtx The {@link CodeGenContext} to use during the deliberation.
     * @return {@code true} iff SIMD processing should be used.
     */
    protected boolean useSIMDNonVec(CodeGenContext cCtx) {
        // If SIMD is not enabled, don't use it
        if (!this.simdEnabled)
            return false;

        // SIMD is enabled, now check if it used
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        if (currentOrdinalMapping.size() > 0
                && !(currentOrdinalMapping.get(0) instanceof SIMDLoopAccessPath)) {
            // First ordinal is not of a SIMD access type, so don't use SIMD
            return false;
        }

        // SIMD is enabled and in use
        return true;
    }

    /**
     * Method to determine if SIMD processing should be used at the current point in the codebase
     * when using vectorised processing.
     * @return {@code true} iff SIMD processing should be used.
     */
    protected boolean useSIMDVec() {
        return this.simdEnabled;
    }

}
