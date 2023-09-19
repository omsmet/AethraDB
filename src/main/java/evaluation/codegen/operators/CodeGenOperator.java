package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.codehaus.janino.Java;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createFloatingPointLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createInitialisedByteArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNestedPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNew2DPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * Class that is extended by all code generator operators.
 * Wraps the {@link RelNode} representing the logical sub-plan that is implemented by this node.
 */
public abstract class CodeGenOperator<T extends RelNode> {

    /**
     * The {@link CodeGenOperator} that consumes the result from this operator.
     */
    protected CodeGenOperator<?> parent;

    /**
     * The logical sub-plan that is represented by the query operator tree rooted at this operator.
     */
    private final T logicalSubplan;

    /**
     * Whether a {@link CodeGenOperator} is allowed to use SIMD for processing.
     */
    private final boolean simdEnabled;

    /**
     * Initialises a new instance of a specific {@link CodeGenOperator}.
     * @param logicalSubplan The logical sub-plan that should be implemented by this operator.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     */
    protected CodeGenOperator(T logicalSubplan, boolean simdEnabled) {
        this.parent = null;
        this.logicalSubplan = logicalSubplan;
        this.simdEnabled = simdEnabled;
    }

    /**
     * Obtain the logical query plan that should be implemented by this operator.
     * @return The logical query plan implemented by the query operator tree rooted at this node.
     */
    public T getLogicalSubplan() {
        return this.logicalSubplan;
    }

    /**
     * Method to set the parent of this {@link CodeGenOperator}.
     * @param parent The parent of this operator.
     */
    public void setParent(CodeGenOperator<?> parent) {
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

// TODO: ensure we can really do without these operators
//
//    /**
//     * Method to indicate whether the current operator can consume code for non-vectorised execution.
//     */
//    public abstract boolean canConsumeNonVectorised();
//
//    /**
//     * Method to indicate whether the current operator can consume code for vectorised execution.
//     */
//    public abstract boolean canConsumeVectorised();

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
        cCtx.getCurrentOrdinalMapping().set(ordinalIndex, accessPathConversionResult.right);

        // Return the r-value corresponding to the requested ordinal (via the potentially new variable)
        return accessPathConversionResult.left;
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
            return new Pair<>(svap.read(), svap);

        } else if (accessPath instanceof IndexedArrowVectorElementAccessPath iaveap) {
            QueryVariableType ordinalType = iaveap.getType();

            // We allocate a new variable and initialise its value based on whether the read needs
            // to be optimised or not
            String operandVariableName;
            if (ordinalType == QueryVariableType.S_FL_BIN) {
                // Allocate a global byte array cache variable to avoid allocations (initially null)
                operandVariableName = cCtx.defineQueryGlobalVariable(
                        "byte_array_cache",
                        createPrimitiveArrayType(getLocation(), Java.Primitive.BYTE),
                        new Java.NullLiteral(getLocation()),
                        false);

                // Perform an optimised read and assign the returned byte array to the variable
                // to ensure proper initialisation
                Java.Rvalue optimisedReadValue = iaveap.readFixedLengthOptimised(operandVariableName);
                codegenTarget.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), operandVariableName),
                                optimisedReadValue
                        )
                );

            } else if (ordinalType == QueryVariableType.S_VARCHAR) {
                // Allocate an array of global byte array caches to avoid allocations as much as possible
                // while keeping the var-charity. The array has space for one byte array cache upto the
                // maximum var char length in the database
                int maximumVarCharLength = cCtx.getDatabase().getMaximumVarCharColumnLength();
                String arrayOfCachesName = cCtx.defineQueryGlobalVariable(
                        "byte_array_caches",
                        createNestedPrimitiveArrayType(getLocation(), Java.Primitive.BYTE),
                        createNew2DPrimitiveArray(
                                getLocation(),
                                Java.Primitive.BYTE,
                                createIntegerLiteral(getLocation(), maximumVarCharLength)
                        ),
                        false
                );

                // Perform an optimised read and assign the returned byte array to a local reference variable
                operandVariableName = cCtx.defineVariable("ordinal_value");
                codegenTarget.add(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), ordinalType),
                                operandVariableName,
                                iaveap.readVarCharOptimised(arrayOfCachesName)
                        )
                );


            } else {
                // Allocate a local variable and assign it the value which is obtained via a generic read
                operandVariableName = cCtx.defineVariable("ordinal_value");
                codegenTarget.add(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), ordinalType),
                                operandVariableName,
                                iaveap.readGeneric()
                        )
                );

            }

            // Update the access path to reflect the allocation of the variable
            var newAccessPath = new ScalarVariableAccessPath(operandVariableName, ordinalType);

            // Return the access path code
            return new Pair<>(newAccessPath.read(), newAccessPath);

        } else {
            throw new IllegalStateException(
                    "CodeGenOperator.getRValueFromAccessPathNonVec does not support the provided access path");
        }
    }

    /**
     * Method to convert a {@link RexLiteral} to an appropriate {@link Java.Rvalue}.
     * @param literal The {@link RexLiteral} to convert.
     * @return The {@link Java.Rvalue} corresponding to {@code literal}.
     */
    Java.Rvalue rexLiteralToRvalue(RexLiteral literal) {
        // Simply return a literal corresponding to the operand
        BasicSqlType literalType = (BasicSqlType) literal.getType();

        return switch (literalType.getSqlTypeName()) {
            case DOUBLE -> createFloatingPointLiteral(getLocation(), literal.getValueAs(Double.class).toString());
            case FLOAT -> createFloatingPointLiteral(getLocation(), literal.getValueAs(Float.class).toString());
            case INTEGER -> createIntegerLiteral(getLocation(), literal.getValueAs(Integer.class).toString());
            case DECIMAL -> createFloatingPointLiteral(getLocation(), literal.getValueAs(BigDecimal.class).doubleValue());
            case CHAR -> {
                String value = Objects.requireNonNull(literal.getValueAs(NlsString.class)).getValue();
                byte[] valueBytes = new byte[literalType.getPrecision()];
                System.arraycopy(value.getBytes(StandardCharsets.US_ASCII), 0, valueBytes, 0, value.length());
                yield createInitialisedByteArray(getLocation(), valueBytes);
            }
            default -> throw new UnsupportedOperationException(
                    "FilterOperator.codeGenOperandNonVec does not support his literal type");
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
