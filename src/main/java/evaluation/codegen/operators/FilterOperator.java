package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDVectorMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_VALIDITY_MASK;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_BOOLEAN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.VECTOR_INT_MASKED;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrowVectorWithSelectionVectorType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrowVectorWithValidityMaskType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveMemberTypeForArrowVector;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * A {@link CodeGenOperator} which filters out records according to a given condition.
 */
public class FilterOperator extends CodeGenOperator<LogicalFilter> {

    /**
     * The {@link CodeGenOperator} producing the records to be filtered by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * Create a {@link FilterOperator} instance for a specific sub-query.
     * @param filter The logical filter (and sub-query) for which the operator is created.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @param child The {@link CodeGenOperator} producing the records to be filtered.
     */
    public FilterOperator(LogicalFilter filter, boolean simdEnabled, CodeGenOperator<?> child) {
        super(filter, simdEnabled);
        this.child = child;
        this.child.setParent(this);
    }

    @Override
    public boolean canProduceNonVectorised() {
        return this.child.canProduceNonVectorised();
    }

    @Override
    public boolean canProduceVectorised() {
        return this.child.canProduceVectorised();
    }

    @Override
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceNonVec(cCtx, oCtx);
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Obtain and process the filter condition
        RexNode filterConditionRaw = this.getLogicalSubplan().getCondition();
        return consumeNonVecOperator(cCtx, oCtx, filterConditionRaw, true);
    }

    /**
     * Method to generate the required code on the non-vectorised backward code generation pass
     * based on the specific filter operator used by the {@link LogicalFilter}.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 this method to generate the required code in a recursive fashion.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexNode filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        if (!(filterOperator instanceof RexCall castFilterOperator))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeNonVec only supports RexCall conditions");

        // Important for all operators: since we are in the non-vectorised style, we must be processing
        // within a for-loop. Any record not matching the condition must therefore result in the
        // invocation of a continue statement to prevent the record from being processed.

        // Forward the generation obligation to the correct method based on the operator type.
        return switch (castFilterOperator.getKind()) {
            case AND -> consumeNonVecAndOperator(cCtx, oCtx, castFilterOperator, callParentConsumeOnMatch);
            case LESS_THAN -> consumeNonVecLtOperator(cCtx, oCtx, castFilterOperator, callParentConsumeOnMatch);
            default -> throw new UnsupportedOperationException(
                    "FilterOperator.consumeNonVecOperator does not support this operator type");
        };
    }

    /**
     * Method to generate the required non-vectorised code on the backward code generation pass for
     * an AND operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The AND operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecAndOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexCall filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the AND operands and generate code for each of them
        // If a record does not match the operand, it will invoke a continue statement
        for (RexNode operand : filterOperator.getOperands())
            codegenResult.addAll(consumeNonVecOperator(cCtx, oCtx, operand, false));

        // Any record which processes beyond the above operands in the code-gen flow, matches the
        // conjunctive condition. Invoke the parent if required to consume the result.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(nonVecParentConsume(cCtx, oCtx));

        // Return the resulting code
        return codegenResult;
    }

    /**
     * Method to generate the required non-vectorised code on the backward code generation pass for
     * a < operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The < operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecLtOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexCall filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the operands
        RexNode lhs = filterOperator.getOperands().get(0);
        RexNode rhs = filterOperator.getOperands().get(1);

        // Check if we are in a SIMD enabled setting with a SIMD-compatible access path
        // or if we can simply perform a scalar code-gen path.
        // Currently, the only SIMD supported path is a lhs RexInputRef and rhs RexLiteral
        if (this.simdEnabled
                && lhs instanceof RexInputRef lhsRef && rhs instanceof RexLiteral rhsLit
                && cCtx.getCurrentOrdinalMapping().get(lhsRef.getIndex()) instanceof SIMDLoopAccessPath lhsAP) {
            // SIMD enabled path: handle acceleration according to the datatype
            if (lhsAP.getType() == VECTOR_INT_MASKED && rhsLit.getType().getSqlTypeName() == SqlTypeName.INTEGER) { // getSqlTypeName allowed here since we are accessing a query literal
                // Extend the SIMD validity mask using a SIMD comparison
                // IntVector [SIMDVector] = IntVector.fromSegment(
                //      [lhsAP.readVectorSpecies()],
                //      [lhsAP.readMemorySegment()],
                //      [lhsAP.readArrowVectorOffset()] * [lhsAP.readArrowVector().TYPE_WIDTH],
                //      java.nio.ByteOrder.LITTLE_ENDIAN,
                //      [lhsAP.readSIMDMask()]
                // );
                String SIMDVectorName = cCtx.defineVariable("SIMDVector");
                codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), lhsAP.getType()),
                            SIMDVectorName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "oCtx"),
                                    "createIntVector",
                                    new Java.Rvalue[] {
                                            lhsAP.readVectorSpecies(),
                                            lhsAP.readMemorySegment(),
                                            mul(
                                                    getLocation(),
                                                    lhsAP.readArrowVectorOffset(),
                                                    createAmbiguousNameRef(
                                                            getLocation(),
                                                            lhsAP.getArrowVectorAccessPath().getVariableName() + ".TYPE_WIDTH"
                                                    )
                                            ),
                                            createAmbiguousNameRef(getLocation(), "java.nio.ByteOrder.LITTLE_ENDIAN"),
                                            lhsAP.readSIMDMask()
                                    }
                            )
                    )
                );

                // Do the comparison and mask extension
                // VectorMask<Integer> [SIMDVector]_sel_mask = [SIMDVector].compare(VectorOperators.LT, [rhsLit], [lhsAP.readSIMDMask()])
                String SIMDVectorSelMaskName = cCtx.defineVariable(SIMDVectorName + "_sel_mask");
                QueryVariableType SIMDVectorSelMaskNameQueryType = lhsAP.getSIMDValidityMaskAccessPath().getType();
                codegenResult.add(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), SIMDVectorSelMaskNameQueryType),
                                SIMDVectorSelMaskName,
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), SIMDVectorName),
                                        "compare",
                                        new Java.Rvalue[] {
                                                createAmbiguousNameRef(getLocation(), "jdk.incubator.vector.VectorOperators.LT"),
                                                codeGenOperandNonVec(cCtx, rhs, codegenResult),
                                                lhsAP.readSIMDMask()
                                        }
                                )
                        )
                );

                // Update the ordinal mapping so it reflects the new validity mask
                List<AccessPath> updatedOrdinalMapping = cCtx.getCurrentOrdinalMapping().stream().map(
                        entry -> {
                            if (entry instanceof SIMDLoopAccessPath slAP)
                                return (AccessPath) new SIMDLoopAccessPath(
                                        slAP.getArrowVectorAccessPath(),
                                        slAP.getArrowVectorLengthAccessPath(),
                                        slAP.getCurrentArrowVectorOffsetAccessPath(),
                                        slAP.getSIMDVectorLengthAccessPath(),
                                        new SIMDVectorMaskAccessPath(SIMDVectorSelMaskName, SIMDVectorSelMaskNameQueryType),
                                        slAP.getMemorySegmentAccessPath(),
                                        slAP.getVectorSpeciesAccessPath(),
                                        slAP.getType()
                                );
                            else
                                throw new UnsupportedOperationException(
                                        "Unsupported AccessPath for SIMD handling");
                        }).toList();
                cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

            } else {
                throw new UnsupportedOperationException("FilterOperator.consumeNonVec is not able to" +
                        "handle this operand combination under SIMD acceleration");
            }
        } else {
            // Scalar path
            // Convert the operands
            Java.Rvalue lhsRvalue = codeGenOperandNonVec(cCtx, lhs, codegenResult);
            Java.Rvalue rhsRvalue = codeGenOperandNonVec(cCtx, rhs, codegenResult);

            // Generate the required control flow
            // if (!(lhsRvalue < rhsRvalue))
            //     continue;
            codegenResult.add(
                    createIfNotContinue(
                            getLocation(),
                            lt(getLocation(), lhsRvalue, rhsRvalue)
                    )
            );
        }

        // The condition matches. Invoke the parent consumption method if required.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(nonVecParentConsume(cCtx, oCtx));

        // Return the result
        return codegenResult;
    }

    /**
     * Generate code for a scalar operand in the non-vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param operand The operand to generate code for.
     * @param target The code generation result to add code to if required for accessing the operand.
     * @return The {@link Java.Rvalue} corresponding to the operand.
     */
    private Java.Rvalue codeGenOperandNonVec(
            CodeGenContext cCtx,
            RexNode operand,
            List<Java.Statement> target
    ) {
        // Generate the required code based on the operand type
        if (operand instanceof RexInputRef inputRef) {
            // RexInputRefs refer to a specific ordinal position in the result of the previous operator
            int ordinalIndex = inputRef.getIndex();
            return getRValueFromAccessPathNonVec(cCtx, ordinalIndex, target);

        } else if (operand instanceof RexLiteral literal) {
            return rexLiteralToRvalue(literal);

        } else {
            throw new UnsupportedOperationException("FilterOperator.codeGenOperandNonVec does not support the provide operand");
        }
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceVec(cCtx, oCtx);
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Obtain and process the filter condition
        RexNode filterConditionRaw = this.getLogicalSubplan().getCondition();
        return consumeVecOperator(cCtx, oCtx, filterConditionRaw, true);
    }

    /**
     * Method to generate the required code on the vectorised backward code generation pass based on
     * the specific filter operator used by the {@link LogicalFilter}.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 this method to generate the required code in a recursive fashion.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeVecOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexNode filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        if (!(filterOperator instanceof RexCall castFilterOperator))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecOperator only supports RexCall conditions");

        // Important for all operators: since we are in the vectorised style, we are processing within
        // a while loop which contains subsequent operators. To indicate the validity of a record within
        // a vector, we will thus be using selection vectors/validity vectors and therefore each
        // operator must support different AccessPath formats to deal with this accordingly.

        // Forward the generation obligation to the correct method based on the operator type.
        return switch (castFilterOperator.getKind()) {
            case AND -> consumeVecAndOperator(cCtx, oCtx, castFilterOperator, callParentConsumeOnMatch);
            case LESS_THAN -> consumeVecLtOperator(cCtx, oCtx, castFilterOperator, callParentConsumeOnMatch);
            default -> throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecOperator does not support this operator type");
        };
    }

    /**
     * Method to generate the required vectorised code on the backward code generation pass for an
     * AND operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The AND operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeVecAndOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexCall filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the AND operands and generate code for each of them
        // If a record does not match the operand, it will invoke a continue statement
        for (RexNode operand : filterOperator.getOperands())
            codegenResult.addAll(consumeVecOperator(cCtx, oCtx, operand, false));

        // The vectors which are in the getCurrentOrdinalMapping() will have validity markers attached
        // to them, so we can now invoke the parent if required to consume the remaining records in the result.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(vecParentConsume(cCtx, oCtx));

        // Return the resulting code
        return codegenResult;
    }

    /**
     * Method to generate the required vectorised code on the backward code generation pass for a
     * < operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The < operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeVecLtOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexCall filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the operands
        RexNode lhs = filterOperator.getOperands().get(0);
        RexNode rhs = filterOperator.getOperands().get(1);

        // Check if the operands match the expected format:
        // Vector < Scalar (which means we need a RexInputRef < RexLiteral)
        if (!(lhs instanceof RexInputRef lhsRef) || !(rhs instanceof RexLiteral rhsLit))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecLtOperator does not support this operand combination");

        // Get the access path for the lhs input reference
        AccessPath lhsAP = cCtx.getCurrentOrdinalMapping().get(lhsRef.getIndex());

        // Currently the filter operator only supports the integer type, check this condition
        if (
                (  lhsAP.getType() != ARROW_INT_VECTOR
                && lhsAP.getType() != ARROW_INT_VECTOR_W_SELECTION_VECTOR
                && lhsAP.getType() != ARROW_INT_VECTOR_W_VALIDITY_MASK
                )
                || rhs.getType().getSqlTypeName() != SqlTypeName.INTEGER) {
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecLtOperator does not supports this operand combination: "
                            + lhsAP.getType() + " - " + rhs.getType().toString());
        }

        // Generate the Rvalue for the rhs integer scalar
        Java.Rvalue rhsIntScalar = rexLiteralToRvalue(rhsLit);

        // Handle the generic part of the code generation based on whether SIMD is enabled
        // Do a scan-surrounding allocation for the selection vector/validity mask that will result from this operator
        ArrayAccessPath selectionResultAP;
        if (this.simdEnabled) {
            // boolean[] ordinal_[index]_val_mask = cCtx.getAllocationManager().getBooleanVector()
            String selectionResultVariableName = cCtx.defineScanSurroundingVariables(
                    "ordinal_" + lhsRef.getIndex() + "_val_mask",
                    createPrimitiveArrayType(getLocation(), Java.Primitive.BOOLEAN),
                    createMethodInvocation(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "getBooleanVector"
                    ),
                    true
            );
            selectionResultAP = new ArrayAccessPath(selectionResultVariableName, P_A_BOOLEAN);
        } else {
            // int[] ordinal_[index]_sel_vec = cCtx.getAllocationManager().getIntVector()
            String selectionResultVariableName = cCtx.defineScanSurroundingVariables(
                    "ordinal_" + lhsRef.getIndex() + "_sel_vec",
                    createPrimitiveArrayType(getLocation(), Java.Primitive.INT),
                    createMethodInvocation(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "getIntVector"
                    ),
                    true
            );
            selectionResultAP = new ArrayAccessPath(selectionResultVariableName, P_A_INT);
        }

        // Perform the actual selection using the appropriate vector support library (based on the
        // left-hand access path type and whether SIMD is enabled) and store the length of
        // the selection result (selection vector/validity mask) in a local variable
        ScalarVariableAccessPath selectionResultLengthAP = new ScalarVariableAccessPath(
                cCtx.defineVariable(selectionResultAP.getVariableName() + "_length"),
                P_INT
        );

        if (lhsAP instanceof ArrowVectorAccessPath lhsArrowVecAP && !this.simdEnabled) {
            // int ordinal_[index]_sel_vec_length = VectorisedFilterOperators.lessThan(
            //      lhsArrowVecAP.read(), rhsIntScalar, ordinal_[index]_sel_vec);
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), selectionResultLengthAP.getType()),
                            selectionResultLengthAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedFilterOperators"),
                                    "lessThan",
                                    new Java.Rvalue[]{
                                            lhsArrowVecAP.read(),
                                            rhsIntScalar,
                                            selectionResultAP.read()
                                    }
                            )
                    )
            );

        } else if (lhsAP instanceof ArrowVectorAccessPath lhsArrowVecAP && this.simdEnabled) {
            // int ordinal_[index]_val_mask_length = VectorisedFilterOperators.lessThanSIMD(
            //      lhsArrowVecAP.read(), rhsIntScalar, ordinal_[index]_val_mask);
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), selectionResultLengthAP.getType()),
                            selectionResultLengthAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(
                                            getLocation(),
                                            "VectorisedFilterOperators"
                                    ),
                                    "lessThanSIMD",
                                    new Java.Rvalue[] {
                                            lhsArrowVecAP.read(),
                                            rhsIntScalar,
                                            selectionResultAP.read()
                                    }
                            )
                    )
            );

        } else if (lhsAP instanceof ArrowVectorWithSelectionVectorAccessPath lhsArrowVecWSAP && !this.simdEnabled) {
            // int ordinal_[index]_sel_vec_length = VectorisedFilterOperators.lessThan(
            //      lhsArrowVecWSAP.readArrowVector(), rhsIntScalar, ordinal_[index]_sel_vec,
            //      lhsArrowVecWSAP.readSelectionVector(), lhsArrowVecWSAP.readSelectionVectorLength());
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), selectionResultLengthAP.getType()),
                            selectionResultLengthAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(),"VectorisedFilterOperators"),
                                    "lessThan",
                                    new Java.Rvalue[] {
                                            lhsArrowVecWSAP.readArrowVector(),
                                            rhsIntScalar,
                                            selectionResultAP.read(),
                                            lhsArrowVecWSAP.readSelectionVector(),
                                            lhsArrowVecWSAP.readSelectionVectorLength()
                                    }
                            )
                    )
            );

        } else if (lhsAP instanceof ArrowVectorWithValidityMaskAccessPath lhsAvwvmAP && this.simdEnabled) {
            // int ordinal_[index]_val_mask_length = VectorisedFilterOperators.lessThanSIMD(
            //      lhsAvwvmAP.readArrowVector(), rhsIntScalar, ordinal_[index]_val_mask,
            //      lhsAvwvmAP.readValidityMask(), lhsAvwvmAP.readValidityMaskLength());
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), selectionResultLengthAP.getType()),
                            selectionResultLengthAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedFilterOperators"),
                                    "lessThanSIMD",
                                    new Java.Rvalue[] {
                                            lhsAvwvmAP.readArrowVector(),
                                            rhsIntScalar,
                                            selectionResultAP.read(),
                                            lhsAvwvmAP.readValidityMask(),
                                            lhsAvwvmAP.readValidityMaskLength()
                                    }
                            )
                    )
            );

        } else {
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecLtOperator does not support this left-hand access path and simd enabled combination");
        }

        // Update the current ordinal mapping to include the selection vector for all arrow vectors
        List<AccessPath> updatedOrdinalMapping = cCtx.getCurrentOrdinalMapping().stream().map(
                entry -> {
                    if (entry instanceof ArrowVectorAccessPath avapEntry && !this.simdEnabled)
                        return new ArrowVectorWithSelectionVectorAccessPath(
                                avapEntry,
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithSelectionVectorType(avapEntry.getType()));
                    else if (entry instanceof ArrowVectorAccessPath avapEntry && this.simdEnabled)
                        return new ArrowVectorWithValidityMaskAccessPath(
                                avapEntry,
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithValidityMaskType(avapEntry.getType()));
                    else if (entry instanceof ArrowVectorWithSelectionVectorAccessPath avwsvapEntry && !this.simdEnabled)
                        return new ArrowVectorWithSelectionVectorAccessPath(
                                avwsvapEntry.getArrowVectorVariable(),
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithSelectionVectorType(avwsvapEntry.getArrowVectorVariable().getType()));
                    else if (entry instanceof ArrowVectorWithValidityMaskAccessPath avwvmapEntry && this.simdEnabled)
                        return new ArrowVectorWithValidityMaskAccessPath(
                                avwvmapEntry.getArrowVectorVariable(),
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithValidityMaskType(avwvmapEntry.getArrowVectorVariable().getType()));
                    else
                        throw new UnsupportedOperationException(
                                "We expected all ordinals to be of specific vector types");
                }).toList();
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Invoke the parent consumption method if required.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(vecParentConsume(cCtx, oCtx));

        // Return the result
        return codegenResult;
    }

}
