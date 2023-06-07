package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
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
     * @param child The {@link CodeGenOperator} producing the records to be filtered.
     */
    public FilterOperator(LogicalFilter filter, CodeGenOperator<?> child) {
        super(filter);
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

        // Convert the operands
        Java.Rvalue lhsRvalue = codeGenOperandNonVec(cCtx, oCtx, lhs, codegenResult);
        Java.Rvalue rhsRvalue = codeGenOperandNonVec(cCtx, oCtx, rhs, codegenResult);

        // Generate the required control flow
        // if (!(lhsRvalue < rhsRvalue))
        //     continue;
        codegenResult.add(
                createIfNotContinue(
                        getLocation(),
                        lt(getLocation(), lhsRvalue, rhsRvalue)
                )
        );

        // The condition matches. Invoke the parent consumption method if required.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(nonVecParentConsume(cCtx, oCtx));

        // Return the result
        return codegenResult;
    }

    /**
     * Generate code for a scalar operand in the non-vectorised code generation process.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param operand The operand to generate code for.
     * @param target The code generation result to add code to if required for accessing the operand.
     * @return The {@link Java.Rvalue} corresponding to the operand.
     */
    private Java.Rvalue codeGenOperandNonVec(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            RexNode operand,
            List<Java.Statement> target
    ) {
        // Generate the required code based on the operand type
        if (operand instanceof RexInputRef inputRef) {
            // RexInputRefs refer to a specific ordinal position in the result of the previous operator
            int ordinalIndex = inputRef.getIndex();
            Java.Type operandType = sqlTypeToScalarJavaType((BasicSqlType) operand.getType());
            return getRValueFromAccessPathNonVec(cCtx, oCtx, ordinalIndex, operandType, target);

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

        // Currently the filter operator only supports the INTEGER SQL type, check this condition
        if (lhs.getType().getSqlTypeName() != SqlTypeName.INTEGER
                || rhs.getType().getSqlTypeName() != SqlTypeName.INTEGER)
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecLtOperator only supports integer operands");

        // Check if the operands match the expected format:
        // Vector < Scalar (which means we need a RexInputRef < RexLiteral)
        if (!(lhs instanceof RexInputRef lhsRef) || !(rhs instanceof RexLiteral rhsLit))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecLtOperator does not support this operand combination");

        // Generate the Rvalue for the rhs integer scalar
        Java.Rvalue rhsIntScalar = rexLiteralToRvalue(rhsLit);

        // Check if left-hand operand matches one of the processing patterns:
        // - ArrowVectorAccessPath
        // - ArrowVectorWithSelectionVectorAccessPath
        AccessPath lhsAP = cCtx.getCurrentOrdinalMapping().get(lhsRef.getIndex());
        if (!(lhsAP instanceof ArrowVectorAccessPath || lhsAP instanceof ArrowVectorWithSelectionVectorAccessPath))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecLtOperator does not support this left-hand access path");

        // Handle the generic part of the code generation
        // Do a scan-surrounding allocation for the selection vector that will result from this operator
        // int[] ordinal_[index]_sel_vec = cCtx.getAllocationManager().getIntVector()
        String ordinalSelectionVectorVariableName = cCtx.defineScanSurroundingVariables(
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
                )
        );

        // Perform the actual selection using the appropriate vector support library (based on the
        // left-hand access path type) and store the length of the selection vector in a local variable
        String ordinalSelectionVectorLengthVariableName =
                cCtx.defineVariable(ordinalSelectionVectorVariableName + "_length");

        if (lhsAP instanceof ArrowVectorAccessPath lhsArrowVecAP) {
            // int ordinal_[index]_sel_vec_length = VectorisedFilterOperators.lessThan(
            //      lhsArrowVecAP.read(), rhsIntScalar, ordinal_[index]_sel_vec);
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveType(getLocation(), Java.Primitive.INT),
                            ordinalSelectionVectorLengthVariableName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(
                                            getLocation(),
                                            "evaluation.vector_support.VectorisedFilterOperators"
                                    ),
                                    "lessThan",
                                    new Java.Rvalue[] {
                                            lhsArrowVecAP.read(),
                                            rhsIntScalar,
                                            createAmbiguousNameRef(
                                                    getLocation(),
                                                    ordinalSelectionVectorVariableName
                                            )
                                    }
                            )
                    )
            );

        } else { // lhsAP instanceof ArrowVectorWithSelectionVectorAccessPath lhsArrowVecWSAP
            var lhsArrowVecWSAP = (ArrowVectorWithSelectionVectorAccessPath) lhsAP;
            // int rdinal_[index]_sel_vec_length = VectorisedFilterOperators.lessThan(
            //      lhsArrowVecWSAP.readArrowVector(), rhsIntScalar, ordinal_[index]_sel_vec,
            //      lhsArrowVecWSAP.readSelectionVector(), lhsArrowVecWSAP.readSelectionVectorLength());
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveType(getLocation(), Java.Primitive.INT),
                            ordinalSelectionVectorLengthVariableName,
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(
                                            getLocation(),
                                            "evaluation.vector_support.VectorisedFilterOperators"
                                    ),
                                    "lessThan",
                                    new Java.Rvalue[] {
                                            lhsArrowVecWSAP.readArrowVector(),
                                            rhsIntScalar,
                                            createAmbiguousNameRef(
                                                    getLocation(),
                                                    ordinalSelectionVectorVariableName
                                            ),
                                            lhsArrowVecWSAP.readSelectionVector(),
                                            lhsArrowVecWSAP.readSelectionVectorLength()
                                    }
                            )
                    )
            );
        }

        // Update the current ordinal mapping to include the selection vector for all arrow vectors
        List<AccessPath> updatedOrdinalMapping = cCtx.getCurrentOrdinalMapping().stream().map(
                entry -> {
                    if (entry instanceof ArrowVectorAccessPath avapEntry)
                        return (AccessPath) new ArrowVectorWithSelectionVectorAccessPath(
                                avapEntry.getVariableName(),
                                ordinalSelectionVectorVariableName,
                                ordinalSelectionVectorLengthVariableName);
                    else if (entry instanceof ArrowVectorWithSelectionVectorAccessPath avwsvapEntry)
                        return (AccessPath) new ArrowVectorWithSelectionVectorAccessPath(
                                avwsvapEntry.getArrowVectorVariable(),
                                ordinalSelectionVectorVariableName,
                                ordinalSelectionVectorLengthVariableName
                        );
                    else
                        throw new UnsupportedOperationException(
                                "We expected all ordinals to be of a vector type");
                }).toList();
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Invoke the parent consumption method if required.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(vecParentConsume(cCtx, oCtx));

        // Return the result
        return codegenResult;
    }

}
