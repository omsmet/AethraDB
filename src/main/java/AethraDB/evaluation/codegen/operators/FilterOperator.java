package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.util.language.AethraExpression;
import AethraDB.util.language.function.AethraBinaryFunction;
import AethraDB.util.language.function.AethraFunction;
import AethraDB.util.language.function.logic.AethraAndFunction;
import AethraDB.util.language.value.AethraInputRef;
import AethraDB.util.language.value.literal.AethraDateDayLiteral;
import AethraDB.util.language.value.literal.AethraDateIntervalLiteral;
import AethraDB.util.language.value.literal.AethraIntegerLiteral;
import AethraDB.util.language.value.literal.AethraLiteral;
import AethraDB.util.language.value.literal.AethraStringLiteral;
import org.codehaus.janino.Java;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DATE_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DATE_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DATE_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_DOUBLE;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT_DATE;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_FL_BIN;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrowVectorWithSelectionVectorType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrowVectorWithValidityMaskType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createInitialisedByteArray;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.eq;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.ge;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.gt;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.le;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * A {@link CodeGenOperator} which filters out records according to a given condition.
 */
public class FilterOperator extends CodeGenOperator {

    /**
     * The {@link CodeGenOperator} producing the records to be filtered by {@code this}.
     */
    private final CodeGenOperator child;

    /**
     * The {@link AethraExpression} encoding the filter condition to be implemented by {@code this}.
     */
    private final AethraExpression filterExpression;

    /**
     * Create a {@link FilterOperator} instance for a specific sub-query.
     * @param child The {@link CodeGenOperator} producing the records to be filtered.
     * @param filterExpression The filter condition to implement.
     */
    public FilterOperator(CodeGenOperator child, AethraExpression filterExpression) {
        this.child = child;
        this.child.setParent(this);
        this.filterExpression = filterExpression;
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
        return consumeNonVecOperator(cCtx, oCtx, this.filterExpression, true);
    }

    /**
     * Method to generate the required code on the non-vectorised backward code generation pass
     * based on the specific filter operator implemented by {@code this}.
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
            AethraExpression filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        if (!(filterOperator instanceof AethraFunction castFilterOperator))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeNonVec only supports AethraFunction conditions");

        // Important for all operators: since we are in the non-vectorised style, we must be processing
        // within a for-loop. Any record not matching the condition must therefore result in the
        // invocation of a continue statement to prevent the record from being processed.

        // Forward the generation obligation to the correct method based on the operator type.
        return switch (castFilterOperator.getKind()) {
            case AND -> consumeNonVecAndOperator(cCtx, oCtx, (AethraAndFunction) castFilterOperator, callParentConsumeOnMatch);
            case EQ, GT, GTE, LT, LTE -> consumeNonVecComparisonOperator(cCtx, oCtx, (AethraBinaryFunction) castFilterOperator, callParentConsumeOnMatch);
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
            AethraAndFunction filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the AND operands and generate code for each of them
        // If a record does not match the operand, it will invoke a continue statement
        for (AethraExpression operand : filterOperator.operands)
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
     * a comparison (>, >=, <, <=) operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The < operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecComparisonOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            AethraBinaryFunction filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the operator
        AethraFunction.Kind comparisonOp = filterOperator.getKind();
        if (comparisonOp != AethraFunction.Kind.EQ
                && comparisonOp != AethraFunction.Kind.GT
                && comparisonOp != AethraFunction.Kind.GTE
                && comparisonOp != AethraFunction.Kind.LT
                && comparisonOp != AethraFunction.Kind.LTE)
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeNonVecComparisonOperator does not support the provided comparison operator: " + comparisonOp);

        // Obtain the operands
        AethraExpression lhs = filterOperator.firstOperand;
        AethraExpression rhs = filterOperator.secondOperand;

        if (this.useSIMDNonVec(cCtx))
            throw new UnsupportedOperationException("FilterOperator.consumeNonVecComparisonOperator no longer supports SIMD");

        // Convert the operands
        Java.Rvalue lhsRvalue = codeGenOperandNonVec(cCtx, lhs, codegenResult);
        Java.Rvalue rhsRvalue = codeGenOperandNonVec(cCtx, rhs, codegenResult);

        // Generate the required control flow
        // if (!(lhsRvalue "operator" rhsRvalue))
        //     continue;
        codegenResult.add(
                createIfNotContinue(
                        getLocation(),
                        switch (comparisonOp) {
                            case EQ -> eq(getLocation(), lhsRvalue, rhsRvalue);
                            case GT -> gt(getLocation(), lhsRvalue, rhsRvalue);
                            case GTE -> ge(getLocation(), lhsRvalue, rhsRvalue);
                            case LT -> lt(getLocation(), lhsRvalue, rhsRvalue);
                            case LTE -> le(getLocation(), lhsRvalue, rhsRvalue);
                            default -> throw new UnsupportedOperationException(
                                    "FilterOperator.consumeNonVecComparisonOperator does not support the provided comparison operator");
                        }
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
     * @param operand The operand to generate code for.
     * @param target The code generation result to add code to if required for accessing the operand.
     * @return The {@link Java.Rvalue} corresponding to the operand.
     */
    private Java.Rvalue codeGenOperandNonVec(
            CodeGenContext cCtx,
            AethraExpression operand,
            List<Java.Statement> target
    ) {
        // Generate the required code based on the operand type
        if (operand instanceof AethraInputRef inputRef) {
            // RexInputRefs refer to a specific ordinal position in the result of the previous operator
            return getRValueFromOrdinalAccessPathNonVec(cCtx, inputRef.columnIndex, target);

        } else if (operand instanceof AethraLiteral literal) {
            return aethraLiteralToRvalue(literal);

        } else if (operand instanceof AethraBinaryFunction binaryFunction) {
            // If we receive a binary function in this location, we know (due to properties of the planner)
            // that we are actually dealing with a computation over constants
            // Current use-case is only for dates, so check that we are dealing with those
            if (binaryFunction.firstOperand instanceof AethraDateDayLiteral
                    || binaryFunction.firstOperand instanceof AethraDateIntervalLiteral
                    || binaryFunction.secondOperand instanceof AethraDateDayLiteral
                    || binaryFunction.secondOperand instanceof AethraDateIntervalLiteral) {
                return createIntegerLiteral(getLocation(), translateToUnixDay(operand));

            } else {
                throw new UnsupportedOperationException(
                        "FilterOperator.codeGenOperandNonVec does not support reducing the given binaryFunction: " + binaryFunction);
            }

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
        return consumeVecOperator(cCtx, oCtx, this.filterExpression, true);
    }

    /**
     * Method to generate the required code on the vectorised backward code generation pass based on
     * the specific filter operator implemented by {@code this}.
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
            AethraExpression filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        if (!(filterOperator instanceof AethraFunction castFilterOperator))
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecOperator only supports AethraFunction conditions");

        // Important for all operators: since we are in the vectorised style, we are processing within
        // a while loop which contains subsequent operators. To indicate the validity of a record within
        // a vector, we will thus be using selection vectors/validity vectors and therefore each
        // operator must support different AccessPath formats to deal with this accordingly.

        // Forward the generation obligation to the correct method based on the operator type.
        return switch (castFilterOperator.getKind()) {
            case AND -> consumeVecAndOperator(cCtx, oCtx, (AethraAndFunction) castFilterOperator, callParentConsumeOnMatch);
            case EQ, GT, GTE, LT, LTE ->
                    consumeVecComparisonOperator(cCtx, oCtx, (AethraBinaryFunction) castFilterOperator, callParentConsumeOnMatch);
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
            AethraAndFunction filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the AND operands and generate code for each of them
        // If a record does not match the operand, it will invoke a continue statement
        for (AethraExpression operand : filterOperator.operands)
            codegenResult.addAll(consumeVecOperator(cCtx, oCtx, operand, false));

        // The vectors which are in the getCurrentOrdinalMapping() will have validity markers attached
        // to them, so we can now invoke the parent if required to consume the remaining records in the result.
        if (callParentConsumeOnMatch)
            codegenResult.addAll(vecParentConsume(cCtx, oCtx));

        // Return the resulting code
        return codegenResult;
    }

    /**
     * Method to generate the required vectorised code on the backward code generation pass for
     * a comparison (>, >=, <, <=) operator.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param filterOperator The </<= operator to generate code for.
     * @param callParentConsumeOnMatch Whether the parent operator consume method should be invoked
     *                                 if this {@code filterOperator} matches. Necessary to allow
     *                                 recursive code generation.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeVecComparisonOperator(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            AethraBinaryFunction filterOperator,
            boolean callParentConsumeOnMatch
    ) {
        // Initialise the result
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Obtain the operator method name
        String operatorName = switch (filterOperator.getKind()) {
            case EQ -> "eq";
            case GT -> "gt";
            case GTE -> "ge";
            case LT -> "lt";
            case LTE -> "le";
            default -> throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecComparisonOperator does not support the provided comparison operator: " + filterOperator.getKind());
        };

        // Obtain the operands
        AethraExpression lhs = filterOperator.firstOperand;
        AethraExpression rhs = filterOperator.secondOperand;

        // Check if the operands match the expected format:
        // Vector < Scalar
        // Get the access path for the lhs input reference
        AccessPath lhsAP;
        if (lhs instanceof AethraInputRef lhsRef) {
            lhsAP = cCtx.getCurrentOrdinalMapping().get(lhsRef.columnIndex);

        } else throw new UnsupportedOperationException("FilterOperator.consumeVecComparisonOperator does not support this left-hand operand");

        Java.Rvalue rhsScalar;
        QueryVariableType rhsScalarType;
        if (rhs instanceof AethraDateDayLiteral rhsLit) {
            rhsScalar = createIntegerLiteral(getLocation(), rhsLit.unixDay);
            rhsScalarType = P_INT_DATE;

        } else if (rhs instanceof AethraIntegerLiteral rhsLit) {
            rhsScalar = createIntegerLiteral(getLocation(), rhsLit.value);
            rhsScalarType = P_INT;

        } else if (rhs instanceof AethraStringLiteral rhsLit) {
            rhsScalar = createInitialisedByteArray(getLocation(), rhsLit.value);
            rhsScalarType = S_FL_BIN;

        } else throw new UnsupportedOperationException("FilterOperator.consumeVecComparisonOperator does not support this right-hand operator");

        // Currently the filter operator only supports the below types, check this condition
        if (
                (
                   lhsAP.getType() != ARROW_DATE_VECTOR
                && lhsAP.getType() != ARROW_DATE_VECTOR_W_SELECTION_VECTOR
                && lhsAP.getType() != ARROW_DATE_VECTOR_W_VALIDITY_MASK
                && lhsAP.getType() != ARROW_DOUBLE_VECTOR
                && lhsAP.getType() != ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR
                && lhsAP.getType() != ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK
                && lhsAP.getType() != ARROW_FIXED_LENGTH_BINARY_VECTOR
                && lhsAP.getType() != ARROW_INT_VECTOR
                && lhsAP.getType() != ARROW_INT_VECTOR_W_SELECTION_VECTOR
                && lhsAP.getType() != ARROW_INT_VECTOR_W_VALIDITY_MASK
                )
                ||
                (
                   rhsScalarType != P_DOUBLE
                && rhsScalarType != P_INT
                && rhsScalarType != P_INT_DATE
                && rhsScalarType != S_FL_BIN
                )
        ) {
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecComparisonOperator does not supports this operand combination: "
                            + lhsAP.getType() + " - " + rhs.getClass());
        }

        if (this.useSIMDVec())
            throw new UnsupportedOperationException("FilterOperator.consumeVecComparisonOperator does no longer support SIMD");

        // Do a scan-surrounding allocation for the selection vector/validity mask that will result from this operator
        // int[] ordinal_[index]_sel_vec = cCtx.getAllocationManager().getIntVector()
        String selectionResultVariableName = cCtx.defineQueryGlobalVariable(
                "ordinal_" + lhsRef.columnIndex + "_sel_vec",
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
        ArrayAccessPath selectionResultAP = new ArrayAccessPath(selectionResultVariableName, P_A_INT);

        // Perform the actual selection using the appropriate vector support library (based on the
        // left-hand access path type and whether SIMD is enabled) and store the length of
        // the selection result (selection vector/validity mask) in a local variable
        ScalarVariableAccessPath selectionResultLengthAP = new ScalarVariableAccessPath(
                cCtx.defineVariable(selectionResultAP.getVariableName() + "_length"),
                P_INT
        );

        if (lhsAP instanceof ArrowVectorAccessPath lhsArrowVecAP && !this.useSIMDVec()) {
            // int ordinal_[index]_sel_vec_length = VectorisedFilterOperators.[operatorName](
            //      lhsArrowVecAP.read(), rhsIntScalar, ordinal_[index]_sel_vec);
            codegenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), selectionResultLengthAP.getType()),
                            selectionResultLengthAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedFilterOperators"),
                                    operatorName,
                                    new Java.Rvalue[] {
                                            lhsArrowVecAP.read(),
                                            rhsScalar,
                                            selectionResultAP.read()
                                    }
                            )
                    )
            );

        } else if (lhsAP instanceof ArrowVectorAccessPath lhsArrowVecAP && this.useSIMDVec()) {
            // int ordinal_[index]_val_mask_length = VectorisedFilterOperators.[operatorName]SIMD(
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
                                    operatorName + "SIMD",
                                    new Java.Rvalue[] {
                                            lhsArrowVecAP.read(),
                                            rhsScalar,
                                            selectionResultAP.read()
                                    }
                            )
                    )
            );

        } else if (lhsAP instanceof ArrowVectorWithSelectionVectorAccessPath lhsArrowVecWSAP && !this.useSIMDVec()) {
            // int ordinal_[index]_sel_vec_length = VectorisedFilterOperators.[operatorName](
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
                                    operatorName,
                                    new Java.Rvalue[] {
                                            lhsArrowVecWSAP.readArrowVector(),
                                            rhsScalar,
                                            selectionResultAP.read(),
                                            lhsArrowVecWSAP.readSelectionVector(),
                                            lhsArrowVecWSAP.readSelectionVectorLength()
                                    }
                            )
                    )
            );

        } else if (lhsAP instanceof ArrowVectorWithValidityMaskAccessPath lhsAvwvmAP && this.useSIMDVec()) {
            // int ordinal_[index]_val_mask_length = VectorisedFilterOperators.[operatorName]SIMD(
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
                                    operatorName + "SIMD",
                                    new Java.Rvalue[] {
                                            lhsAvwvmAP.readArrowVector(),
                                            rhsScalar,
                                            selectionResultAP.read(),
                                            lhsAvwvmAP.readValidityMask(),
                                            lhsAvwvmAP.readValidityMaskLength()
                                    }
                            )
                    )
            );

        } else {
            throw new UnsupportedOperationException(
                    "FilterOperator.consumeVecComparisonOperator does not support this left-hand access path and simd enabled combination");
        }

        // Update the current ordinal mapping to include the selection vector for all arrow vectors
        List<AccessPath> updatedOrdinalMapping = cCtx.getCurrentOrdinalMapping().stream().map(
                entry -> {
                    if (entry instanceof ArrowVectorAccessPath avapEntry && !this.useSIMDVec())
                        return new ArrowVectorWithSelectionVectorAccessPath(
                                avapEntry,
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithSelectionVectorType(avapEntry.getType()));
                    else if (entry instanceof ArrowVectorAccessPath avapEntry && this.useSIMDVec())
                        return new ArrowVectorWithValidityMaskAccessPath(
                                avapEntry,
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithValidityMaskType(avapEntry.getType()));
                    else if (entry instanceof ArrowVectorWithSelectionVectorAccessPath avwsvapEntry && !this.useSIMDVec())
                        return new ArrowVectorWithSelectionVectorAccessPath(
                                avwsvapEntry.getArrowVectorVariable(),
                                selectionResultAP,
                                selectionResultLengthAP,
                                arrowVectorWithSelectionVectorType(avwsvapEntry.getArrowVectorVariable().getType()));
                    else if (entry instanceof ArrowVectorWithValidityMaskAccessPath avwvmapEntry && this.useSIMDVec())
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

    /**
     * Method to translate a given date into an integer representing the UNIX day.
     * @param dateSpecification The specification to find the resulting UNIX day for.
     * @return The integer corresponding to the UNIX day represented by {@code dateSpecification}.
     */
    private int translateToUnixDay(AethraExpression dateSpecification) {
        if (dateSpecification instanceof AethraBinaryFunction dateComputation)
            return (int) translateToUnixDay(dateComputation).toEpochDay();
        else if (dateSpecification instanceof AethraDateDayLiteral dateDayLiteral)
            return (int) translateToUnixDay(dateDayLiteral).toEpochDay();
        else
            throw new UnsupportedOperationException(
                    "FilterOperator.translateToUnixDay does not support the provided dateSpecification");
    }

    /**
     * Method to translate a given date computation into a {@link LocalDate} representing the UNIX
     * day that results from simplifying the computation.
     * @param dateComputation The computation to find the resulting UNIX day for.
     * @return The {@link LocalDate} corresponding to the UNIX day represented by {@code dateComputation}.
     */
    private LocalDate translateToUnixDay(AethraBinaryFunction dateComputation) {
        // Get and translate the operand values
        AethraExpression lhsOperand = dateComputation.firstOperand;
        AethraExpression rhsOperand = dateComputation.secondOperand;

        // Check that the lhs operand is a literal
        if (!(lhsOperand instanceof AethraDateDayLiteral lhsDateDayLiteral))
            throw new UnsupportedOperationException(
                    "FilterOperator.translateToUnixDay expects the lhs operand of a date computation to be literal representing a date");
        LocalDate lhsDate = translateToUnixDay(lhsDateDayLiteral);

        // Check that the rhs operand is some interval specification
        // That is, a multiplication of an integer constant and an interval literal
        if (!(
                rhsOperand instanceof AethraBinaryFunction rhsIntervalConstruct
                && rhsIntervalConstruct.getKind() == AethraFunction.Kind.MULTIPLY
                && rhsIntervalConstruct.firstOperand instanceof AethraIntegerLiteral rhsIntervalCountLiteral
                && rhsIntervalConstruct.secondOperand instanceof AethraDateIntervalLiteral rhsIntervalIntervalLiteral
        ))
            throw new UnsupportedOperationException(
                    "FilterOperator.translateToUnixDay expects the rhs operand of a date computation to be an interval specification");

        // Get the amount of intervals to apply (i.e. x in x months, x years, etc.)
        int rhsIntervalCount = rhsIntervalCountLiteral.value;

        // Get the interval qualifier (i.e. months in x months, years in x years, etc.)
        AethraDateIntervalLiteral.Unit rhsIntervalUnit = rhsIntervalIntervalLiteral.unit;

        // Perform the actual computation based on the interval type and operator
        if (dateComputation.getKind() == AethraFunction.Kind.SUBTRACT) {
            return switch (rhsIntervalUnit) {
                case DAY -> lhsDate.minusDays(rhsIntervalCount);
                case MONTH -> lhsDate.minusMonths(rhsIntervalCount);
                case WEEK -> lhsDate.minusWeeks(rhsIntervalCount);
                case YEAR -> lhsDate.minusYears(rhsIntervalCount);
            };

        } else if (dateComputation.getKind() == AethraFunction.Kind.ADD) {
            return switch (rhsIntervalUnit) {
                case DAY -> lhsDate.plusDays(rhsIntervalCount);
                case MONTH -> lhsDate.plusMonths(rhsIntervalCount);
                case WEEK -> lhsDate.plusWeeks(rhsIntervalCount);
                case YEAR -> lhsDate.plusYears(rhsIntervalCount);
            };

        } else {
            throw new UnsupportedOperationException(
                    "FilterOperator.translateToUnixDay does not support the given dateComputation");
        }
    }

    /**
     * Method to translate a given date literal into a {@link LocalDate} the UNIX day.
     * @param dateLiteral The literal to find the resulting UNIX day for.
     * @return The {@link LocalDate} corresponding to the UNIX day represented by {@code dateLiteral}.
     */
    private LocalDate translateToUnixDay(AethraDateDayLiteral dateLiteral) {
        return LocalDate.ofEpochDay(dateLiteral.unixDay);
    }
}
