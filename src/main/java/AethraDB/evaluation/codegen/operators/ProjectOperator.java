package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen;
import AethraDB.util.language.AethraExpression;
import AethraDB.util.language.function.AethraBinaryFunction;
import AethraDB.util.language.function.AethraFunction;
import AethraDB.util.language.function.logic.AethraCaseFunction;
import AethraDB.util.language.value.AethraInputRef;
import AethraDB.util.language.value.literal.AethraDoubleLiteral;
import AethraDB.util.language.value.literal.AethraIntegerLiteral;
import AethraDB.util.language.value.literal.AethraLiteral;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.util.language.function.AethraFunction.Kind.DIVIDE;
import static AethraDB.util.language.function.AethraFunction.Kind.EQ;

/**
 * A {@link CodeGenOperator} which project out records according to a given criterion.
 */
public class ProjectOperator extends CodeGenOperator {

    /**
     * The {@link CodeGenOperator} producing the records to be projected by {@code this}.
     */
    private final CodeGenOperator child;

    /**
     * The {@link AethraExpression}s prescribing the projections to be implemented by {@code} this.
     */
    private final AethraExpression[] projectionExpressions;

    /**
     * Create a {@link ProjectOperator} instance for a specific sub-query.
     * @param child The {@link CodeGenOperator} producing the records to be projected.
     * @param projections The {@link AethraExpression}s defining the projection to be performed.
     */
    public ProjectOperator(CodeGenOperator child, AethraExpression[] projections) {
        this.child = child;
        this.child.setParent(this);
        this.projectionExpressions = projections;
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
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Handle each projection expression separately, to update the ordinal mapping to the correct state
        List<AccessPath> updatedOrdinalMapping = new ArrayList<>(this.projectionExpressions.length);
        for (int i = 0; i < this.projectionExpressions.length; i++) {
            AethraExpression projectionExp = this.projectionExpressions[i];
             updatedOrdinalMapping.add(i, generateProjectionCode(cCtx, projectionExp, codeGenResult, false));
        }

        // Set the updated ordinal mapping
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Consume the parent operator
        codeGenResult.addAll(nonVecParentConsume(cCtx, oCtx));

        // Return the resulting code
        return codeGenResult;
    }

    /**
     * Method to generate a {@link AccessPath} (and supporting code) that implements a given
     * projection expression.
     * @param cCtx The {@link CodeGenContext} to use in the generation of the projection expression.
     * @param projectionExpression The projection expression to generate code for.
     * @param codeGenResult The list to add generated code to that backs the returned {@link AccessPath}.
     * @param vectorised Whether the code supporting the {@link AccessPath} should be generated in
     *                   a vectorised fashion or not.
     * @return The {@link AccessPath} to the "result" of the provided {@code projectionExpression}.
     */
    private AccessPath generateProjectionCode(
            CodeGenContext cCtx,
            AethraExpression projectionExpression,
            List<Java.Statement> codeGenResult,
            boolean vectorised
    ) {
        // Check the projection expression type
        if (projectionExpression instanceof AethraLiteral alExpr) {
            // Simply need to give access to the literal value of the expression
            QueryVariableType translatedLiteralType;
            Java.Rvalue translatedLiteralValue;

            if (alExpr instanceof AethraDoubleLiteral adlExpr) {
                translatedLiteralType = QueryVariableType.P_DOUBLE;
                if (Double.isNaN(adlExpr.value))
                    translatedLiteralValue = JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Double.NaN");
                else
                    translatedLiteralValue = JaninoGeneralGen.createFloatingPointLiteral(JaninoGeneralGen.getLocation(), adlExpr.value);

            } else if (alExpr instanceof AethraIntegerLiteral ailExpr) {
                translatedLiteralType = QueryVariableType.P_INT;
                translatedLiteralValue = JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), ailExpr.value);

            } else {
                throw new UnsupportedOperationException(
                        "ProjectOperator.generateProjectionCode does not support the provided literal type: " + alExpr.getClass());
            }

            ScalarVariableAccessPath literalVariableAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("projection_literal"),
                    translatedLiteralType
            );

            codeGenResult.add(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), literalVariableAP.getType()),
                            literalVariableAP.getVariableName(),
                            translatedLiteralValue
                    )
            );

            return literalVariableAP;

        } else if (projectionExpression instanceof AethraInputRef airExpr) {
            // Simply need to project out a column already present in the ordinal mapping
            return cCtx.getCurrentOrdinalMapping().get(airExpr.columnIndex);

        } else if (projectionExpression instanceof AethraFunction afExpr) {
            return vectorised
                    ? createVecComputationCode(cCtx, afExpr, codeGenResult)
                    : createNonVecComputationCode(cCtx, afExpr, codeGenResult);

        } else {
            throw new UnsupportedOperationException(
                    "ProjectOperator.generateProjectionCode does not support the current projection expression");
        }
    }

    /**
     * Method to generate non-vectorised code for projections that implement an actual computation.
     * @param cCtx The {@link CodeGenContext} to use in the generation of the projection expression.
     * @param computationExpression The projection (i.e. computation) expression to generate code for.
     * @param codeGenResult The list to add generated code to that backs the returned {@link AccessPath}.
     * @return The {@link AccessPath} to the "result" of the provided {@code computationExpression}.
     */
    private AccessPath createNonVecComputationCode(
            CodeGenContext cCtx,
            AethraFunction computationExpression,
            List<Java.Statement> codeGenResult
    ) {
        if (this.useSIMDNonVec(cCtx))
            throw new UnsupportedOperationException("ProjectOperator.createNonVecComputationCode does not support SIMD code generation");

        if (computationExpression instanceof AethraBinaryFunction aethraBinaryFunction) {
            // Deal with recursive "projections" first
            AccessPath lhopResult = this.generateProjectionCode(cCtx, aethraBinaryFunction.firstOperand, codeGenResult, false);
            QueryVariableType primLhsResType = QueryVariableTypeMethods.primitiveType(lhopResult.getType());
            AccessPath rhopResult = this.generateProjectionCode(cCtx, aethraBinaryFunction.secondOperand, codeGenResult, false);
            QueryVariableType primRhsResType = QueryVariableTypeMethods.primitiveType(rhopResult.getType());

            // Compute the result type first
            QueryVariableType primitiveReturnType;
            if (aethraBinaryFunction.getKind() == DIVIDE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == QueryVariableType.P_DOUBLE && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == QueryVariableType.P_INT && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == QueryVariableType.P_INT && primRhsResType == QueryVariableType.P_INT)
                primitiveReturnType = QueryVariableType.P_INT;
            else
                throw new UnsupportedOperationException("ProjectOperator.createNonVecComputationCode could not determine desired return type");

            // Now "combine" the results from the operands depending on the operator and the result access paths
            // Obtain r-values for the operands
            Java.Rvalue lhsRValue, rhsRValue;

            if (lhopResult instanceof ScalarVariableAccessPath lhopResultSVAP)
                lhsRValue = lhopResultSVAP.read();
            else if (lhopResult instanceof IndexedArrowVectorElementAccessPath lhopResultIAVEA)
                lhsRValue = getRValueFromAccessPathNonVec(cCtx, lhopResultIAVEA, codeGenResult).getLeft();
            else
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputation does not support this lhopResult AccessPath");

            if (rhopResult instanceof ScalarVariableAccessPath rhopResultSVAP)
                rhsRValue = rhopResultSVAP.read();
            else if (rhopResult instanceof IndexedArrowVectorElementAccessPath rhopResultIAVEA)
                rhsRValue = getRValueFromAccessPathNonVec(cCtx, rhopResultIAVEA, codeGenResult).getLeft();
            else
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputation does not support this rhopResult AccessPath");

            // Create a new scalar variable to store the computation result in
            ScalarVariableAccessPath resultPath = new ScalarVariableAccessPath(
                    cCtx.defineVariable("projection_computation_result"),
                    primitiveReturnType
            );

            // Generate the code for the actual computation
            Java.Rvalue operatorComputation = switch (aethraBinaryFunction.getKind()) {
                case MULTIPLY -> JaninoOperatorGen.mul(
                        JaninoGeneralGen.getLocation(),
                        lhsRValue,
                        rhsRValue
                );
                case ADD -> JaninoOperatorGen.plus(
                        JaninoGeneralGen.getLocation(),
                        lhsRValue,
                        rhsRValue
                );
                case SUBTRACT -> JaninoOperatorGen.sub(
                        JaninoGeneralGen.getLocation(),
                        lhsRValue,
                        rhsRValue
                );
                case DIVIDE -> JaninoOperatorGen.div(
                        JaninoGeneralGen.getLocation(),
                        (primLhsResType != QueryVariableType.P_DOUBLE)
                                ? JaninoGeneralGen.createCast(JaninoGeneralGen.getLocation(), QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), QueryVariableType.P_DOUBLE), lhsRValue)
                                : lhsRValue,
                        rhsRValue
                );
                default -> throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputation does not support the provided operator type");
            };

            codeGenResult.add(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), primitiveReturnType),
                            resultPath.getVariableName(),
                            operatorComputation
                    )
            );

            return resultPath;

        } else if (computationExpression instanceof AethraCaseFunction aethraCaseFunction) {
            // Only supports the if-then-else paradigm which is already checked by the AethraCaseFunction instantiation
            AethraFunction caseCondition = aethraCaseFunction.ifExpression;
            if (!(caseCondition instanceof AethraBinaryFunction binaryCondition && binaryCondition.getKind() == EQ))
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputationCode only supports CASE operators using the binary = operator");

            // Translate the result of the case branches
            AccessPath tbResult = this.generateProjectionCode(cCtx, aethraCaseFunction.trueValue, codeGenResult, false);
            QueryVariableType primitiveTbResultType = QueryVariableTypeMethods.primitiveType(tbResult.getType());
            AccessPath fbResult = this.generateProjectionCode(cCtx, aethraCaseFunction.falseValue, codeGenResult, false);
            QueryVariableType primitiveFbResultType = QueryVariableTypeMethods.primitiveType(fbResult.getType());

            // And obtain the r-values corresponding to these values
            Java.Rvalue tbResultRvalue, fbResultRvalue;

            if (tbResult instanceof ScalarVariableAccessPath tbSvap)
                tbResultRvalue = tbSvap.read();
            else
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputation does not support this true-branch AP for the CASE operator");

            if (fbResult instanceof ScalarVariableAccessPath fbSvap)
                fbResultRvalue = fbSvap.read();
            else
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputation does not support this false-branch AP for the CASE operator");

            // Compute the result type
            QueryVariableType primitiveReturnType;
            if (primitiveTbResultType == QueryVariableType.P_DOUBLE && primitiveFbResultType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primitiveTbResultType == QueryVariableType.P_INT && primitiveFbResultType == QueryVariableType.P_INT)
                primitiveReturnType = QueryVariableType.P_INT;
            else if (primitiveTbResultType == QueryVariableType.P_INT && primitiveFbResultType == QueryVariableType.P_LONG)
                primitiveReturnType = QueryVariableType.P_LONG;
            else
                throw new UnsupportedOperationException("ProjectOperator.createNonVecComputation could not determine the return type");

            // Resolve the lhs and rhs of the equality comparison
            AccessPath lhsCompAp = this.generateProjectionCode(cCtx, binaryCondition.firstOperand, codeGenResult, false);
            AccessPath rhsCompAp = this.generateProjectionCode(cCtx, binaryCondition.secondOperand, codeGenResult, false);

            // And get the r-values corresponding to the operands
            Java.Rvalue lhsCompRValue, rhsCompRValue;

            if (lhsCompAp instanceof ScalarVariableAccessPath lhsCompSvap)
                lhsCompRValue = lhsCompSvap.read();
            else
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputation does not support this lhsCompAp AccessPath");

            if (rhsCompAp instanceof ScalarVariableAccessPath rhsCompSvap)
                rhsCompRValue = rhsCompSvap.read();
            else
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputation does not support this rhsCompAp AccessPath");

            // Allocate a variable
            ScalarVariableAccessPath resultPath = new ScalarVariableAccessPath(
                    cCtx.defineVariable("projection_computation_result"),
                    primitiveReturnType
            );

            // Perform the comparison
            codeGenResult.add(
                    JaninoVariableGen.createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), resultPath.getType()),
                            resultPath.getVariableName(),
                            JaninoOperatorGen.ternary(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoOperatorGen.eq(JaninoGeneralGen.getLocation(), lhsCompRValue, rhsCompRValue),
                                    tbResultRvalue,
                                    fbResultRvalue
                            )
                    )
            );

            return resultPath;

        } else {
            throw new UnsupportedOperationException(
                    "ProjectOperator.createNonVecComputationCode does not support the provided computation expression");
        }
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Simply forward the call to the child operator to get the results of the query
        return this.child.produceVec(cCtx, oCtx);
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Handle each projection expression separately, to update the ordinal mapping to the correct state
        List<AccessPath> updatedOrdinalMapping = new ArrayList<>(this.projectionExpressions.length);
        for (int i = 0; i < this.projectionExpressions.length; i++) {
            AethraExpression projectionExp = this.projectionExpressions[i];
            updatedOrdinalMapping.add(i, generateProjectionCode(cCtx, projectionExp, codeGenResult, true));
        }

        // Set the updated ordinal mapping
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Consume the parent operator
        codeGenResult.addAll(vecParentConsume(cCtx, oCtx));

        // Return the resulting code
        return codeGenResult;
    }

    /**
     * Method to generate vectorised code for projections that implement an actual computation.
     * @param cCtx The {@link CodeGenContext} to use in the generation of the projection expression.
     * @param computationExpression The projection (i.e. computation) expression to generate code for.
     * @param codeGenResult The list to add generated code to that backs the returned {@link AccessPath}.
     * @return The {@link AccessPath} to the "result" of the provided {@code computationExpression}.
     */
    private AccessPath createVecComputationCode(
            CodeGenContext cCtx,
            AethraFunction computationExpression,
            List<Java.Statement> codeGenResult
    ) {
        if (computationExpression instanceof AethraBinaryFunction aethraBinaryFunction) {
            // Deal with recursive "projections" first
            AccessPath lhopResult = this.generateProjectionCode(cCtx, aethraBinaryFunction.firstOperand, codeGenResult, true);
            QueryVariableType primLhsResType = QueryVariableTypeMethods.primitiveType(lhopResult.getType());
            AccessPath rhopResult = this.generateProjectionCode(cCtx, aethraBinaryFunction.secondOperand, codeGenResult, true);
            QueryVariableType primRhsResType = QueryVariableTypeMethods.primitiveType(rhopResult.getType());

            // Compute the result type first
            QueryVariableType primitiveReturnType;
            if (aethraBinaryFunction.getKind() == DIVIDE) {
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            } else if (primLhsResType == QueryVariableType.P_DOUBLE && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == QueryVariableType.P_INT && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == QueryVariableType.P_INT && primRhsResType == QueryVariableType.P_INT)
                primitiveReturnType = QueryVariableType.P_INT;
            else
                throw new UnsupportedOperationException("ProjectOperator.createVecComputationCode could not determine desired return type");

            // Allocate a variable for the result using scan-surrounding allocation
            QueryVariableType returnVectorType = QueryVariableTypeMethods.primitiveArrayTypeForPrimitive(primitiveReturnType);
            // returnVectorType[] projection_computation_result = cCtx.getAllocationManager().get[returnVectorType]Vector();
            String projectionComputationResultVariableName = cCtx.defineQueryGlobalVariable(
                    "projection_computation_result",
                    JaninoGeneralGen.createPrimitiveArrayType(JaninoGeneralGen.getLocation(), QueryVariableTypeMethods.toJavaPrimitive(primitiveReturnType)),
                    JaninoMethodGen.createMethodInvocation(
                            JaninoGeneralGen.getLocation(),
                            JaninoMethodGen.createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            switch (returnVectorType.logicalType) {
                                case P_A_BOOLEAN -> "getBooleanVector";
                                case P_A_DOUBLE -> "getDoubleVector";
                                case P_A_INT -> "getIntVector";
                                case P_A_LONG -> "getLongVector";
                                default -> throw new UnsupportedOperationException("ProjectOperator.createVecComputationCode cannot allocate this result type");
                            }
                    ),
                    true
            );
            ArrayAccessPath projectionComputationResultAP = new ArrayAccessPath(projectionComputationResultVariableName, returnVectorType);

            // Allocate a variable for the length of the result vector
            ScalarVariableAccessPath projectionComputationResultLengthAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable(projectionComputationResultVariableName + "_length"),
                    QueryVariableType.P_INT
            );
            codeGenResult.add(
                    JaninoVariableGen.createPrimitiveLocalVar(
                            JaninoGeneralGen.getLocation(),
                            QueryVariableTypeMethods.toJavaPrimitive(projectionComputationResultLengthAP.getType()),
                            projectionComputationResultLengthAP.getVariableName()
                    )
            );

            // Now "combine" the results from the operands depending on the operator, the SIMD processing mode and the result access paths
            // First we obtain the name of the correct vectorised primitive for the operator
            String operatorMethodName = switch (aethraBinaryFunction.getKind()) {
                case MULTIPLY -> "multiply";
                case ADD -> "add";
                case SUBTRACT -> "subtract";
                case DIVIDE -> "divide";
                default -> throw new UnsupportedOperationException("ProjectOperator.createVecComputationOperator does not support the required operator");
            };

            // And we select the SIMDed version if required
            if (this.useSIMDVec())
                operatorMethodName += "SIMD";

            // Finally, we invoke the correct operator version based on the provided access paths
            if (lhopResult instanceof ScalarVariableAccessPath lhopScalar && rhopResult instanceof ArrowVectorAccessPath rhopArrowVec) {
                // length = operatorMethodName(lhopScalar, rhopArrowVector, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopScalar.read(),
                                                rhopArrowVec.read(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorAccessPath(
                        projectionComputationResultAP,
                        projectionComputationResultLengthAP,
                        QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ScalarVariableAccessPath lhopScalar && rhopResult instanceof ArrowVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // length = operatorMethodName(
                //         lhopScalar, rhopAVWSVAP.readArrowVector(), rhopAVWSVAP.readSelectionVector(), rhopAVWSVAP.readSelectionVectorLength(), result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopScalar.read(),
                                                rhopAVWSVAP.readArrowVector(),
                                                rhopAVWSVAP.readSelectionVector(),
                                                rhopAVWSVAP.readSelectionVectorLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithSelectionVectorAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        rhopAVWSVAP.getSelectionVectorVariable(),
                        rhopAVWSVAP.getSelectionVectorLengthVariable(),
                        QueryVariableTypeMethods.arrayVectorWithSelectionVectorType(QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrowVectorAccessPath lhopArrowVec && rhopResult instanceof ArrowVectorAccessPath rhopArrowVec) {
                // length = operatorMethodName(lhopArrowVec, rhopArrowVec, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopArrowVec.read(),
                                                rhopArrowVec.read(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorAccessPath(
                        projectionComputationResultAP,
                        projectionComputationResultLengthAP,
                        QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ArrowVectorAccessPath lhopArrowVec && rhopResult instanceof ArrayVectorAccessPath rhopArrayVec) {
                // length = operatorMethodName(lhopArrowVec, rhopArrayVec, rhopArrayVecLength, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopArrowVec.read(),
                                                rhopArrayVec.getVectorVariable().read(),
                                                rhopArrayVec.getVectorLengthVariable().read(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorAccessPath(
                        projectionComputationResultAP,
                        projectionComputationResultLengthAP,
                        QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ArrayVectorAccessPath lhopArrayVec && rhopResult instanceof ArrowVectorAccessPath rhopArrowVec) {
                // length = operatorMethodName(lhopArrayVec, lhsArrayVecLength, rhopArrowVec, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopArrayVec.getVectorVariable().read(),
                                                lhopArrayVec.getVectorLengthVariable().read(),
                                                rhopArrowVec.read(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorAccessPath(
                        projectionComputationResultAP,
                        projectionComputationResultLengthAP,
                        QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ArrowVectorWithSelectionVectorAccessPath lhopAVWSVAP
                    && rhopResult instanceof ArrayVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // The vectors should have the same selection vector by design
                assert lhopAVWSVAP.getSelectionVectorVariable().getVariableName().equals(
                        rhopAVWSVAP.getSelectionVectorVariable().getVariableName());

                // length = operatorMethodName(
                //         lhopArrowVec, rhopArrayVec, rhopArrayVecLength, lhopAVWSVAP.readSelectionVector(), lhopAVWSVAP.readSelectionVectorLength(), result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopAVWSVAP.readArrowVector(),
                                                rhopAVWSVAP.getArrayVectorVariable().getVectorVariable().read(),
                                                rhopAVWSVAP.getArrayVectorVariable().getVectorLengthVariable().read(),
                                                lhopAVWSVAP.readSelectionVector(),
                                                lhopAVWSVAP.readSelectionVectorLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithSelectionVectorAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWSVAP.getSelectionVectorVariable(),
                        lhopAVWSVAP.getSelectionVectorLengthVariable(),
                        QueryVariableTypeMethods.arrayVectorWithSelectionVectorType(QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrayVectorAccessPath lhopArrayVec && rhopResult instanceof ArrayVectorAccessPath rhopArrayVec) {
                // length = operatorMethodName(lhopArrayVec, lhopArrayVecLength, rhopArrayVec, rhopArrayVecLength, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopArrayVec.getVectorVariable().read(),
                                                lhopArrayVec.getVectorLengthVariable().read(),
                                                rhopArrayVec.getVectorVariable().read(),
                                                rhopArrayVec.getVectorLengthVariable().read(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorAccessPath(
                        projectionComputationResultAP,
                        projectionComputationResultLengthAP,
                        QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ArrayVectorWithSelectionVectorAccessPath lhopAVWSVAP
                    && rhopResult instanceof ArrayVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // The vectors should have the same selection vector by design
                assert lhopAVWSVAP.getSelectionVectorVariable().getVariableName().equals(
                        rhopAVWSVAP.getSelectionVectorVariable().getVariableName());

                // length = operatorMethodName(lhopArrayVec, lhopArrayVecLength, rhopArrayVec, rhopArrayVecLength, lhopSelVec, lhopSelVecLen, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopAVWSVAP.getArrayVectorVariable().getVectorVariable().read(),
                                                lhopAVWSVAP.getArrayVectorVariable().getVectorLengthVariable().read(),
                                                rhopAVWSVAP.getArrayVectorVariable().getVectorVariable().read(),
                                                rhopAVWSVAP.getArrayVectorVariable().getVectorLengthVariable().read(),
                                                lhopAVWSVAP.readSelectionVector(),
                                                lhopAVWSVAP.readSelectionVectorLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithSelectionVectorAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWSVAP.getSelectionVectorVariable(),
                        lhopAVWSVAP.getSelectionVectorLengthVariable(),
                        QueryVariableTypeMethods.arrayVectorWithSelectionVectorType(QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrowVectorWithSelectionVectorAccessPath lhopAVWSVAP
                    && rhopResult instanceof ArrowVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // The vectors should have the same selection vector by design
                assert lhopAVWSVAP.getSelectionVectorVariable().getVariableName().equals(
                        rhopAVWSVAP.getSelectionVectorVariable().getVariableName());

                // length = operatorMethodName(lhsArrowVec, rhsArrowVec, lhsSelVec, lhsSelVecLen, result);
                codeGenResult.add(
                        JaninoVariableGen.createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                projectionComputationResultLengthAP.write(),
                                JaninoMethodGen.createMethodInvocation(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopAVWSVAP.readArrowVector(),
                                                rhopAVWSVAP.readArrowVector(),
                                                lhopAVWSVAP.readSelectionVector(),
                                                lhopAVWSVAP.readSelectionVectorLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithSelectionVectorAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWSVAP.getSelectionVectorVariable(),
                        lhopAVWSVAP.getSelectionVectorLengthVariable(),
                        QueryVariableTypeMethods.arrayVectorWithSelectionVectorType(QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else {
                throw new UnsupportedOperationException(
                        "ProjectOperator.createVecComputationCode does not support the provided access path combination");
            }

        } else {
            throw new UnsupportedOperationException(
                    "ProjectOperator.createVecComputationCode does not support the provided computation expression");
        }
    }

}
