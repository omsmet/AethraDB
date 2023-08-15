package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.sqlTypeToPrimitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createFloatingPointLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.sub;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * A {@link CodeGenOperator} which project out records according to a given criterion.
 */
public class ProjectOperator extends CodeGenOperator<LogicalProject> {

    /**
     * The {@link CodeGenOperator} producing the records to be projected by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * Create a {@link ProjectOperator} instance for a specific sub-query.
     * @param project The logical project (and sub-query) for which the operator is created.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @param child The {@link CodeGenOperator} producing the records to be projected.
     */
    public ProjectOperator(LogicalProject project, boolean simdEnabled, CodeGenOperator<?> child) {
        super(project, simdEnabled);
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
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Obtain and process the projection specification
        List<RexNode> projectionExpressions = this.getLogicalSubplan().getProjects();

        // Handle each projection expression separately, to update the ordinal mapping to the correct state
        List<AccessPath> updatedOrdinalMapping = new ArrayList<>(projectionExpressions.size());
        for (int i = 0; i < projectionExpressions.size(); i++) {
            RexNode projectionExp = projectionExpressions.get(i);
            updatedOrdinalMapping.add(i, generatedProjectionCode(cCtx, projectionExp, codeGenResult, false));
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
    private AccessPath generatedProjectionCode(
            CodeGenContext cCtx,
            RexNode projectionExpression,
            List<Java.Statement> codeGenResult,
            boolean vectorised
    ) {
        // Check the projection expression type
        if (projectionExpression instanceof RexLiteral rlExpr) {
            // Simply need to give access to the literal value of the expression
            ScalarVariableAccessPath literalVariableAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("projection_literal"),
                    sqlTypeToPrimitiveType(rlExpr.getType().getSqlTypeName())
            );

            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), literalVariableAP.getType()),
                            literalVariableAP.getVariableName(),
                            switch (literalVariableAP.getType()) {
                                case P_DOUBLE, P_FLOAT -> createFloatingPointLiteral(getLocation(), rlExpr.toString());
                                case P_INT, P_LONG -> createIntegerLiteral(getLocation(), rlExpr.toString());
                                default -> throw new UnsupportedOperationException("ProjectOperator.generatedProjectionCode does not support the provided literal type");
                            }
                    )
            );

            return literalVariableAP;

        } else if (projectionExpression instanceof RexInputRef rirExpr) {
            // Simply need to project out a column already present in the ordinal mapping
            int projectedColumnIndex = rirExpr.getIndex();
            return cCtx.getCurrentOrdinalMapping().get(projectedColumnIndex);

        } else if (projectionExpression instanceof RexCall rcExpr) {
            return vectorised
                    ? createVecComputationCode(cCtx, rcExpr, codeGenResult)
                    : createNonVecComputationCode(cCtx, rcExpr, codeGenResult);

        } else {
            throw new UnsupportedOperationException(
                    "ProjectOperator.generatedProjectionCode does not support the current projection expression");
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
            RexCall computationExpression,
            List<Java.Statement> codeGenResult
    ) {
        if (computationExpression.getOperator() instanceof SqlMonotonicBinaryOperator sqlMonBinOp) {
            // Deal with recursive "projections" first
            List<RexNode> computationOperands = computationExpression.getOperands();
            if (computationOperands.size() != 2)
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputationCode expects two operands for a binary operator");

            AccessPath lhopResult = this.generatedProjectionCode(cCtx, computationOperands.get(0), codeGenResult, false);
            QueryVariableType primLhsResType = primitiveType(lhopResult.getType());
            AccessPath rhopResult = this.generatedProjectionCode(cCtx, computationOperands.get(1), codeGenResult, false);
            QueryVariableType primRhsResType = primitiveType(rhopResult.getType());

            // Compute the result type first
            QueryVariableType primitiveReturnType;
            if (primLhsResType == QueryVariableType.P_DOUBLE && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == P_INT && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else
                throw new UnsupportedOperationException("ProjectOperator.createNonVecComputation could not determine desired return type");

            // Now "combine" the results from the operands depending on the operator, the SIMD processing mode and the result access paths
            if (!this.useSIMDNonVec(cCtx)) {
                // Simple scalar processing
                // Obtain r-values for the operands
                Java.Rvalue lhsRValue, rhsRValue;

                if (lhopResult instanceof ScalarVariableAccessPath lhopResultSVAP)
                    lhsRValue = lhopResultSVAP.read();
                else if (lhopResult instanceof IndexedArrowVectorElementAccessPath lhopResultIAVEA)
                    lhsRValue = lhopResultIAVEA.read();
                else
                    throw new UnsupportedOperationException(
                            "ProjectionOperator.createNonVecComputation does not support this lhopResult AccessPath");

                if (rhopResult instanceof ScalarVariableAccessPath rhopResultSVAP)
                    rhsRValue = rhopResultSVAP.read();
                else if (rhopResult instanceof IndexedArrowVectorElementAccessPath rhopResultIAVEA)
                    rhsRValue = rhopResultIAVEA.read();
                else
                    throw new UnsupportedOperationException(
                            "ProjectionOperator.createNonVecComputation does not support this rhopResult AccessPath");

                // Create a new scalar variable to store the computation result in
                ScalarVariableAccessPath resultPath = new ScalarVariableAccessPath(
                        cCtx.defineVariable("projection_computation_result"),
                        primitiveReturnType
                );

                // Generate the code for the actual computation
                Java.Rvalue operatorComputation = switch (sqlMonBinOp.getKind()) {
                    case TIMES ->
                            mul(
                                    getLocation(),
                                    lhsRValue,
                                    rhsRValue
                            );
                    case PLUS ->
                            plus(
                                    getLocation(),
                                    lhsRValue,
                                    rhsRValue
                            );
                    case MINUS ->
                            sub(
                                    getLocation(),
                                    lhsRValue,
                                    rhsRValue
                            );
                    default -> throw new UnsupportedOperationException(
                            "ProjectOperator.createNonVecComputation does not support the provided operator type");
                };

                codeGenResult.add(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), primitiveReturnType),
                                resultPath.getVariableName(),
                                operatorComputation
                        )
                );

                return resultPath;

            } else { // this.useSIMDNonVec(cCtx)
                throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
            }

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

        // Obtain and process the projection specification
        List<RexNode> projectionExpressions = this.getLogicalSubplan().getProjects();

        // Handle each projection expression separately, to update the ordinal mapping to the correct state
        List<AccessPath> updatedOrdinalMapping = new ArrayList<>(projectionExpressions.size());
        for (int i = 0; i < projectionExpressions.size(); i++) {
            RexNode projectionExp = projectionExpressions.get(i);
            updatedOrdinalMapping.add(i, generatedProjectionCode(cCtx, projectionExp, codeGenResult, true));
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
            RexCall computationExpression,
            List<Java.Statement> codeGenResult
    ) {
        if (computationExpression.getOperator() instanceof SqlMonotonicBinaryOperator sqlMonBinOp) {
            // Deal with recursive "projections" first
            List<RexNode> computationOperands = computationExpression.getOperands();
            if (computationOperands.size() != 2)
                throw new UnsupportedOperationException(
                        "ProjectOperator.createVecComputationCode expects two operands for a binary operator");

            AccessPath lhopResult = this.generatedProjectionCode(cCtx, computationOperands.get(0), codeGenResult, true);
            QueryVariableType primLhsResType = primitiveType(lhopResult.getType());
            AccessPath rhopResult = this.generatedProjectionCode(cCtx, computationOperands.get(1), codeGenResult, true);
            QueryVariableType primRhsResType = primitiveType(rhopResult.getType());

            // Compute the result type first
            QueryVariableType primitiveReturnType;
            if (primLhsResType == QueryVariableType.P_DOUBLE && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == P_INT && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else
                throw new UnsupportedOperationException("ProjectOperator.createVecComputationCode could not determine desired return type");

            // Allocate a variable for the result using scan-surrounding allocation
            QueryVariableType returnVectorType = primitiveArrayTypeForPrimitive(primitiveReturnType);
            // returnVectorType[] projection_computation_result = cCtx.getAllocationManager().get[returnVectorType]Vector();
            String projectionComputationResultVariableName = cCtx.defineScanSurroundingVariable(
                    "projection_computation_result",
                    createPrimitiveArrayType(getLocation(), toJavaPrimitive(primitiveReturnType)),
                    createMethodInvocation(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            switch (returnVectorType) {
                                case P_A_BOOLEAN -> "getBooleanVector";
                                case P_A_DOUBLE -> "getDoubleVector";
                                case P_A_FLOAT -> "getFloatVector";
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
                    P_INT
            );
            codeGenResult.add(
                    createPrimitiveLocalVar(
                            getLocation(),
                            toJavaPrimitive(projectionComputationResultLengthAP.getType()),
                            projectionComputationResultLengthAP.getVariableName()
                    )
            );

            // Now "combine" the results from the operands depending on the operator, the SIMD processing mode and the result access paths
            // First we obtain the name of the correct vectorised primitive for the operator
            SqlKind computationOperator = sqlMonBinOp.getKind();
            String operatorMethodName;
            if (computationOperator == SqlKind.TIMES) {
                operatorMethodName = "multiply";
            } else if (computationOperator == SqlKind.PLUS) {
                operatorMethodName = "add";
            } else if (computationOperator == SqlKind.MINUS) {
                operatorMethodName = "subtract";
            } else {
                throw new UnsupportedOperationException("ProjectOperator.createVecComputationOperator does not support the required operator");
            }

            // And we select the SIMDed version if required
            if (this.useSIMDVec())
                operatorMethodName += "SIMD";

            // Finally, we invoke the correct operator version based on the provided access paths
            if (lhopResult instanceof ScalarVariableAccessPath lhopScalar && rhopResult instanceof ArrowVectorAccessPath rhopArrowVec) {
                // length = operatorMethodName(lhopScalar, rhopArrowVector, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
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
                        vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ArrowVectorAccessPath lhopArrowVec && rhopResult instanceof ArrayVectorAccessPath rhopArrayVec) {
                // length = operatorMethodName(lhopArrowVec, rhopArrayVec, rhopArrayVecLength, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
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
                        vectorTypeForPrimitiveArrayType(returnVectorType)
                );

            } else if (lhopResult instanceof ArrayVectorAccessPath lhopArrayVec && rhopResult instanceof ArrayVectorAccessPath rhopArrayVec) {
                // length = operatorMethodName(lhopArrayVec, lhopArrayVecLength, rhopArrayVec, rhopArrayVecLength, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
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
                        vectorTypeForPrimitiveArrayType(returnVectorType)
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