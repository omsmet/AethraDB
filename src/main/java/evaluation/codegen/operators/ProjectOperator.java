package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithValidityMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.codehaus.janino.Java;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static evaluation.codegen.infrastructure.context.QueryVariableType.P_DOUBLE;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrayVectorWithSelectionVectorType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.arrayVectorWithValidityMaskType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.sqlTypeToPrimitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createFloatingPointLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.div;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.eq;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.sub;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.ternary;
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

            Java.Rvalue translatedLiteralValue;
            if (rlExpr.toString().equals("null:DOUBLE"))
                translatedLiteralValue = createAmbiguousNameRef(getLocation(), "Double.NaN");
            else if (rlExpr.toString().equals("null:FLOAT"))
                translatedLiteralValue = createAmbiguousNameRef(getLocation(), "Float.NaN");
            else
                translatedLiteralValue = switch (literalVariableAP.getType()) {
                    case P_DOUBLE, P_FLOAT -> createFloatingPointLiteral(getLocation(), rlExpr.toString());
                    case P_INT, P_LONG -> createIntegerLiteral(getLocation(), rlExpr.toString());
                    default -> throw new UnsupportedOperationException("ProjectOperator.generatedProjectionCode does not support the provided literal type");
                };

            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), literalVariableAP.getType()),
                            literalVariableAP.getVariableName(),
                            translatedLiteralValue
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
        SqlOperator computationOperator = computationExpression.getOperator();
        List<RexNode> computationOperands = computationExpression.getOperands();

        if (computationOperator instanceof SqlBinaryOperator sqlBinaryOperator) {
            // Deal with recursive "projections" first
            if (computationOperands.size() != 2)
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputationCode expects two operands for a binary operator");

            AccessPath lhopResult = this.generatedProjectionCode(cCtx, computationOperands.get(0), codeGenResult, false);
            QueryVariableType primLhsResType = primitiveType(lhopResult.getType());
            AccessPath rhopResult = this.generatedProjectionCode(cCtx, computationOperands.get(1), codeGenResult, false);
            QueryVariableType primRhsResType = primitiveType(rhopResult.getType());

            // Compute the result type first
            QueryVariableType primitiveReturnType;
            if (computationOperator.getKind() == SqlKind.DIVIDE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == QueryVariableType.P_DOUBLE && primRhsResType == QueryVariableType.P_DOUBLE)
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
                Java.Rvalue operatorComputation = switch (sqlBinaryOperator.getKind()) {
                    case TIMES -> mul(
                            getLocation(),
                            lhsRValue,
                            rhsRValue
                    );
                    case PLUS -> plus(
                            getLocation(),
                            lhsRValue,
                            rhsRValue
                    );
                    case MINUS -> sub(
                            getLocation(),
                            lhsRValue,
                            rhsRValue
                    );
                    case DIVIDE -> div(
                            getLocation(),
                            (primLhsResType != P_DOUBLE)
                                    ? createCast(getLocation(), toJavaType(getLocation(), P_DOUBLE), lhsRValue)
                                    : lhsRValue,
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

        } else if (computationOperator instanceof SqlCaseOperator caseOperator) {
            // Currently the case operator is only supported for non-SIMD mode
            if (this.useSIMDNonVec(cCtx))
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputationCode only supports the CASE operator in non-SIMD mode");

            // Only supports "binary-choice" case operator
            if (computationExpression.getOperands().size() != 3)
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputationCode only supports 3 operands for the CASE operator");

            if (!(computationOperands.get(0) instanceof RexCall))
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputationCode only supports CASE operators with a first RexCall operand");
            RexCall caseRexCall = (RexCall) computationOperands.get(0);

            if (caseRexCall.getOperator().getKind() != SqlKind.EQUALS)
                throw new UnsupportedOperationException(
                        "ProjectionOperator.createNonVecComputationCode only supports CASE operators using the = operator");

            // Translate the result of the case branches
            AccessPath tbResult = this.generatedProjectionCode(cCtx, computationOperands.get(1), codeGenResult, false);
            QueryVariableType primitiveTbResultType = primitiveType(tbResult.getType());
            AccessPath fbResult = this.generatedProjectionCode(cCtx, computationOperands.get(2), codeGenResult, false);
            QueryVariableType primitiveFbResultType = primitiveType(fbResult.getType());

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
            if (primitiveTbResultType == P_DOUBLE && primitiveFbResultType == P_DOUBLE)
                primitiveReturnType = P_DOUBLE;
            else
                throw new UnsupportedOperationException("ProjectOperator.createNonVecComputation could not determine the return type");

            // Resolve the lhs and rhs of the equality comparison
            List<RexNode> comparisonOperands = caseRexCall.getOperands();
            AccessPath lhsCompAp = this.generatedProjectionCode(cCtx, comparisonOperands.get(0), codeGenResult, false);
            AccessPath rhsCompAp = this.generatedProjectionCode(cCtx, comparisonOperands.get(1), codeGenResult, false);

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
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), resultPath.getType()),
                            resultPath.getVariableName(),
                            ternary(
                                    getLocation(),
                                    eq(getLocation(), lhsCompRValue, rhsCompRValue),
                                    tbResultRvalue,
                                    fbResultRvalue
                            )
                    )
            );

            return resultPath;

        } else if (computationOperator instanceof SqlExtractFunction sqlExtractFunction) {
            // Currently the extract operator is only supported for non-SIMD mode
            if (this.useSIMDNonVec(cCtx))
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputationCode only supports the EXTRACT operator in non-SIMD mode");

            // The extraction operator currently only supports extracting a year from a date, so check this pre-condition
            if (!(computationOperands.get(0) instanceof RexLiteral extractLiteral))
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputationCode expects the first operand of an EXTRACT to be a literal");

            TimeUnitRange extractTimeUnit = Objects.requireNonNull(extractLiteral.getValueAs(TimeUnitRange.class));
            if (extractTimeUnit.startUnit != TimeUnit.YEAR || extractTimeUnit.endUnit != null)
                throw new UnsupportedOperationException(
                        "ProjectOperator.createNonVecComputationCode only supports extracting years from a date column");

            if (!(computationOperands.get(1) instanceof RexInputRef baseValueRef))
                throw new UnsupportedOperationException(
                    "ProjectOperator.createNonVecComputationCode expects the second operand of an EXTRACT to be an input reference");

            // Generate the code for extracting the year
            Java.Rvalue baseValue = getRValueFromAccessPathNonVec(cCtx, baseValueRef.getIndex(), codeGenResult);
            ScalarVariableAccessPath extractedYearAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("extractedYear"), P_INT);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), extractedYearAP.getType()),
                            extractedYearAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "java.time.LocalDate"),
                                            "ofEpochDay",
                                            new Java.Rvalue[] { baseValue }
                                    ),
                                    "getYear"
                            )
                    )
            );

            return extractedYearAP;

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
        if (computationExpression.getOperator() instanceof SqlBinaryOperator sqlBinaryOperator) {
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
            if (sqlBinaryOperator.getKind() == SqlKind.DIVIDE) {
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            } else if (primLhsResType == QueryVariableType.P_DOUBLE && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else if (primLhsResType == P_INT && primRhsResType == QueryVariableType.P_DOUBLE)
                primitiveReturnType = QueryVariableType.P_DOUBLE;
            else
                throw new UnsupportedOperationException("ProjectOperator.createVecComputationCode could not determine desired return type");

            // Allocate a variable for the result using scan-surrounding allocation
            QueryVariableType returnVectorType = primitiveArrayTypeForPrimitive(primitiveReturnType);
            // returnVectorType[] projection_computation_result = cCtx.getAllocationManager().get[returnVectorType]Vector();
            String projectionComputationResultVariableName = cCtx.defineQueryGlobalVariable(
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
            SqlKind computationOperator = sqlBinaryOperator.getKind();
            String operatorMethodName;
            if (computationOperator == SqlKind.TIMES) {
                operatorMethodName = "multiply";
            } else if (computationOperator == SqlKind.PLUS) {
                operatorMethodName = "add";
            } else if (computationOperator == SqlKind.MINUS) {
                operatorMethodName = "subtract";
            } else if (computationOperator == SqlKind.DIVIDE) {
                operatorMethodName = "divide";
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

            } else if (lhopResult instanceof ScalarVariableAccessPath lhopScalar && rhopResult instanceof ArrowVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // length = operatorMethodName(
                //         lhopScalar, rhopAVWSVAP.readArrowVector(), rhopAVWSVAP.readSelectionVector(), rhopAVWSVAP.readSelectionVectorLength(), result);
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
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        rhopAVWSVAP.getSelectionVectorVariable(),
                        rhopAVWSVAP.getSelectionVectorLengthVariable(),
                        arrayVectorWithSelectionVectorType(vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ScalarVariableAccessPath lhopScalar && rhopResult instanceof ArrowVectorWithValidityMaskAccessPath rhopAVWVMAP) {
                // length = operatorMethodName(
                //         lhopScalar, rhopArrowVector, rhopValidityMask, rhopValidityMaskLength, result);
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
                                                rhopAVWVMAP.readArrowVector(),
                                                rhopAVWVMAP.readValidityMask(),
                                                rhopAVWVMAP.readValidityMaskLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithValidityMaskAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        rhopAVWVMAP.getValidityMaskVariable(),
                        rhopAVWVMAP.getValidityMaskLengthVariable(),
                        arrayVectorWithValidityMaskType(vectorTypeForPrimitiveArrayType(returnVectorType))
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

            } else if (lhopResult instanceof ArrowVectorWithSelectionVectorAccessPath lhopAVWSVAP
                    && rhopResult instanceof ArrayVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // The vectors should have the same selection vector by design
                assert lhopAVWSVAP.getSelectionVectorVariable().getVariableName().equals(
                        rhopAVWSVAP.getSelectionVectorVariable().getVariableName());

                // length = operatorMethodName(
                //         lhopArrowVec, rhopArrayVec, rhopArrayVecLength, lhopAVWSVAP.readSelectionVector(), lhopAVWSVAP.readSelectionVectorLength(), result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
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
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWSVAP.getSelectionVectorVariable(),
                        lhopAVWSVAP.getSelectionVectorLengthVariable(),
                        arrayVectorWithSelectionVectorType(vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrowVectorWithValidityMaskAccessPath lhopAVWVMAP
                    && rhopResult instanceof ArrayVectorWithValidityMaskAccessPath rhopAVWVMAP) {
                // The vectors should have the same validity mask by design
                assert lhopAVWVMAP.getValidityMaskVariable().getVariableName().equals(
                        rhopAVWVMAP.getValidityMaskVariable().getVariableName());

                // length = operatorMethodName(
                //         lhopArrowVec, rhopArrayVec, rhopArrayVecLength, lhopAVWSVAP.readValidityMask(), lhopAVWSVAP.readValidityMaskLength(), result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopAVWVMAP.readArrowVector(),
                                                rhopAVWVMAP.getArrayVectorVariable().getVectorVariable().read(),
                                                rhopAVWVMAP.getArrayVectorVariable().getVectorLengthVariable().read(),
                                                lhopAVWVMAP.readValidityMask(),
                                                lhopAVWVMAP.readValidityMaskLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithValidityMaskAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWVMAP.getValidityMaskVariable(),
                        lhopAVWVMAP.getValidityMaskLengthVariable(),
                        arrayVectorWithValidityMaskType(vectorTypeForPrimitiveArrayType(returnVectorType))
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

            } else if (lhopResult instanceof ArrayVectorWithSelectionVectorAccessPath lhopAVWSVAP
                    && rhopResult instanceof ArrayVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // The vectors should have the same selection vector by design
                assert lhopAVWSVAP.getSelectionVectorVariable().getVariableName().equals(
                        rhopAVWSVAP.getSelectionVectorVariable().getVariableName());

                // length = operatorMethodName(lhopArrayVec, lhopArrayVecLength, rhopArrayVec, rhopArrayVecLength, lhopSelVec, lhopSelVecLen, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
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
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWSVAP.getSelectionVectorVariable(),
                        lhopAVWSVAP.getSelectionVectorLengthVariable(),
                        arrayVectorWithSelectionVectorType(vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrayVectorWithValidityMaskAccessPath lhopAVWVMAP
                    && rhopResult instanceof ArrayVectorWithValidityMaskAccessPath rhopAVWVMAP) {
                // The vectors should have the same validity mask by design
                assert lhopAVWVMAP.getValidityMaskVariable().getVariableName().equals(
                        rhopAVWVMAP.getValidityMaskVariable().getVariableName());

                // length = operatorMethodName(lhopArrayVec, lhopArrayVecLength, rhopArrayVec, rhopArrayVecLength, lhopValMask, lhopValMaskLen, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopAVWVMAP.getArrayVectorVariable().getVectorVariable().read(),
                                                lhopAVWVMAP.getArrayVectorVariable().getVectorLengthVariable().read(),
                                                rhopAVWVMAP.getArrayVectorVariable().getVectorVariable().read(),
                                                rhopAVWVMAP.getArrayVectorVariable().getVectorLengthVariable().read(),
                                                lhopAVWVMAP.readValidityMask(),
                                                lhopAVWVMAP.readValidityMaskLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithValidityMaskAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWVMAP.getValidityMaskVariable(),
                        lhopAVWVMAP.getValidityMaskLengthVariable(),
                        arrayVectorWithValidityMaskType(vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrowVectorWithSelectionVectorAccessPath lhopAVWSVAP
                    && rhopResult instanceof ArrowVectorWithSelectionVectorAccessPath rhopAVWSVAP) {
                // The vectors should have the same selection vector by design
                assert lhopAVWSVAP.getSelectionVectorVariable().getVariableName().equals(
                        rhopAVWSVAP.getSelectionVectorVariable().getVariableName());

                // length = operatorMethodName(lhsArrowVec, rhsArrowVec, lhsSelVec, lhsSelVecLen, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
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
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWSVAP.getSelectionVectorVariable(),
                        lhopAVWSVAP.getSelectionVectorLengthVariable(),
                        arrayVectorWithSelectionVectorType(vectorTypeForPrimitiveArrayType(returnVectorType))
                );

            } else if (lhopResult instanceof ArrowVectorWithValidityMaskAccessPath lhopAVWVMAP
                    && rhopResult instanceof ArrowVectorWithValidityMaskAccessPath rhopAVWVMAP) {
                // The vectors should have the same validity mask by design
                assert lhopAVWVMAP.getValidityMaskVariable().getVariableName().equals(
                        rhopAVWVMAP.getValidityMaskVariable().getVariableName());

                // length = operatorMethodName(lhopArrowVec, rhopArrowVec, lhopValMask, lhopValMaskLen, result);
                codeGenResult.add(
                        createVariableAssignmentStm(
                                getLocation(),
                                projectionComputationResultLengthAP.write(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedArithmeticOperators"),
                                        operatorMethodName,
                                        new Java.Rvalue[]{
                                                lhopAVWVMAP.readArrowVector(),
                                                rhopAVWVMAP.readArrowVector(),
                                                lhopAVWVMAP.readValidityMask(),
                                                lhopAVWVMAP.readValidityMaskLength(),
                                                projectionComputationResultAP.read()
                                        }
                                )
                        )
                );
                return new ArrayVectorWithValidityMaskAccessPath(
                        new ArrayVectorAccessPath(
                                projectionComputationResultAP,
                                projectionComputationResultLengthAP,
                                vectorTypeForPrimitiveArrayType(returnVectorType)
                        ),
                        lhopAVWVMAP.getValidityMaskVariable(),
                        lhopAVWVMAP.getValidityMaskLengthVariable(),
                        arrayVectorWithValidityMaskType(vectorTypeForPrimitiveArrayType(returnVectorType))
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
