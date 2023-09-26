package AethraDB.benchmarks.aggregation_query;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveMemberTypeForArray;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * {@link CodeGenOperator} for packaging the result of a generated aggregation query as four arrays.
 * That is, all values produced by the query are put into an array per column in a contiguous fashion.
 */
public class ResultPackageOperator extends CodeGenOperator<RelNode> {

    /**
     * The {@link CodeGenOperator} producing the query result that should be packaged in the integer array.
     */
    private final CodeGenOperator<?> child;

    /**
     * The number of elements that will be present in the packed array.
     */
    private final int resultSize;

    /**
     * {@link ArrayAccessPath} to the array into which the key/col1 values are to be packed.
     */
    private ArrayAccessPath keyArrayAP;

    /**
     * {@link ArrayAccessPath} to the array into which the sum(col2) values are to be packed.
     */
    private ArrayAccessPath col2SumArrayAP;

    /**
     * {@link ArrayAccessPath} to the array into which the sum(col3) values are to be packed.
     */
    private ArrayAccessPath col3SumArrayAP;

    /**
     * {@link ArrayAccessPath} to the array into which the sum(col4) values are to be packed.
     */
    private ArrayAccessPath col4SumArrayAP;

    /**
     * {@link ScalarVariableAccessPath} to the index variable for writing into the target array.
     */
    private ScalarVariableAccessPath resultArrayIndexVariableAP;

    /**
     * Create an {@link ResultPackageOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     * @param resultSize The number of elements that will be put into the result array.
     */
    public ResultPackageOperator(RelNode logicalSubplan, CodeGenOperator<?> child, int resultSize) {
        super(logicalSubplan, false);
        this.child = child;
        this.child.setParent(this);
        this.resultSize = resultSize;
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
        return produce(cCtx, oCtx, false);
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Generate code to pack the values into the array
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // We need to generate the following lines to store all ordinal values
        // [this.keyArrayAP.read()][this.resultArrayIndexVariableAP.read()] =
        //      [getRValueFromAccessPathNonVec(cCtx, 0, codeGenResult)];
        codeGenResult.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(getLocation(), this.keyArrayAP.read(), this.resultArrayIndexVariableAP.read()),
                        getRValueFromOrdinalAccessPathNonVec(cCtx, 0, codeGenResult)
                ));

        // [this.col2SumArrayAP.read()][this.resultArrayIndexVariableAP.read()] =
        //      [getRValueFromAccessPathNonVec(cCtx, 1, codeGenResult)];
        codeGenResult.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(getLocation(), this.col2SumArrayAP.read(), this.resultArrayIndexVariableAP.read()),
                        getRValueFromOrdinalAccessPathNonVec(cCtx, 1, codeGenResult)
                ));

        // [this.col3SumArrayAP.read()][this.resultArrayIndexVariableAP.read()] =
        //      [getRValueFromAccessPathNonVec(cCtx, 1, codeGenResult)];
        codeGenResult.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(getLocation(), this.col3SumArrayAP.read(), this.resultArrayIndexVariableAP.read()),
                        getRValueFromOrdinalAccessPathNonVec(cCtx, 2, codeGenResult)
                ));

        // [this.col4SumArrayAP.read()][this.resultArrayIndexVariableAP.read()++] =
        //      [getRValueFromAccessPathNonVec(cCtx, 1, codeGenResult)];
        codeGenResult.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                this.col4SumArrayAP.read(),
                                postIncrement(getLocation(), this.resultArrayIndexVariableAP.write())
                        ),
                        getRValueFromOrdinalAccessPathNonVec(cCtx, 3, codeGenResult)
                ));

        // Do not consume the parent operator here, but in the produceNonVec method since this is
        // a blocking operator.
        return codeGenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        return produce(cCtx, oCtx, true);
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Generate code to pack the values into the array
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // We need to generate the following statements:
        // System.arraycopy(
        //      [cCtx.currentOrdinalMapping.get(0).getVectorVariable().read()],
        //      0,
        //      [this.keyArrayAP.read()],
        //      [this.resultArrayIndexVariableAP.read()],
        //      [aggregateLengthVariableAP.read()]);
        ScalarVariableAccessPath aggregateLengthVariableAP = ((ArrayVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(0)).getVectorLengthVariable();
        codeGenResult.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                ((ArrayVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(0)).getVectorVariable().read(),
                                createIntegerLiteral(getLocation(), 0),
                                this.keyArrayAP.read(),
                                this.resultArrayIndexVariableAP.read(),
                                aggregateLengthVariableAP.read()
                        }
                )
        );

        // System.arraycopy(
        //      [cCtx.currentOrdinalMapping.get(1).getVectorVariable().read()],
        //      0,
        //      [this.col2SumArrayAP.read()],
        //      [this.resultArrayIndexVariableAP.read()],
        //      [aggregateLengthVariableAP.read()]);
        codeGenResult.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                ((ArrayVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(1)).getVectorVariable().read(),
                                createIntegerLiteral(getLocation(), 0),
                                this.col2SumArrayAP.read(),
                                this.resultArrayIndexVariableAP.read(),
                                aggregateLengthVariableAP.read()
                        }
                )
        );

        // System.arraycopy(
        //      [cCtx.currentOrdinalMapping.get(2).getVectorVariable().read()],
        //      0,
        //      [this.col3SumArrayAP.read()],
        //      [this.resultArrayIndexVariableAP.read()],
        //      [aggregateLengthVariableAP.read()]);
        codeGenResult.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                ((ArrayVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(2)).getVectorVariable().read(),
                                createIntegerLiteral(getLocation(), 0),
                                this.col3SumArrayAP.read(),
                                this.resultArrayIndexVariableAP.read(),
                                aggregateLengthVariableAP.read()
                        }
                )
        );

        // System.arraycopy(
        //      [cCtx.currentOrdinalMapping.get(3).getVectorVariable().read()],
        //      0,
        //      [this.col4SumArrayAP.read()],
        //      [this.resultArrayIndexVariableAP.read()],
        //      [aggregateLengthVariableAP.read()]);
        codeGenResult.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                ((ArrayVectorAccessPath) cCtx.getCurrentOrdinalMapping().get(3)).getVectorVariable().read(),
                                createIntegerLiteral(getLocation(), 0),
                                this.col4SumArrayAP.read(),
                                this.resultArrayIndexVariableAP.read(),
                                aggregateLengthVariableAP.read()
                        }
                )
        );

        // [this.resultArrayIndexVariableAP.write()] += [aggregateLengthVariableAP.read()]
        codeGenResult.add(
                createVariableAdditionAssignmentStm(
                        getLocation(),
                        this.resultArrayIndexVariableAP.write(),
                        aggregateLengthVariableAP.read()
                )
        );

        // Do not consume the parent operator here, but in the produceNonVec method since this is
        // a blocking operator.
        return codeGenResult;
    }

    /**
     * Internal method to unify the {@code produceNonVec} and {@code produceVec} methods.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param vectorised Whether to use the vectorised code generation style.
     * @return The generated production code.
     */
    public List<Java.Statement> produce(CodeGenContext cCtx, OptimisationContext oCtx, boolean vectorised) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Allocate the arrays
        this.keyArrayAP = new ArrayAccessPath(cCtx.defineVariable("resultKeyArray"), P_A_INT);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), keyArrayAP.getType()),
                        this.keyArrayAP.getVariableName(),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaType(getLocation(), primitiveMemberTypeForArray(this.keyArrayAP.getType())),
                                this.resultSize
                        )
                )
        );

        this.col2SumArrayAP = new ArrayAccessPath(cCtx.defineVariable("resultSum2Array"), P_A_LONG);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), col2SumArrayAP.getType()),
                        this.col2SumArrayAP.getVariableName(),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaType(getLocation(), primitiveMemberTypeForArray(this.col2SumArrayAP.getType())),
                                this.resultSize
                        )
                )
        );

        this.col3SumArrayAP = new ArrayAccessPath(cCtx.defineVariable("resultSum3Array"), P_A_LONG);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), col3SumArrayAP.getType()),
                        this.col3SumArrayAP.getVariableName(),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaType(getLocation(), primitiveMemberTypeForArray(this.col3SumArrayAP.getType())),
                                this.resultSize
                        )
                )
        );

        this.col4SumArrayAP = new ArrayAccessPath(cCtx.defineVariable("resultSum4Array"), P_A_LONG);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), col4SumArrayAP.getType()),
                        this.col4SumArrayAP.getVariableName(),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaType(getLocation(), primitiveMemberTypeForArray(this.col4SumArrayAP.getType())),
                                this.resultSize
                        )
                )
        );

        // Allocate the indexing variable/write pointer
        this.resultArrayIndexVariableAP = new ScalarVariableAccessPath(cCtx.defineVariable("resultWritePtr"), P_INT);
        codeGenResult.add(
                createPrimitiveLocalVar(
                        getLocation(),
                        Java.Primitive.INT,
                        resultArrayIndexVariableAP.getVariableName(),
                        "0"
                )
        );

        // Now forward the call to the child operator to get the results of the query
        if (vectorised)
            codeGenResult.addAll(this.child.produceVec(cCtx, oCtx));
        else
            codeGenResult.addAll(this.child.produceNonVec(cCtx, oCtx));

        // Update the ordinal mapping to present the four arrays that are left
        cCtx.setCurrentOrdinalMapping(List.of(
                new ArrayAccessPath[] {
                        this.keyArrayAP,
                        this.col2SumArrayAP,
                        this.col3SumArrayAP,
                        this.col4SumArrayAP
                }));

        // Have the parent operator consume the result
        codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }


}
