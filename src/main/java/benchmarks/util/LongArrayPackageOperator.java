package benchmarks.util;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveMemberTypeForArray;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * {@link CodeGenOperator} for packaging the result of a generated query as a single integer array.
 * That is, all values produced by the query are put into an array contiguously.
 * If the processing paradigm is non-vectorised the result is presented in a row-major order, and for
 * vectorised processing the result is presented in a column-major order.
 */
public class LongArrayPackageOperator extends CodeGenOperator<RelNode> {

    /**
     * The {@link CodeGenOperator} producing the query result that should be packaged in the integer array.
     */
    private final CodeGenOperator<?> child;

    /**
     * The number of elements that will be present in the packed array.
     */
    private final int resultSize;

    /**
     * {@link ArrayAccessPath} to the array into which the values are to be packed.
     */
    private ArrayAccessPath resultArrayAP;

    /**
     * {@link ScalarVariableAccessPath} to the index variable for writing into the target array.
     */
    private ScalarVariableAccessPath resultArrayIndexVariableAP;

    /**
     * Create an {@link LongArrayPackageOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     * @param resultSize The number of elements that will be put into the result array.
     */
    public LongArrayPackageOperator(RelNode logicalSubplan, CodeGenOperator<?> child, int resultSize) {
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
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Allocate the array
        this.resultArrayAP = new ArrayAccessPath(
                cCtx.defineVariable("resultLongArray"), P_A_LONG);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), resultArrayAP.getType()),
                        this.resultArrayAP.getVariableName(),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaType(getLocation(), primitiveMemberTypeForArray(this.resultArrayAP.getType())),
                                this.resultSize
                        )
                )
        );

        // Allocate the indexing variable/write pointer
        this.resultArrayIndexVariableAP = new ScalarVariableAccessPath(
                cCtx.defineVariable(this.resultArrayAP.getVariableName() + "_writePtr"), P_INT);
        codeGenResult.add(
                createPrimitiveLocalVar(
                        getLocation(),
                        Java.Primitive.INT,
                        resultArrayIndexVariableAP.getVariableName(),
                        "0"
                )
        );

        // Now forward the call to the child operator to get the results of the query
        codeGenResult.addAll(this.child.produceNonVec(cCtx, oCtx));

        // Update the ordinal mapping to present the single array that is left
        cCtx.setCurrentOrdinalMapping(List.of(new ArrayAccessPath[] { this.resultArrayAP }));

        // Have the parent operator consume the result
        codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Generate code to pack the values into the array
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // For each ordinal, we want to generate the following line:
        // [this.resultArrayAP.read()][this.resultArrayIndexVariableAP.read()++] = ordinal_value;

        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            Java.Rvalue ordinal_value = getRValueFromAccessPathNonVec(cCtx, i, codeGenResult);

            codeGenResult.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    this.resultArrayAP.read(),
                                    postIncrement(getLocation(), this.resultArrayIndexVariableAP.write())
                            ),
                            ordinal_value
                    )
            );
        }

        // Do not consume the parent operator here, but in the produceNonVec method since this is
        // a blocking operator.
        return codeGenResult;
    }



}
