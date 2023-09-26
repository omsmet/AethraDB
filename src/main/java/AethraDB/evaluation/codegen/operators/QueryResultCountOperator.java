package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen;
import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithSelectionVectorAccessPath;
import org.apache.calcite.rel.RelNode;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;

/**
 * A {@link CodeGenOperator} which simply counts the number of records passed into it.
 */
public class QueryResultCountOperator extends CodeGenOperator<RelNode> {

    /**
     * The {@link CodeGenOperator} producing the records to be aggregated by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * Stores the {@link ScalarVariableAccessPath} to the count variable of this operator.
     */
    private ScalarVariableAccessPath countStateVariable;

    /**
     * Create an {@link QueryResultCountOperator} instance for a specific query.
     * @param logicalSubplan The logical plan of the query for which the operator is created.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     */
    public QueryResultCountOperator(RelNode logicalSubplan, CodeGenOperator<?> child) {
        super(logicalSubplan, false);
        this.child = child;
        this.child.setParent(this);
    }

    @Override
    public boolean canProduceNonVectorised() {
        // Since this is a blocking operator, we can always expose the result in the non-vectorised paradigm.
        return true;
    }

    @Override
    public boolean canProduceVectorised() {
        // Since this is a blocking operator, we can always expose the result in the vectorised paradigm.
        return true;
    }

    @Override
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Initialise the count variable and add it to the code generation result
        this.countStateVariable = new ScalarVariableAccessPath(
                cCtx.defineVariable("result_count"),
                P_LONG);
        codeGenResult.add(
                JaninoVariableGen.createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), this.countStateVariable.getType()),
                        this.countStateVariable.getVariableName(),
                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                ));

        // Store the context and continue production in the child operator, so that eventually this
        // operator's consumeNonVec method is invoked
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.child.produceNonVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        // Expose the result of this operator by updating the ordinal mapping
        List<AccessPath> newOrdinalMapping = new ArrayList<>(1);
        newOrdinalMapping.add(this.countStateVariable);
        cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

        // Have the parent operator consume the result
        codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Update the result count based on the ordinal mapping that we currently have
        boolean useSIMD = this.useSIMDNonVec(cCtx);
        AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
        if (!useSIMD && firstOrdinalAP instanceof ScalarVariableAccessPath) {
            codeGenResult.add(JaninoOperatorGen.postIncrementStm(JaninoGeneralGen.getLocation(), this.countStateVariable.write()));

        } else if (!useSIMD && firstOrdinalAP instanceof IndexedArrowVectorElementAccessPath) {
            codeGenResult.add(JaninoOperatorGen.postIncrementStm(JaninoGeneralGen.getLocation(), this.countStateVariable.write()));

        } else if (useSIMD && firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
            // For a count aggregation over a SIMD loop access path, add the number of true entries in the valid mask
            codeGenResult.add(
                    JaninoVariableGen.createVariableAdditionAssignmentStm(
                            JaninoGeneralGen.getLocation(),
                            this.countStateVariable.write(),
                            JaninoMethodGen.createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    slap.readSIMDMask(),
                                    "trueCount"
                            )
                    )
            );

        } else {
            throw new UnsupportedOperationException(
                    "QueryResultCountOperator.consumeNonVec does not support this AccessPath while "
                            + (useSIMD ? "" : "not ") + "using SIMD: "  + firstOrdinalAP);
        }

        // Do not consume parent operator here, but in the produce method since this is a blocking operator
        return codeGenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Initialise the count variable and add it to the code generation result
        this.countStateVariable = new ScalarVariableAccessPath(
                cCtx.defineVariable("result_count"),
                P_LONG);
        codeGenResult.add(
                JaninoVariableGen.createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), this.countStateVariable.getType()),
                        this.countStateVariable.getVariableName(),
                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                ));

        // Store the context and continue production in the child operator, so that eventually this
        // operator's consumeVec method is invoked
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.child.produceVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        // Expose the result of this operator by updating the ordinal mapping
        List<AccessPath> newOrdinalMapping = new ArrayList<>(1);
        newOrdinalMapping.add(this.countStateVariable);
        cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

        // Have the parent operator consume the result
        codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // We simply need to add the length of the valid part of the current vector to the count variable
        AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
        Java.Rvalue countIncrementRValue;

        if (firstOrdinalAP instanceof ArrowVectorAccessPath avap) {
            // count += avap.read().getValueCount();
            countIncrementRValue = JaninoMethodGen.createMethodInvocation(JaninoGeneralGen.getLocation(), avap.read(), "getValueCount");

        } else if (firstOrdinalAP instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
            // count += avwsvap.readSelectionVectorLength();
            countIncrementRValue = avwsvap.readSelectionVectorLength();

        } else if (firstOrdinalAP instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
            // count += VectorisedAggregationOperators.count(avwvmap.readValidityMask(), avwvmap.readValidityMaskLength());
            countIncrementRValue = JaninoMethodGen.createMethodInvocation(
                    JaninoGeneralGen.getLocation(),
                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedAggregationOperators"),
                    "count",
                    new Java.Rvalue[]{
                            avwvmap.readValidityMask(),
                            avwvmap.readValidityMaskLength()
                    }
            );

        } else if (firstOrdinalAP instanceof ArrayVectorAccessPath avap) {
            // count += avap.getVectorLengthVariable().read();
            countIncrementRValue = avap.getVectorLengthVariable().read();

        } else if (firstOrdinalAP instanceof ArrayVectorWithSelectionVectorAccessPath avwsvap) {
            // count += avwsvap.readSelectionVectorLength();
            countIncrementRValue = avwsvap.readSelectionVectorLength();

        } else if (firstOrdinalAP instanceof ArrayVectorWithValidityMaskAccessPath avwvmap) {
            // count += VectorisedAggregationOperators.count(avwvmap.readValidityMask(), avwvmap.readValidityMaskLength());
            countIncrementRValue = JaninoMethodGen.createMethodInvocation(
                    JaninoGeneralGen.getLocation(),
                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedAggregationOperators"),
                    "count",
                    new Java.Rvalue[]{
                            avwvmap.readValidityMask(),
                            avwvmap.readValidityMaskLength()
                    }
            );

        } else {
            throw new UnsupportedOperationException(
                    "QueryResultCountOperator.consumeVec does not support this access path for the count aggregation");

        }

        // Do the actual increment
        // count += [countIncrementRValue];
        codeGenResult.add(
                JaninoVariableGen.createVariableAdditionAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        this.countStateVariable.write(),
                        countIncrementRValue
                )
        );

        // Do not consume parent operator here, but in the produce method since the aggregation is a blocking operator
        return codeGenResult;
    }

}
