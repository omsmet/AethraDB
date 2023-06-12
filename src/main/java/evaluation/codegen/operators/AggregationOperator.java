package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrementStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;

/**
 * A {@link CodeGenOperator} which computes some aggregation function over the records.
 */
public class AggregationOperator extends CodeGenOperator<LogicalAggregate> {

    /**
     * The {@link CodeGenOperator} producing the records to be aggregated by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * List for keeping track of aggCall to aggregation state variable mapping.
     */
    private final List<String> aggCallToStateVariableNameMapping;

    /**
     * Create a {@link AggregationOperator} instance for a specific sub-query.
     * @param aggregation The logical aggregation (and sub-query) for which the operator is created.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @param child The {@link CodeGenOperator} producing the records to be aggregated.
     */
    public AggregationOperator(
            LogicalAggregate aggregation,
            boolean simdEnabled,
            CodeGenOperator<?> child
    ) {
        super(aggregation, simdEnabled);
        this.child = child;
        this.child.setParent(this);

        this.aggCallToStateVariableNameMapping = new ArrayList<>(aggregation.getAggCallList().size());
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
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceNonVec: We expect exactly one GroupSet to exist in the logical plan");

        // Check if we are dealing with a group-by aggregation
        boolean groupByAggregation = this.getLogicalSubplan().getGroupSet().size() > 0;

        // Initialise the aggregation state based on the operator
        codeGenResult.addAll(this.initialiseAggregationState(cCtx, oCtx, groupByAggregation, aggregationCalls));

        // Store the context and continue production in the child operator
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.child.produceNonVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        // Expose the result of this operator to its parent as a new "scan" (since aggregation is blocking)
        // Exposure way depends on whether we are dealing with a group-by aggregation and the aggregation function
        // TODO: below is candidate for generalised handling with vecProduce
        if (!groupByAggregation) {
            // Expose the result per aggregation call by updating the ordinal mapping
            List<AccessPath> newOrdinalMapping = new ArrayList<>(aggregationCalls.size());

            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentCallFunction = currentCall.getAggregation();

                // Check that the distinct keyword is not used by the current call
                if (currentCall.isDistinct())
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceNonVec does not support DISTINCT keyword");

                // Allocate the required variables based on the aggregation type
                if (currentCallFunction instanceof SqlCountAggFunction countFunction) {
                    // Simply set the current ordinal to refer to the count variable
                    newOrdinalMapping.add(
                            i,
                            new ScalarVariableAccessPath(this.aggCallToStateVariableNameMapping.get(i)));

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceNonVec does not support aggregation function " + currentCallFunction.getName());
                }

            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

        } else {
            // Group-by aggregation
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceNonVec does not support group-by aggregates");
        }

        // Have the parent operator consume the result
        codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        // Initialise the aggregation state based on the operator
        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator.consumeNonVec: We expect exactly one GroupSet to exist in the logical plan");

        // Check if we are dealing with a group-by aggregation
        boolean groupByAggregate = this.getLogicalSubplan().getGroupSet().size() > 0;

        // Check if we are dealing with a group-by aggregation
        if (!groupByAggregate) {
            // Update the state per aggregation call
            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentAggregationFunction = currentCall.getAggregation();

                // Check that the distinct keyword is not used by the current call
                if (currentCall.isDistinct())
                    throw new UnsupportedOperationException(
                            "AggregationOperator.consumeNonVec does not support DISTINCT keyword");

                // Update code depends on the aggregation function
                if (currentAggregationFunction instanceof SqlCountAggFunction) { // COUNT aggregation

                    // Check the type of the first ordinal to be able to generate the correct count update statements
                    AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
                    if (firstOrdinalAP instanceof ScalarVariableAccessPath svap) {
                        // For a count aggregation over scalar varaibles simply increment the relevant variable.
                        codeGenResult.add(
                                postIncrementStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), this.aggCallToStateVariableNameMapping.get(i))
                                ));
                    } else if (firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
                        // For a count aggregation over a SIMD loop access path, add the number of true entries in the valid mask
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), this.aggCallToStateVariableNameMapping.get(i)),
                                        createMethodInvocation(
                                                getLocation(),
                                                slap.readSIMDMask(),
                                                "trueCount"
                                        )
                                )
                        );
                    }

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.consumeNonVec does not support aggregation function " + currentAggregationFunction.getName());
                }

            }

        } else {
            // Group-by aggregation
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceNonVec does not support group-by aggregates");
        }

        // Do not consume parent operator here, but in the produce method since the aggregation is a blocking operator
        return codeGenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        // Initialise the aggregation state based on the operator
        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceVec: We expect exactly one GroupSet to exist in the logical plan");

        // Check if we are dealing with a group-by aggregation
        boolean groupByAggregation = this.getLogicalSubplan().getGroupSet().size() > 0;

        // Initialise the aggregation state
        codeGenResult.addAll(this.initialiseAggregationState(cCtx, oCtx, groupByAggregation, aggregationCalls));

        // Store the context and continue production in the child operator
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.child.produceVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        // Expose the result of this operator to its parent as a new "scan" (since aggregation is blocking)
        // Exposure way depends on whether we are dealing with a group-by aggregation and the aggregation function
        // TODO: below is candidate for generalised handling with nonVecProduce
        if (!groupByAggregation) {
            // Expose the result per aggregation call by updating the ordinal mapping
            List<AccessPath> newOrdinalMapping = new ArrayList<>(aggregationCalls.size());

            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentCallFunction = currentCall.getAggregation();

                // Check that the distinct keyword is not used by the current call
                if (currentCall.isDistinct())
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceVec does not support DISTINCT keyword");

                // Allocate the required variables based on the aggregation type
                if (currentCallFunction instanceof SqlCountAggFunction countFunction) {
                    // Simply set the current ordinal to refer to the count variable
                    newOrdinalMapping.add(
                            i,
                            new ScalarVariableAccessPath(this.aggCallToStateVariableNameMapping.get(i)));

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceVec does not support aggregation function " + currentCallFunction.getName());
                }

            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

        } else {
            // Group-by aggregation
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceVec does not support group-by aggregates");
        }

        // Have the parent operator consume the result
        codeGenResult.addAll(this.vecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        // Initialise the aggregation state based on the operator
        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator.consumeVec: We expect exactly one GroupSet to exist in the logical plan");

        // Check if we are dealing with a group-by aggregation
        boolean groupByAggregate = this.getLogicalSubplan().getGroupSet().size() > 0;

        // Check if we are dealing with a group-by aggregation
        if (!groupByAggregate) {
            // Update the state per aggregation call
            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentAggregationFunction = currentCall.getAggregation();

                // Check that the distinct keyword is not used by the current call
                if (currentCall.isDistinct())
                    throw new UnsupportedOperationException(
                            "AggregationOperator.consumeVec does not support DISTINCT keyword");

                // Update code depends on the aggregation function
                if (currentAggregationFunction instanceof SqlCountAggFunction) {
                    // For a count aggregation simply add the length of the valid part of the current
                    // vector the relevant variable.
                    AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
                    if (firstOrdinalAP instanceof ArrowVectorAccessPath avap) {
                        // count += avap.read().getValueCount();
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(),
                                                this.aggCallToStateVariableNameMapping.get(i)
                                        ),
                                        createMethodInvocation(
                                                getLocation(),
                                                avap.read(),
                                                "getValueCount"
                                        )
                                )
                        );

                    } else if (firstOrdinalAP instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                        // count += avwsvap.readSelectionVectorLength();
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(),
                                                this.aggCallToStateVariableNameMapping.get(i)
                                        ),
                                        avwsvap.readSelectionVectorLength()
                                )
                        );

                    } else if (firstOrdinalAP instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                        // count += VectorisedAggregationOperators.count(avwvmap.readValidityMask(), avwvmap.readValidityMaskLength());
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(),
                                                this.aggCallToStateVariableNameMapping.get(i)
                                        ),
                                        createMethodInvocation(
                                                getLocation(),
                                                createAmbiguousNameRef(getLocation(), "evaluation.vector_support.VectorisedAggregationOperators"),
                                                "count",
                                                new Java.Rvalue[] {
                                                        avwvmap.readValidityMask(),
                                                        avwvmap.readValidityMaskLength()
                                                }
                                        )
                                )
                        );

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.consumeVec does not support this access path for the count aggregation");

                    }

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.consumeVec does not support aggregation function " + currentAggregationFunction.getName());
                }

            }

        } else {
            // Group-by aggregation
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceVec does not support group-by aggregates");
        }

        // Do not consume parent operator here, but in the produce method since the aggregation is a blocking operator
        return codeGenResult;
    }

    /**
     * Method to handle initialisation of the aggregation state for the operator, as this is shared
     * between the vectorised and non-vectorised implementation.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param groupByAggregation Whether the aggregation uses a group-by statement.
     * @param aggregationCalls The aggregation calls for which to initialise the state.
     * @return The statements to initialise the aggregation state.
     */
    private List<Java.Statement> initialiseAggregationState(
            CodeGenContext cCtx,
            OptimisationContext oCtx,
            boolean groupByAggregation,
            List<AggregateCall> aggregationCalls
    ) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Handle the initialisation of the aggregation based on whether we have a group-by
        if (!groupByAggregation) {
            // Initialise the state per aggregation call
            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentCallFunction = currentCall.getAggregation();

                // Check that the distinct keyword is not used by the current call
                if (currentCall.isDistinct())
                    throw new UnsupportedOperationException(
                            "AggregationOperator.initialiseAggregationState does not support DISTINCT keyword");

                // Allocate the required variables based on the aggregation type
                if (currentCallFunction instanceof SqlCountAggFunction countFunction) {
                    // Simply allocate the variable required based on the data type
                    String aggregationStateVariableName = cCtx.defineVariable("aggCall_" + i + "_state");
                    this.aggCallToStateVariableNameMapping.add(i, aggregationStateVariableName);
                    codeGenResult.add(
                            this.sqlTypeToScalarJavaVariable(
                                    (BasicSqlType) currentCall.getType(),
                                    aggregationStateVariableName)
                    );

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceNonVec does not support aggregation function " + currentCallFunction.getName());
                }

            }

        } else {
            // Group-by aggregation
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceNonVec does not support group-by aggregates");
        }

        return codeGenResult;
    }

}
