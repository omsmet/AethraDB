package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedMapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrementStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
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
     * List for keeping track of the aggregation state mapping per aggCall.
     */
    private final List<AggregationStateMapping> aggregationStateMappings;

    /**
     * Structure for storing the required information about the aggregation state of an aggregation operator.
     */
    private static final class AggregationStateMapping {

        /**
         * Array of variables names in the current aggregation state.
         */
        public final String[] variableNames;

        /**
         * Array of variable types in the current aggregation state.
         */
        public final AggregationStateVariableType[] variableTypes;

        /**
         * Enum indicating the list of supported aggregation variable types.
         */
        public enum AggregationStateVariableType {
            // Primitive variable types
            SCALAR_INT,

            // Complex variable types
            MAP_INT_INT,
        }

        /**
         * Creates a new {@link AggregationStateMapping} instance for a single variable.
         * @param variableName The name of the aggregation state variable.
         * @param variableType The type of the aggregation state variable.
         */
        public AggregationStateMapping(String variableName, AggregationStateVariableType variableType) {
            this.variableNames = new String[] { variableName };
            this.variableTypes = new AggregationStateVariableType[] {variableType};
        }

        /**
         * Creates a new {@link AggregationStateMapping} instance for a multiple variables.
         * @param variableNames The names of the aggregation state variables.
         * @param variableTypes The types of the aggregation state variables.
         */
        public AggregationStateMapping(
                String[] variableNames, AggregationStateVariableType[] variableTypes) {
            this.variableNames = variableNames;
            this.variableTypes = variableTypes;
        }
    }

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

        this.aggregationStateMappings = new ArrayList<>(aggregation.getAggCallList().size());
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
        if (!groupByAggregation) {
            // In a non-group-by aggregate, the result is always a scalar, so there is no difference between
            // SIMD enabled operator and non-SIMD enabled operator here.
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
                            new ScalarVariableAccessPath(this.aggregationStateMappings.get(i).variableNames[0]));

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceNonVec does not support aggregation function " + currentCallFunction.getName());
                }

            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

            // Have the parent operator consume the result
            codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        } else {
            // Expose the result of the group-by aggregation
            // Group-by aggregation needs to distinguish between SIMD and Non-SIMD processing.
            if (!this.simdEnabled) {
                // Regular row-based processing
                // Create a loop iterating over the group-by key values based on the first aggregation
                // state's first variable
                AggregationStateMapping firstAggregationState = this.aggregationStateMappings.get(0);
                AggregationStateMapping.AggregationStateVariableType fasfvt = firstAggregationState.variableTypes[0];

                if (fasfvt == AggregationStateMapping.AggregationStateVariableType.MAP_INT_INT) {
                    // Simply iterate over the map, which will provide the keys
                    // Observation: a LogicalAggregate output always has the group-by column first,
                    // followed by the aggregation calls in the given order

                    // Simple_Int_Int_Map_Iterator [groupKeyIterator] = [firstAggregationState.variableNames[0]].getIterator();
                    // while ([groupKeyIterator].hasNext()] {
                    //     int [groupKey] = [groupKeyIterator].next();
                    //     [whileLoopBody]
                    // }

                    String groupKeyIteratorName = cCtx.defineVariable("groupKeyIterator");
                    codeGenResult.add(
                            createLocalVariable(
                                    getLocation(),
                                    createReferenceType(getLocation(), "Simple_Int_Int_Map_Iterator"),
                                    groupKeyIteratorName,
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), firstAggregationState.variableNames[0]),
                                            "getIterator"
                                    )
                            )
                    );

                    Java.Block whileLoopBody = createBlock(getLocation());
                    codeGenResult.add(
                            createWhileLoop(
                                    getLocation(),
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), groupKeyIteratorName),
                                            "hasNext"
                                    ),
                                    whileLoopBody
                            )
                    );

                    String groupKeyName = cCtx.defineVariable("groupKey");
                    ScalarVariableAccessPath groupKeyAP = new ScalarVariableAccessPath(groupKeyName);
                    whileLoopBody.addStatement(
                            createLocalVariable(
                                    getLocation(),
                                    createPrimitiveType(getLocation(), Java.Primitive.INT),
                                    groupKeyName,
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), groupKeyIteratorName),
                                            "next"
                                    )
                            )
                    );

                    // Update the ordinal mapping using the group-by key and the aggregation functions
                    List<AccessPath> newOrdinalMapping =
                            new ArrayList<>(this.getLogicalSubplan().getRowType().getFieldCount());
                    int currentOrdinalIndex = 0;

                    // First put the group-by key
                    newOrdinalMapping.add(currentOrdinalIndex++, groupKeyAP);

                    // Then add the access path per aggregation function
                    for (int i = 0; i < aggregationCalls.size(); i++) {
                        // Get information to classify the current aggregation call
                        AggregateCall currentCall = aggregationCalls.get(i);
                        SqlAggFunction currentCallFunction = currentCall.getAggregation();

                        // Check that the distinct keyword is not used by the current call
                        if (currentCall.isDistinct())
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.produceNonVec does not support DISTINCT keyword in group-by aggregation");

                        // Return the value for the current groupKey of the current aggregation call based on the aggregation type
                        if (currentCallFunction instanceof SqlSumEmptyIsZeroAggFunction sumEmptyIsZeroAggFunction) {
                            // Simply need to return the correct hash-map value based on the groupKey
                            String aggregationStateVariableName = this.aggregationStateMappings.get(i).variableNames[0];
                            MapAccessPath aggregationMapAP = new MapAccessPath(aggregationStateVariableName);
                            IndexedMapAccessPath indexedAggregationMapAP = new IndexedMapAccessPath(aggregationMapAP, groupKeyAP);
                            newOrdinalMapping.add(currentOrdinalIndex++, indexedAggregationMapAP);

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.produceNonVec does not support this group-by aggregation function: "
                                            + currentCallFunction.getName());
                        }
                    }

                    // Store the updated ordinal mapping
                    cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

                    // Have the parent operator consume the result
                    whileLoopBody.addStatements(this.nonVecParentConsume(cCtx, oCtx));

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceNonVec does not currently support obtaining keys from aggregation state type " + fasfvt);
                }

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.produceNonVec does not currently SIMD processing for group-by aggregations");
            }
        }

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
                                        createAmbiguousNameRef(getLocation(), this.aggregationStateMappings.get(i).variableNames[0])
                                ));
                    } else if (firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
                        // For a count aggregation over a SIMD loop access path, add the number of true entries in the valid mask
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), this.aggregationStateMappings.get(i).variableNames[0]),
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
            // Group-by aggregation currently only support single-column groups of the integer type
            // First we check if we received that
            ImmutableBitSet groupSet = this.getLogicalSubplan().getGroupSet();
            int firstGroupByColumnIndex = groupSet.nextSetBit(0); // There must be at least 1 column to group by
            RelDataTypeField groupByColumn = this.getLogicalSubplan().getRowType().getFieldList().get(firstGroupByColumnIndex);
            SqlTypeName groupByColumnType = groupByColumn.getType().getSqlTypeName();

            if (groupSet.cardinality() == 1 && groupByColumnType == SqlTypeName.INTEGER) {
                // Then, check if we are working a SIMD or non-SIMD enabled fashion
                if (!this.simdEnabled) { // Regular row-based processing
                    // Now update each hash-table to reflect the correct state of each aggregate
                    // depending on the aggregation function and data type
                    for (int i = 0; i < aggregationCalls.size(); i++) {
                        // Get information to classify the current aggregation call
                        AggregateCall currentCall = aggregationCalls.get(i);
                        SqlAggFunction currentCallFunction = currentCall.getAggregation();

                        // Check that the distinct keyword is not used by the current call
                        if (currentCall.isDistinct())
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.consumeNonVec does not support DISTINCT keyword in group-by aggregation");

                        // Allocate the required variables based on the aggregation type
                        if (currentCallFunction instanceof SqlSumEmptyIsZeroAggFunction sumEmptyIsZeroAggFunction) {
                            // Simply allocate the correct hash-table type based on the result type
                            String aggregationStateVariableName = this.aggregationStateMappings.get(i).variableNames[0];

                            if (currentCall.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                                // Key is of type int, result is of type int --> we are updating an IntInt map
                                // We simply need to invoke its "addToKeyOrPutIfNotExist" method for the group-by key and the value
                                // indicated by the current call (which should refer to a single column ordinal)
                                if (currentCall.getArgList().size() != 1)
                                    throw new UnsupportedOperationException("AggregationOperator.consumeNonVec received an unexpected" +
                                            "number of arguments for the SqlSumEmptyIsZeroAggFunction case");

                                codeGenResult.add(
                                        createMethodInvocationStm(
                                                getLocation(),
                                                createAmbiguousNameRef(getLocation(), aggregationStateVariableName),
                                                "addToKeyOrPutIfNotExist",
                                                new Java.Rvalue[] {
                                                        this.getRValueFromAccessPathNonVec(         // Group-By Key
                                                                cCtx,
                                                                oCtx,
                                                                firstGroupByColumnIndex,
                                                                createPrimitiveType(getLocation(), Java.Primitive.INT),
                                                                codeGenResult),
                                                        this.getRValueFromAccessPathNonVec(         // Value
                                                                cCtx,
                                                                oCtx,
                                                                currentCall.getArgList().get(0),
                                                                createPrimitiveType(getLocation(), Java.Primitive.INT),
                                                                codeGenResult)
                                                }
                                        )
                                );

                            } else {
                                throw new UnsupportedOperationException(
                                        "AggregationOperator.consumeNonVec only supports an integer" +
                                                " result for the SqlSumEmptyIsZeroAggFunction");
                            }

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.consumeNonVec does not support this group-by aggregation function: "
                                            + currentCallFunction.getName());
                        }

                    }
                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.consumeNonVec does not currently support SIMD processing");
                }

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.consumeNonVec currently only supports single integer-column group-by's");
            }

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
                            new ScalarVariableAccessPath(this.aggregationStateMappings.get(i).variableNames[0]));

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
                                        createAmbiguousNameRef(
                                                getLocation(),
                                                this.aggregationStateMappings.get(i).variableNames[0]
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
                                        createAmbiguousNameRef(
                                                getLocation(),
                                                this.aggregationStateMappings.get(i).variableNames[0]
                                        ),
                                        avwsvap.readSelectionVectorLength()
                                )
                        );

                    } else if (firstOrdinalAP instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                        // count += VectorisedAggregationOperators.count(avwvmap.readValidityMask(), avwvmap.readValidityMaskLength());
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(
                                                getLocation(),
                                                this.aggregationStateMappings.get(i).variableNames[0]
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
                    "AggregationOperator.consumeVec does not support group-by aggregates");
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
                    this.aggregationStateMappings.add(i, new AggregationStateMapping(
                            aggregationStateVariableName,
                            AggregationStateMapping.AggregationStateVariableType.SCALAR_INT));
                    codeGenResult.add(
                            this.sqlTypeToScalarJavaVariable(
                                    (BasicSqlType) currentCall.getType(),
                                    aggregationStateVariableName)
                    );

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.initialiseAggregationState does not support aggregation function: "
                                    + currentCallFunction.getName());
                }

            }

        } else {
            // Group-by aggregation currently only support single-column groups of the integer type
            // First we check if we received that
            ImmutableBitSet groupSet = this.getLogicalSubplan().getGroupSet();
            int firstGroupByColumnIndex = groupSet.nextSetBit(0); // There must be at least 1 column to group by
            RelDataTypeField groupByColumnType = this.getLogicalSubplan().getRowType().getFieldList().get(firstGroupByColumnIndex);

            if (groupSet.cardinality() == 1
                    && groupByColumnType.getType().getSqlTypeName() == SqlTypeName.INTEGER) {

                // Now initialise a hash-table to keep track of the state of each aggregate
                // depending on the aggregation function and data type
                for (int i = 0; i < aggregationCalls.size(); i++) {
                    // Get information to classify the current aggregation call
                    AggregateCall currentCall = aggregationCalls.get(i);
                    SqlAggFunction currentCallFunction = currentCall.getAggregation();

                    // Check that the distinct keyword is not used by the current call
                    if (currentCall.isDistinct())
                        throw new UnsupportedOperationException(
                                "AggregationOperator.initialiseAggregationState does not support DISTINCT keyword in group-by aggregation");

                    // Allocate the required variables based on the aggregation type
                    if (currentCallFunction instanceof SqlSumEmptyIsZeroAggFunction sumEmptyIsZeroAggFunction) {
                        // Simply allocate the correct hash-table type based on the result type
                        String aggregationStateVariableName = cCtx.defineVariable("aggCall_" + i + "_state");
                        this.aggregationStateMappings.add(i, new AggregationStateMapping(
                                aggregationStateVariableName,
                                AggregationStateMapping.AggregationStateVariableType.MAP_INT_INT));

                        if (currentCall.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                            // Key is of type int, result is of type int --> we need an IntInt map
                            Java.Type intIntHashMapType = createReferenceType(getLocation(), "Simple_Int_Int_Map");
                            codeGenResult.add(
                                    createLocalVariable(
                                            getLocation(),
                                            intIntHashMapType,
                                            aggregationStateVariableName,
                                            createClassInstance(getLocation(), intIntHashMapType)
                                    )
                            );

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.initialiseAggregationState only supports an integer" +
                                            " result for the SqlSumEmptyIsZeroAggFunction");
                        }

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.initialiseAggregationState does not support this group-by aggregation function: "
                                        + currentCallFunction.getName());
                    }

                }

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.initialiseAggregationState currently only supports single integer-column group-by's");
            }
        }

        return codeGenResult;
    }

}
