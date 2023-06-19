package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.MAP_INT_LONG_SIMPLE;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.valueTypeForMap;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
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
        public QueryVariableType[] variableTypes;

        /**
         * Array of statements to initialise the aggregation state.
         */
        public Java.ArrayInitializerOrRvalue[] variableInitialisationStatements;

        /**
         * Creates a new {@link AggregationStateMapping} instance for a single variable.
         * @param variableName The name of the aggregation state variable.
         */
        public AggregationStateMapping(String variableName) {
            this.variableNames = new String[] { variableName };
            this.variableTypes = new QueryVariableType[1];
            this.variableInitialisationStatements = new Java.ArrayInitializerOrRvalue[1];
        }

        /**
         * Creates a new {@link AggregationStateMapping} instance for a multiple variables.
         * @param variableNames The names of the aggregation state variables.
         */
        public AggregationStateMapping(String[] variableNames) {
            this.variableNames = variableNames;
            this.variableTypes = new QueryVariableType[this.variableNames.length];
            this.variableInitialisationStatements = new Java.ArrayInitializerOrRvalue[this.variableNames.length];
        }
    }

    /**
     * Whether this aggregation represents a group-by aggregation.
     */
    private final boolean groupByAggregation;

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

        // Check pre-conditions
        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator: We expect exactly one GroupSet to exist in the logical plan");

        for (AggregateCall call : this.getLogicalSubplan().getAggCallList())
            if (call.isDistinct())
                throw new UnsupportedOperationException(
                        "AggregationOperator does not support DISTINCT keyword");

        // Store some meta-data about the aggregation
        this.groupByAggregation = this.getLogicalSubplan().getGroupSet().cardinality() > 0;
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
        List<Java.Statement> codeGenResult;
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        // Declare the aggregation state names based on the operators
        this.declareAggregationState(cCtx, aggregationCalls);

        // Store the context and continue production in the child operator
        // This will also call this operator's consumeNonVec method, which will initialise the
        // aggregation state variable definitions
        cCtx.pushCodeGenContext();
        List<Java.Statement> childProductionResult = this.child.produceNonVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // Add the aggregation state variable definitions to the code gen result
        codeGenResult = initialiseAggregationStates();
        codeGenResult.addAll(childProductionResult);

        // Expose the result of this operator to its parent as a new "scan" (since aggregation is blocking)
        // Exposure way depends on whether we are dealing with a group-by aggregation and the aggregation function
        if (!this.groupByAggregation) {
            // In a non-group-by aggregate, the result is always a scalar, so there is no difference between
            // SIMD enabled operator and non-SIMD enabled operator here.
            // Expose the result per aggregation call by updating the ordinal mapping
            List<AccessPath> newOrdinalMapping = new ArrayList<>(aggregationCalls.size());

            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentCallFunction = currentCall.getAggregation();

                // Allocate the required variables based on the aggregation type
                if (currentCallFunction instanceof SqlCountAggFunction) {
                    // Simply set the current ordinal to refer to the count variable
                    newOrdinalMapping.add(
                            i,
                            new ScalarVariableAccessPath(
                                    this.aggregationStateMappings.get(i).variableNames[0],
                                    this.aggregationStateMappings.get(i).variableTypes[0]));

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
                // Regular row-based processing: create a loop iterating over the group-by key values
                // based on the first aggregation state's first variable
                AggregationStateMapping firstAggregationState = this.aggregationStateMappings.get(0);
                QueryVariableType firstAggregationStateType = firstAggregationState.variableTypes[0];

                if (firstAggregationStateType == MAP_INT_LONG_SIMPLE) {
                    // Simply iterate over the map, which will provide the keys
                    // Observation: a LogicalAggregate output always has the group-by column first,
                    // followed by the aggregation calls in the given order

                    // Simple_Int_Long_Map [groupKeyIterator] = [firstAggregationState.variableNames[0]].getIterator();
                    // while ([groupKeyIterator].hasNext()] {
                    //     int [groupKey] = [groupKeyIterator].next();
                    //     [whileLoopBody]
                    // }

                    String groupKeyIteratorName = cCtx.defineVariable("groupKeyIterator");
                    codeGenResult.add(
                            createLocalVariable(
                                    getLocation(),
                                    createReferenceType(getLocation(), "Simple_Int_Long_Map_Iterator"),
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

                    ScalarVariableAccessPath groupKeyAP = new ScalarVariableAccessPath(
                            cCtx.defineVariable("groupKey"), P_INT);
                    whileLoopBody.addStatement(
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), groupKeyAP.getType()),
                                    groupKeyAP.getVariableName(),
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

                        // Return the value for the current groupKey of the current aggregation call based on the aggregation type
                        if (currentCallFunction instanceof SqlSumEmptyIsZeroAggFunction) {
                            // Simply need to return the correct hash-map value based on the groupKey
                            String aggregationMapName = this.aggregationStateMappings.get(i).variableNames[0];
                            QueryVariableType aggregationMapType = this.aggregationStateMappings.get(i).variableTypes[0];
                            MapAccessPath aggregationMapAP = new MapAccessPath(aggregationMapName, aggregationMapType);
                            IndexedMapAccessPath indexedAggregationMapAP =
                                    new IndexedMapAccessPath(aggregationMapAP, groupKeyAP, valueTypeForMap(aggregationMapType));
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
                    throw new UnsupportedOperationException("AggregationOperator.produceNonVec does not currently support" +
                            "obtaining keys from aggregation state type " + firstAggregationStateType);
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

        // Check if we are dealing with a group-by aggregation
        if (!this.groupByAggregation) {
            // We have a simple, non-group-by aggregation. Update the state per aggregation call
            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentAggregationFunction = currentCall.getAggregation();

                // Update code depends on the aggregation function
                if (currentAggregationFunction instanceof SqlCountAggFunction) { // COUNT aggregation
                    // Set-up the correct aggregation state initialisation
                    AggregationStateMapping aggregationStateMapping = this.aggregationStateMappings.get(i);
                    aggregationStateMapping.variableTypes[0] = P_INT;
                    aggregationStateMapping.variableInitialisationStatements[0] = createIntegerLiteral(getLocation(), 0);

                    // Check the type of the first ordinal to be able to generate the correct count update statements
                    AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
                    if (firstOrdinalAP instanceof ScalarVariableAccessPath) {
                        // For a count aggregation over scalar variables simply increment the relevant variable.
                        codeGenResult.add(
                                postIncrementStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), aggregationStateMapping.variableNames[0])
                                ));
                    } else if (firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
                        // For a count aggregation over a SIMD loop access path, add the number of true entries in the valid mask
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), aggregationStateMapping.variableNames[0]),
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
            // Group-by aggregation currently only supports single-column groups of the integer type
            // First we check if we received that
            ImmutableBitSet groupSet = this.getLogicalSubplan().getGroupSet();
            int firstGroupByColumnIndex = groupSet.nextSetBit(0); // There must be at least 1 column to group by
            QueryVariableType firstGroupByColumnType = cCtx.getCurrentOrdinalMapping().get(firstGroupByColumnIndex).getType();

            if (groupSet.cardinality() == 1 && firstGroupByColumnType == P_INT) {
                // Then, check if we are working a SIMD or non-SIMD enabled fashion
                if (!this.simdEnabled) { // Regular row-based processing
                    // Now update each hash-table to reflect the correct state of each aggregate
                    // depending on the aggregation function and data type
                    for (int i = 0; i < aggregationCalls.size(); i++) {
                        // Get information to classify the current aggregation call
                        AggregateCall currentCall = aggregationCalls.get(i);
                        SqlAggFunction currentCallFunction = currentCall.getAggregation();

                        // Distinguish the required behaviour based on the aggregation type
                        if (currentCallFunction instanceof SqlSumEmptyIsZeroAggFunction) {
                            if (currentCall.getArgList().size() != 1) {
                                throw new UnsupportedOperationException("AggregationOperator.consumeNonVec received an unexpected" +
                                        "number of arguments for the SqlSumEmptyIsZeroAggFunction case");
                            }

                            int inputOrdinalIndex = currentCall.getArgList().get(0);
                            AccessPath inputOrdinal = cCtx.getCurrentOrdinalMapping().get(inputOrdinalIndex);

                            // Need to maintain a hash-table based on the input ordinal type
                            if (inputOrdinal.getType() == P_INT) {
                                // Key and value are both of type int --> we want to upgrade the result to a long
                                // Set-up the correct aggregation state initialisation
                                AggregationStateMapping aggregationStateMapping = this.aggregationStateMappings.get(i);
                                aggregationStateMapping.variableTypes[0] = MAP_INT_LONG_SIMPLE;
                                Java.Type intLongHashMapType = toJavaType(getLocation(), aggregationStateMapping.variableTypes[0]);
                                aggregationStateMapping.variableInitialisationStatements[0] =
                                                createClassInstance(getLocation(), intLongHashMapType);

                                // We simply need to invoke its "addToKeyOrPutIfNotExist" method for the group-by key and the value
                                codeGenResult.add(
                                        createMethodInvocationStm(
                                                getLocation(),
                                                createAmbiguousNameRef(getLocation(), aggregationStateMapping.variableNames[0]),
                                                "addToKeyOrPutIfNotExist",
                                                new Java.Rvalue[] {
                                                        this.getRValueFromAccessPathNonVec(         // Group-By Key
                                                                cCtx,
                                                                firstGroupByColumnIndex,
                                                                codeGenResult),
                                                        this.getRValueFromAccessPathNonVec(         // Value
                                                                cCtx,
                                                                inputOrdinalIndex,
                                                                codeGenResult)
                                                }
                                        )
                                );

                            } else {
                                throw new UnsupportedOperationException(
                                        "AggregationOperator.consumeNonVec only supports an integer" +
                                                " values for the SqlSumEmptyIsZeroAggFunction");
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
        List<Java.Statement> codeGenResult;
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        // Initialise the aggregation state
        this.declareAggregationState(cCtx, aggregationCalls);

        // Store the context and continue production in the child operator
        // This will also call this operator's consumeVec method, which will initialise the
        // aggregation state variable definitions
        cCtx.pushCodeGenContext();
        List<Java.Statement> childProductionResult = this.child.produceVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // Add the aggregation state variable definitions to the code gen result
        codeGenResult = initialiseAggregationStates();
        codeGenResult.addAll(childProductionResult);

        // Expose the result of this operator to its parent as a new "scan" (since aggregation is blocking)
        // Exposure way depends on whether we are dealing with a group-by aggregation and the aggregation function
        if (!this.groupByAggregation) {
            // Non-group-by aggregates always result in a scalar value, so there is no need to distinguish
            // between SIMD and Non-SIMD processing
            // Expose the result per aggregation call by updating the ordinal mapping
            List<AccessPath> newOrdinalMapping = new ArrayList<>(aggregationCalls.size());

            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentCallFunction = currentCall.getAggregation();

                // Expose the required variables based on the aggregation type
                if (currentCallFunction instanceof SqlCountAggFunction) {
                    // Simply set the current ordinal to refer to the count variable
                    newOrdinalMapping.add(
                            i,
                            new ScalarVariableAccessPath(
                                    this.aggregationStateMappings.get(i).variableNames[0],
                                    this.aggregationStateMappings.get(i).variableTypes[0]));

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.produceVec does not support aggregation function " + currentCallFunction.getName());
                }

            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

            // Have the parent operator consume the result
            codeGenResult.addAll(this.vecParentConsume(cCtx, oCtx));

        } else {
            // Group-by aggregation
            throw new UnsupportedOperationException(
                    "AggregationOperator.produceVec does not support group-by aggregates");
        }

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();
        List<AggregateCall> aggregationCalls = this.getLogicalSubplan().getAggCallList();

        // Check if we are dealing with a group-by aggregation
        if (!this.groupByAggregation) {
            // We have a simple, non-group-by aggregation. Update the state per aggregation call
            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentAggregationFunction = currentCall.getAggregation();

                // Update code depends on the aggregation function
                if (currentAggregationFunction instanceof SqlCountAggFunction) {
                    // Set-up the correct aggregation state initialisation
                    AggregationStateMapping aggregationStateMapping = this.aggregationStateMappings.get(i);
                    aggregationStateMapping.variableTypes[0] = P_INT;
                    aggregationStateMapping.variableInitialisationStatements[0] = createIntegerLiteral(getLocation(), 0);

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
                                                aggregationStateMapping.variableNames[0]
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
                                                aggregationStateMapping.variableNames[0]
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
                                                aggregationStateMapping.variableNames[0]
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
     * Method to handle declare the names of the aggregation state for the operator
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param aggregationCalls The aggregation calls for which to initialise the state.
     */
    private void declareAggregationState(CodeGenContext cCtx, List<AggregateCall> aggregationCalls) {
        // Handle the initialisation of the aggregation based on whether we have a group-by
        if (!groupByAggregation) {
            // Initialise the state per aggregation call
            for (int i = 0; i < aggregationCalls.size(); i++) {
                // Get information to classify the current aggregation call
                AggregateCall currentCall = aggregationCalls.get(i);
                SqlAggFunction currentCallFunction = currentCall.getAggregation();

                // Allocate the required variables based on the aggregation type
                if (currentCallFunction instanceof SqlCountAggFunction) {
                    // Declare the name [aggCall_$i$_state]
                    String aggregationStateVariableName = cCtx.defineVariable("aggCall_" + i + "_state");
                    this.aggregationStateMappings.add(i, new AggregationStateMapping(aggregationStateVariableName));

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.declareAggregationState does not support aggregation function: "
                                    + currentCallFunction.getName());
                }
            }

        } else {
            // Group-by aggregation currently only support single-column groups (of integer type, checked later on)
            ImmutableBitSet groupSet = this.getLogicalSubplan().getGroupSet();

            if (groupSet.cardinality() == 1) {
                // Now declare the state of each aggregate depending on the aggregation function
                for (int i = 0; i < aggregationCalls.size(); i++) {
                    // Get information to classify the current aggregation call
                    AggregateCall currentCall = aggregationCalls.get(i);
                    SqlAggFunction currentCallFunction = currentCall.getAggregation();

                    if (currentCallFunction instanceof SqlSumEmptyIsZeroAggFunction) {
                        // Check there is only a single input column
                        if (currentCall.getArgList().size() != 1)
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.declareAggregationState received an unexpected" +
                                    "number of arguments for the SqlSumEmptyIsZeroAggFunction case");

                        // Simply declare the name [aggCall_i_state]
                        String aggregationStateVariableName = cCtx.defineVariable("aggCall_" + i + "_state");
                        this.aggregationStateMappings.add(i, new AggregationStateMapping(aggregationStateVariableName));

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.declareAggregationState does not support this group-by aggregation function: "
                                        + currentCallFunction.getName());
                    }

                }

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.declareAggregationState currently only supports single column group-by's");
            }
        }
    }

    /**
     * Method to initialise the aggregation state variables per aggregation operator as stored in
     * {@code this.aggregationStateMappings}.
     */
    private List<Java.Statement> initialiseAggregationStates() {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Allocate all required state variables as local variables
        for (AggregationStateMapping currentMapping : this.aggregationStateMappings) {
            for (int i = 0; i < currentMapping.variableNames.length; i++) {
                codeGenResult.add(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), currentMapping.variableTypes[i]),
                                currentMapping.variableNames[i],
                                currentMapping.variableInitialisationStatements[i]
                        )
                );
            }
        }

        return codeGenResult;
    }

}
