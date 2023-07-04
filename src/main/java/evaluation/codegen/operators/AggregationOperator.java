package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import evaluation.codegen.infrastructure.context.access_path.IndexedMapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.general_support.hashmaps.Int_Hash_Function;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_LONG_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.MAP_INT_LONG_SIMPLE;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.valueTypeForMap;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrementStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

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
        public final QueryVariableType[] variableTypes;

        /**
         * Array of statements to initialise the aggregation state.
         */
        public final Java.ArrayInitializerOrRvalue[] variableInitialisationStatements;

        /**
         * Array indicating whether the variable in the aggregation state needs to be released via
         * the allocation manager at the end of the query.
         */
        public final boolean[] variableNeedsRelease;

        /**
         * Creates a new {@link AggregationStateMapping} instance for a single variable.
         * @param variableName The name of the aggregation state variable.
         * @param variableType The type of the aggregation state variable.
         * @param initialiser The statement to initialise the aggregation state variable.
         * @param needsRelease Whether the aggregation state variable needs to be released via the
         *                     allocation manager at the end of the query.
         */
        public AggregationStateMapping(
                String variableName,
                QueryVariableType variableType,
                Java.ArrayInitializerOrRvalue initialiser,
                boolean needsRelease)
        {
            this.variableNames = new String[] { variableName };
            this.variableTypes = new QueryVariableType[] { variableType };
            this.variableInitialisationStatements = new Java.ArrayInitializerOrRvalue[] { initialiser };
            this.variableNeedsRelease = new boolean[] { needsRelease };
        }

        /**
         * Creates a new {@link AggregationStateMapping} instance for a multiple variables.
         * @param variableNames The names of the aggregation state variables.
         * @param variableTypes The types of the aggregation state variables.
         * @param initialisers The statements to initialise each aggregation state variable.
         * @param needsRelease Whether each of the aggregation state variables needs to be released
         *                     via the allocation manager at the end of the query.
         */
        public AggregationStateMapping(
                String[] variableNames,
                QueryVariableType[] variableTypes,
                Java.ArrayInitializerOrRvalue[] initialisers,
                boolean[] needsRelease)
        {
            this.variableNames = variableNames;
            this.variableTypes = variableTypes;
            this.variableInitialisationStatements = initialisers;
            this.variableNeedsRelease = needsRelease;
        }
    }

    /**
     * Whether this aggregation represents a group-by aggregation.
     */
    private final boolean groupByAggregation;

    /**
     * The input ordinal of the group-by key column if this operator is a group-by aggregation and
     * -1 otherwise. Note that this implies that only a single group column is supported like this.
     */
    private final int groupByKeyColumnIndex;

    /**
     * The primitive scalar type of the group-by key column if this operator is a group-by aggregation
     * and null otherwise. Note that this value will only be set after the {@code declareAggregationState}
     * method has been invoked.
     */
    private QueryVariableType groupByKeyColumnPrimitiveScalarType;

    /**
     * An enum indicating the internal type of each aggregation function of this operator.
     */
    private enum AggregationFunction {
        // Non-group-by functions
        NG_COUNT,

        // Group-by functions
        G_SUM
    }

    /**
     * The internal classification of the aggregation functions to be generated by this operator.
     */
    private final AggregationFunction[] aggregationFunctions;

    /**
     * The list of input ordinals per aggregation function to be generated by this operator.
     */
    private final int[][] aggregationFunctionInputOrdinals;

    /**
     * A long array vector which can be used for the vectorised implementation of this operator
     * to compute pre-hash vectors. The vectorised implementation needs to allocate this access path
     * itself.
     */
    private ArrayAccessPath groupKeyPreHashVector = null;

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

        // Initialise the list of aggregation state mapping per aggregation call
        this.aggregationStateMappings = new ArrayList<>(aggregation.getAggCallList().size());

        // Check pre-conditions
        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator: We expect exactly one GroupSet to exist in the logical plan");

        for (AggregateCall call : this.getLogicalSubplan().getAggCallList())
            if (call.isDistinct())
                throw new UnsupportedOperationException(
                        "AggregationOperator does not support DISTINCT keyword");

        // Classify all aggregation functions
        this.groupByAggregation = this.getLogicalSubplan().getGroupSet().cardinality() > 0;
        if (this.groupByAggregation)
            this.groupByKeyColumnIndex = this.getLogicalSubplan().getGroupSet().nextSetBit(0);
        else
            this.groupByKeyColumnIndex = -1;

        this.aggregationFunctions = new AggregationFunction[aggregation.getAggCallList().size()];
        this.aggregationFunctionInputOrdinals = new int[this.aggregationFunctions.length][];
        for (int i = 0; i < this.aggregationFunctions.length; i++) {
            AggregateCall call = aggregation.getAggCallList().get(i);
            SqlAggFunction aggregationFunction = call.getAggregation();

            if (!this.groupByAggregation) { // Simple non-group-by aggregations
                if (aggregationFunction instanceof SqlCountAggFunction) {
                    this.aggregationFunctions[i] = AggregationFunction.NG_COUNT;
                    this.aggregationFunctionInputOrdinals[i] = new int[0];

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator does not support this non-group-by aggregation function " + aggregationFunction.getName());
                }

            } else { // Group-by aggregations
                if (aggregationFunction instanceof SqlSumEmptyIsZeroAggFunction) {
                    if (call.getArgList().size() != 1)
                        throw new UnsupportedOperationException("AggregationOperator expects exactly one input ordinal for a SUM aggregation function");

                    this.aggregationFunctions[i] = AggregationFunction.G_SUM;
                    this.aggregationFunctionInputOrdinals[i] = new int[] { call.getArgList().get((0)) };

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator does not support this group-by aggregation function " + aggregationFunction.getName());
                }

            }
        }
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
        List<Java.Statement> codeGenResult;

        // Store the context and continue production in the child operator, so that eventually this
        // operator's consumeNonVec method is invoked, which will declare the aggregation states
        cCtx.pushCodeGenContext();
        List<Java.Statement> childProductionResult = this.child.produceNonVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // Add the aggregation state variable definitions before the codegen result
        codeGenResult = initialiseAggregationStates();
        codeGenResult.addAll(childProductionResult);

        // Expose the result of this operator to its parent as a new "scan" (since aggregation is blocking)
        // Exposure way depends on whether we are dealing with a group-by aggregation and the aggregation function
        if (!this.groupByAggregation) {
            // In a non-group-by aggregate, the result is always a scalar, so there is no difference between
            // SIMD enabled operator and non-SIMD enabled operator here.
            // Expose the result per aggregation call by updating the ordinal mapping
            List<AccessPath> newOrdinalMapping = new ArrayList<>(this.aggregationFunctions.length);

            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.NG_COUNT) {
                    // Simply set the current ordinal to refer to the count variable
                    newOrdinalMapping.add(
                            i,
                            new ScalarVariableAccessPath(
                                    this.aggregationStateMappings.get(i).variableNames[0],
                                    this.aggregationStateMappings.get(i).variableTypes[0]));

                }

                // No other possibilities
            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

            // Have the parent operator consume the result
            codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        } else {
            // Expose the result of the group-by aggregation
            // Even though the group-by aggregation needs to distinguish between SIMD and non-SIMD
            // processing, we handle some code generation in a shared fashion

            // We obtain an iterator to iterate over the group-by key values based on the first aggregation state's first variable
            AggregationStateMapping firstAggregationState = this.aggregationStateMappings.get(0);
            QueryVariableType firstAggregationStateType = firstAggregationState.variableTypes[0];

            String groupKeyIteratorName;
            if (firstAggregationStateType == MAP_INT_LONG_SIMPLE) {
                // Simple_Int_Long_Map [groupKeyIterator] = [firstAggregationState.variableNames[0]].getIterator();
                groupKeyIteratorName = cCtx.defineVariable("groupKeyIterator");
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
            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.produceNonVec does not support obtaining group-by key values from " + firstAggregationStateType);
            }

            // Now generate a while loop to iterate over the keys
            // while ([groupKeyIterator].hasNext()]) { [whileLoopBody] }
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

            // Generate the value exposure within the whileLoopBody and update the ordinal mapping
            // Currently, this operator only supports exposing the result in a non-SIMD fashion.
            // TODO: consider SIMD-compatible result exposure when required.

            // Property: group-by aggregates first expose the group key and then the aggregate results
            List<AccessPath> newOrdinalMapping = new ArrayList<>(this.aggregationFunctions.length + 1);
            int currentOrdinalIndex = 0;

            // Simply expose each record as a set of scalar variables
            // First expose the group-by key values
            // int [groupKey] = [groupKeyIterator].next();
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
            newOrdinalMapping.add(currentOrdinalIndex++, groupKeyAP);

            // Do the pre-hash computation
            // long [groupKey]PreHash = Int_Hash_Function.preHash([groupKey]);
            ScalarVariableAccessPath groupKeyPreHashAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable(groupKeyAP.getVariableName() + "PreHash"),
                    P_LONG
            );
            whileLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), groupKeyPreHashAP.getType()),
                            groupKeyPreHashAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "Int_Hash_Function"),
                                    "preHash",
                                    new Java.Rvalue[] { groupKeyAP.read() }
                            )
                    ));

            // Then expose the aggregate results per aggregation function
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_SUM) {
                    // Simply need to invoke the get method of the correct hash-map using the groupKey
                    String aggregationMapName = this.aggregationStateMappings.get(i).variableNames[0];
                    QueryVariableType aggregationMapType = this.aggregationStateMappings.get(i).variableTypes[0];
                    MapAccessPath aggregationMapAP = new MapAccessPath(aggregationMapName, aggregationMapType);
                    IndexedMapAccessPath indexedAggregationMapAP =
                            new IndexedMapAccessPath(aggregationMapAP, groupKeyAP, groupKeyPreHashAP, valueTypeForMap(aggregationMapType));
                    newOrdinalMapping.add(currentOrdinalIndex++, indexedAggregationMapAP);

                }

                // No other possibilities due to constructor
            }

            // Have the parent operator consume the result
            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);
            whileLoopBody.addStatements(this.nonVecParentConsume(cCtx, oCtx));
        }

        // Release the aggregation state variables as needed
        codeGenResult.addAll(this.releaseAggregationStates());

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Declare the required aggregation state
        this.declareAggregationState(cCtx);

        // Handle the aggregation state update depending on whether we have a group-by aggregation
        if (!groupByAggregation) { // Regular scalar processing
            boolean useSIMD = this.useSIMDNonVec(cCtx);

            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                switch (currentFunction) {
                    case NG_COUNT -> {
                        // Check the type of the first ordinal to be able to generate the correct count update statements
                        AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
                        if (!useSIMD && firstOrdinalAP instanceof ScalarVariableAccessPath) {
                            // For a count aggregation over scalar variables simply increment the relevant variable.
                            codeGenResult.add(
                                    postIncrementStm(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), this.aggregationStateMappings.get(i).variableNames[0])
                                    ));

                        } else if (!useSIMD && firstOrdinalAP instanceof IndexedArrowVectorElementAccessPath iaveap) {
                            // For an indexed arrow vector element access path ("for loop over arrow vector elements")
                            // simply increment the relevant count variable
                            codeGenResult.add(
                                    postIncrementStm(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), this.aggregationStateMappings.get(i).variableNames[0])
                                    ));

                        } else if (useSIMD && firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
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

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.consumeNonVec does not support this AccessPath for the COUNT aggregation while "
                                            + (useSIMD ? "" : "not ") + "using SIMD: "  + firstOrdinalAP);
                        }
                    }

                    // No other cases possible due to constructor
                }

            }
        } else { // Group-by processing
            // Idea: first perform the hashing of the key-column, then perform the hash map maintenance
            AccessPath keyColumnAccessPath;
            ScalarVariableAccessPath keyColumnPreHashAccessPath;
            Java.Rvalue[] aggregationValues = new Java.Rvalue[this.aggregationFunctions.length];
            Java.Block hashMapMaintenanceTarget;

            if (this.groupByKeyColumnPrimitiveScalarType == P_INT) {

                // Hashing of key-column depends on whether we are in SIMD mode or not
                if (!this.useSIMDNonVec(cCtx)) {
                    // Ensure we have a "local" access path for the key column value
                    Java.Rvalue keyColumnRValue = getRValueFromAccessPathNonVec(cCtx, this.groupByKeyColumnIndex, codeGenResult);
                    keyColumnAccessPath = cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndex);

                    // Now compute the pre-hash value in a local variable
                    keyColumnPreHashAccessPath =
                            new ScalarVariableAccessPath(cCtx.defineVariable("group_key_pre_hash"), P_LONG);
                    codeGenResult.add(
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), keyColumnPreHashAccessPath.getType()),
                                    keyColumnPreHashAccessPath.getVariableName(),
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "Int_Hash_Function"),
                                            "preHash",
                                            new Java.Rvalue[]{ keyColumnRValue }
                                    )
                            ));

                    // Also obtain the values to insert into the hash-map later on
                    for (int i = 0; i < this.aggregationFunctions.length; i++) {
                        AggregationFunction currentFunction = this.aggregationFunctions[i];

                        if (currentFunction == AggregationFunction.G_SUM) {
                            aggregationValues[i] = this.getRValueFromAccessPathNonVec(cCtx, this.aggregationFunctionInputOrdinals[i][0], codeGenResult);
                        }

                        // No other possibilities due to the constructor

                    }

                    // Set the correct hashMapMaintenanceTarget
                    hashMapMaintenanceTarget = createBlock(getLocation());
                    codeGenResult.add(hashMapMaintenanceTarget);

                } else { // this.useSIMDNonVec(cCtx)

                    // Perform the SIMD-ed pre-hashing and flattening
                    var preHashAndFlattenResult = Int_Hash_Function.preHashAndFlattenSIMD(
                            cCtx,
                            cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndex)
                    );
                    keyColumnAccessPath = preHashAndFlattenResult.keyColumnAccessPath;
                    keyColumnPreHashAccessPath = preHashAndFlattenResult.keyColumnPreHashAccessPath;
                    codeGenResult.addAll(preHashAndFlattenResult.generatedCode);

                    // Obtain the values to insert into the hash-map later on
                    for (int i = 0; i < this.aggregationFunctions.length; i++) {
                        AggregationFunction currentFunction = this.aggregationFunctions[i];

                        if (currentFunction == AggregationFunction.G_SUM) {

                            AccessPath valueAP = cCtx.getCurrentOrdinalMapping().get(this.aggregationFunctionInputOrdinals[i][0]);
                            if (valueAP instanceof SIMDLoopAccessPath valueAP_slap) {
                                // aggregationValues[i] = [valueAP_slap.readArrowVector()].get([valueAP_slap.readArrowVectorOffset()] + [simd_vector_i]);
                                aggregationValues[i] = createMethodInvocation(
                                        getLocation(),
                                        valueAP_slap.readArrowVector(),
                                        "get",
                                        new Java.Rvalue[]{
                                                plus(
                                                        getLocation(),
                                                        valueAP_slap.readArrowVectorOffset(),
                                                        preHashAndFlattenResult.simdVectorIAp.read()
                                                )
                                        }
                                );

                            } else {
                                throw new UnsupportedOperationException(
                                        "AggregationOperator.consumeNonVec does not support this valueAP for hash-map maintenance");
                            }
                        }
                    }

                    // And set the correct hashMapMaintenanceTarget
                    hashMapMaintenanceTarget = preHashAndFlattenResult.flattenedForLoopBody;
                }

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.consumeNonVec does not support group-by key column type " + this.groupByKeyColumnPrimitiveScalarType);
            }

            // Now perform hash-table maintenance
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_SUM) {
                    // In the case of SIMD, the access path will have been flattened, so there is no need to distinguish here anymore
                    // We simply need to invoke the hashmap's "addToKeyOrPutIfNotExist" method for the group-by key and the value
                    hashMapMaintenanceTarget.addStatement(
                            createMethodInvocationStm(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), this.aggregationStateMappings.get(i).variableNames[0]),
                                    "addToKeyOrPutIfNotExist",
                                    new Java.Rvalue[] {
                                            ((ScalarVariableAccessPath) keyColumnAccessPath).read(),        // Group-By Key
                                            keyColumnPreHashAccessPath.read(),                              // Group-By Key pre-hash
                                            aggregationValues[i]                                            // Value
                                    }
                            )
                    );

                }

                // No other possibilities due to the constructor

            }

        }

        // Do not consume parent operator here, but in the produce method since the aggregation is a blocking operator
        return codeGenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Allocate the pre-hash vector if necessary
        if (this.groupByAggregation) {
            // long[] groupKeyPreHashVector = cCtx.getAllocationManager().getLongVector();
            this.groupKeyPreHashVector = new ArrayAccessPath(cCtx.defineVariable("groupKeyPreHashVector"), P_A_LONG);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), this.groupKeyPreHashVector.getType()),
                            this.groupKeyPreHashVector.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"),"getAllocationManager"),
                                    "getLongVector"
                            )
                    )
            );
        }

        // Store the context and continue production in the child operator, so that eventually this
        // operator's consumeVec method is invoked, which will declare the aggregation states
        cCtx.pushCodeGenContext();
        List<Java.Statement> childProductionResult = this.child.produceVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // Add the aggregation state variable definitions to the code gen result
        codeGenResult.addAll(initialiseAggregationStates());
        codeGenResult.addAll(childProductionResult);

        // Expose the result of this operator to its parent as a new "scan" (since aggregation is blocking)
        // Exposure way depends on whether we are dealing with a group-by aggregation and the aggregation function
        if (!this.groupByAggregation) {
            // Non-group-by aggregates always result in a scalar value, so there is no need to distinguish
            // between SIMD and Non-SIMD processing
            // Expose the result per aggregation call by updating the ordinal mapping
            List<AccessPath> newOrdinalMapping = new ArrayList<>(this.aggregationFunctions.length);

            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.NG_COUNT) {
                    // Simply set the current ordinal to refer to the count variable
                    newOrdinalMapping.add(
                            i,
                            new ScalarVariableAccessPath(
                                    this.aggregationStateMappings.get(i).variableNames[0],
                                    this.aggregationStateMappings.get(i).variableTypes[0]));

                }

                // No other possibilities
            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

            // Have the parent operator consume the result
            codeGenResult.addAll(this.vecParentConsume(cCtx, oCtx));

        } else {
            // Expose the result of the group-by aggregation: we don't need to distinguish between SIMD and Non-SIMD processing
            // since the vectorised operators will create SIMD vectors themselves.

            // First gather and allocate the types of vectors that will be exposed in the result while updating the ordinal mapping
            List<AccessPath> updatedOrdinalMapping = new ArrayList<>(this.aggregationStateMappings.size() + 1);

            // Property: group-by aggregates first expose the group-by key, then each aggregate result
            // First, we allocate the key vector based on the first aggregation state's first variable
            AggregationStateMapping firstAggregationState = this.aggregationStateMappings.get(0);
            QueryVariableType firstAggregationStateType = firstAggregationState.variableTypes[0];
            ArrayVectorAccessPath groupKeyVectorAP;

            // int aggregationResultVectorLength;
            ScalarVariableAccessPath aggregationResultVectorLengthAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("aggregationResultVectorLength"),
                    P_INT
            );
            codeGenResult.add(createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, aggregationResultVectorLengthAP.getVariableName()));

            if (firstAggregationStateType == MAP_INT_LONG_SIMPLE) {
                // We have an integer key type, so allocate an integer key vector
                // int[] groupKeyVector = cCtx.getAllocationManager().getIntVector();
                ArrayAccessPath groupKeyVectorArrayAP = new ArrayAccessPath(cCtx.defineVariable("groupKeyVector"), P_A_INT);
                codeGenResult.add(createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), groupKeyVectorArrayAP.getType()),
                        groupKeyVectorArrayAP.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"),"getAllocationManager"),
                                "getIntVector"
                        )
                ));
                groupKeyVectorAP = new ArrayVectorAccessPath(groupKeyVectorArrayAP, aggregationResultVectorLengthAP, ARRAY_INT_VECTOR);
                updatedOrdinalMapping.add(0, groupKeyVectorAP);

            } else {
                throw new UnsupportedOperationException("AggregationOperator.produceVec does not currently support" +
                        "obtaining keys from aggregation state type " + firstAggregationStateType);
            }

            // Allocate the vectors for the aggregation functions based on the aggregation function type
            ArrayVectorAccessPath[] aggregationResultAPs = new ArrayVectorAccessPath[this.aggregationFunctions.length];
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                // Define the vector name as "aggregation_[currentFunction]_[i]_vector"
                String aggregationResultVectorName = cCtx.defineVariable("agg_" + currentFunction + "_" + i + "_vector");

                // Expose the result based on the function type
                if (currentFunction == AggregationFunction.G_SUM) {
                    // The vector type simply corresponds to the value type of the hash-map
                    QueryVariableType aggregationMapType = this.aggregationStateMappings.get(i).variableTypes[0];

                    if (aggregationMapType == MAP_INT_LONG_SIMPLE) {
                        // Need to allocate a long vector
                        ArrayAccessPath aggregationResultVectorAP = new ArrayAccessPath(aggregationResultVectorName, P_A_LONG);
                        codeGenResult.add(createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), aggregationResultVectorAP.getType()),
                                aggregationResultVectorAP.getVariableName(),
                                createMethodInvocation(
                                        getLocation(),
                                        createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"), "getAllocationManager"),
                                        "getLongVector"
                                )
                        ));

                        aggregationResultAPs[i] = new ArrayVectorAccessPath(aggregationResultVectorAP, aggregationResultVectorLengthAP, ARRAY_LONG_VECTOR);

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.produceVec cannot allocate a vector for the aggregation result of type " + aggregationMapType);
                    }

                }

                // No other possibilities due to constructor

                updatedOrdinalMapping.add(i + 1, aggregationResultAPs[i]);
            }

            // Set the updated ordinal mapping
            cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

            // Construct and expose the actual vectors
            // Get an iterator for they keys based on the first aggregation state's first aggregation variable
            String groupKeyIteratorName = cCtx.defineVariable("groupKeyIterator");

            if (firstAggregationStateType == MAP_INT_LONG_SIMPLE) {
                // Simple_Int_Long_Map [groupKeyIterator] = [firstAggregationState.variableNames[0]].getIterator();
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

            } else {
                throw new UnsupportedOperationException("AggregationOperator.produceVec does not currently support" +
                        "obtaining keys from aggregation state type " + firstAggregationStateType);
            }

            // Construct the key and value vectors using this iterator
            // while ([groupKeyIterator].hasNext()] {
            //     [aggregationResultVectorLength] = VectorisedAggregationOperators.constructKeyVector([groupKeyVector], [groupKeyIterator]);
            //     VectorisedHashOperators.constructPreHashKeyVector([groupKeyPreHashVector], [groupKeyVector], [aggregationResultVectorLength])
            //     $ for each aggregationResult $
            //     VectorisedAggregationOperators.constructValueVector([aggCall_$i$_vector], [groupKeyVector], [groupKeyPreHashVector], [aggregationResultVectorLength], [aggCall_$i$_state]);
            //     [whileLoopBody]
            // }

            // Iterate over the groupKeyIterator
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

            // Construct the key vector
            whileLoopBody.addStatement(
                    createVariableAssignmentStm(
                            getLocation(),
                            aggregationResultVectorLengthAP.write(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedAggregationOperators"),
                                    "constructKeyVector",
                                    new Java.Rvalue[] {
                                            groupKeyVectorAP.getVectorVariable().read(),
                                            createAmbiguousNameRef(getLocation(), groupKeyIteratorName)
                                    }
                            )
                    )
            );

            // Construct the pre-hash vector for the key
            whileLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVector",
                            new Java.Rvalue[] {
                                    this.groupKeyPreHashVector.read(),
                                    groupKeyVectorAP.getVectorVariable().read(),
                                    groupKeyVectorAP.getVectorLengthVariable().read()
                            }
                    )
            );

            // Create each value vector based on the aggregation type
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_SUM) {
                    // The value vector can simply be constructed by obtaining values from the aggregation state map based on the keys
                    String aggregationMapVariableName = this.aggregationStateMappings.get(i).variableNames[0];
                    whileLoopBody.addStatement(
                            createMethodInvocationStm(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedAggregationOperators"),
                                    "constructValueVector",
                                    new Java.Rvalue[] {
                                            aggregationResultAPs[i].getVectorVariable().read(),
                                            groupKeyVectorAP.getVectorVariable().read(),
                                            this.groupKeyPreHashVector.read(),
                                            aggregationResultVectorLengthAP.read(),
                                            createAmbiguousNameRef(getLocation(), aggregationMapVariableName)
                                    }
                            )
                    );

                }

                // No other possibilities due to constructor
            }

            // Have the parent operator consume the result
            whileLoopBody.addStatements(this.vecParentConsume(cCtx, oCtx));

            // Generate the statements to deallocate the vectors as they won't be used anymore
            for (AccessPath accessPath : updatedOrdinalMapping) {
                // cCtx.getAllocationManager.release(accessPath.getVectorVariable().read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"), "getAllocationManager"),
                                "release",
                                new Java.Rvalue[] {
                                        // Cast to ArrayVectorAccessPath valid as this is produced by this method
                                        ((ArrayVectorAccessPath) accessPath).getVectorVariable().read()
                                }
                        ));
            }
        }

        // Release the groupKeyPreHashVector if needed
        if (this.groupKeyPreHashVector != null) {
            // cCtx.getAllocationManager.release([this.groupKeyPreHashVectorread()]);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"), "getAllocationManager"),
                            "release",
                            new Java.Rvalue[]{this.groupKeyPreHashVector.read()}
                    ));
        }

        // Release the aggregation state variables as needed
        codeGenResult.addAll(this.releaseAggregationStates());

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Declare the required aggregation state
        this.declareAggregationState(cCtx);

        // Handle the aggregation state update depending on whether we have a group-by aggregation
        if (!this.groupByAggregation) {

            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                switch (currentFunction) {
                    case NG_COUNT -> {
                        // We simply need to add the length of the valid part of the current vector to the count variable
                        AccessPath firstOrdinalAP = cCtx.getCurrentOrdinalMapping().get(0);
                        Java.Rvalue countIncrementRValue;

                        if (firstOrdinalAP instanceof ArrowVectorAccessPath avap) {
                            // count += avap.read().getValueCount();
                            countIncrementRValue = createMethodInvocation(getLocation(), avap.read(), "getValueCount");

                        } else if (firstOrdinalAP instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                            // count += avwsvap.readSelectionVectorLength();
                            countIncrementRValue = avwsvap.readSelectionVectorLength();

                        } else if (firstOrdinalAP instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                            // count += VectorisedAggregationOperators.count(avwvmap.readValidityMask(), avwvmap.readValidityMaskLength());
                            countIncrementRValue = createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedAggregationOperators"),
                                    "count",
                                    new Java.Rvalue[]{
                                            avwvmap.readValidityMask(),
                                            avwvmap.readValidityMaskLength()
                                    }
                            );

                        } else if (firstOrdinalAP instanceof ArrayVectorAccessPath avap) {
                            // count += avap.getVectorLengthVariable().read();
                            countIncrementRValue = avap.getVectorLengthVariable().read();

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.consumeVec does not support this access path for the count aggregation");

                        }

                        // Do the actual increment
                        // count += [countIncrementRValue];
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        getLocation(),
                                        createAmbiguousNameRef(
                                                getLocation(),
                                                this.aggregationStateMappings.get(i).variableNames[0]
                                        ),
                                        countIncrementRValue
                                )
                        );

                    }

                    // No other cases possible due to constructor
                }
            }
        } else { // this.groupByAggregation
            // Idea: first perform the hashing of the key-column, then perform the hash-table maintenance
            AccessPath keyColumnAccessPath = cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndex);
            QueryVariableType keyColumnAccessPathType = keyColumnAccessPath.getType();

            if (this.groupByKeyColumnPrimitiveScalarType == P_INT) {

                // Hashing of the key-column depends on whether we are in SIMD mode or not
                if (!this.useSIMDVec()) {
                    if (keyColumnAccessPathType == ARROW_INT_VECTOR) {
                        // VectorisedHashOperators.constructPreHashKeyVector([this.groupKeyPreHashVector], [keyColumn]);
                        codeGenResult.add(
                                createMethodInvocationStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                        "constructPreHashKeyVector",
                                        new Java.Rvalue[]{
                                                this.groupKeyPreHashVector.read(),
                                                ((ArrowVectorAccessPath) keyColumnAccessPath).read()
                                        }
                                ));

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.consumeVec does not support his key column access path type for pre-hashing: " + keyColumnAccessPathType);
                    }

                } else { // this.useSIMDVec()
                    if (keyColumnAccessPathType == ARROW_INT_VECTOR) {
                        // VectorisedHashOperators.constructPreHashKeyVectorSIMD([this.groupKeyPreHashVector], [keyColumn]);
                        codeGenResult.add(
                                createMethodInvocationStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                        "constructPreHashKeyVectorSIMD",
                                        new Java.Rvalue[]{
                                                this.groupKeyPreHashVector.read(),
                                                ((ArrowVectorAccessPath) keyColumnAccessPath).read()
                                        }
                                ));

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.consumeVec does not support his key column access path type for pre-hashing: " + keyColumnAccessPathType);
                    }

                }

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.consumeNonVec does not support group-by key column type " + this.groupByKeyColumnPrimitiveScalarType);
            }

            // Now perform hash-table maintenance depending on the aggregation function
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_SUM) {
                    // We simply need to maintain the sum based on the input ordinal type
                    AccessPath inputOrdinal = cCtx.getCurrentOrdinalMapping().get(this.aggregationFunctionInputOrdinals[i][0]);

                    if (inputOrdinal.getType() == ARROW_INT_VECTOR) {
                        // We need to invoke the VectorisedAggregationOperators.maintainSum method
                        codeGenResult.add(
                                createMethodInvocationStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "VectorisedAggregationOperators"),
                                        "maintainSum",
                                        new Java.Rvalue[]{
                                                ((ArrowVectorAccessPath) keyColumnAccessPath).read(),   // Key column, cast valid due to branch
                                                this.groupKeyPreHashVector.read(),                      // Pre-hash key value
                                                ((ArrowVectorAccessPath) inputOrdinal).read(),          // Value column, cast valid due to branch
                                                createAmbiguousNameRef(getLocation(), this.aggregationStateMappings.get(i).variableNames[0]) // Map to maintain
                                        }
                                )
                        );

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.consumeVec does not support this input ordinal type for the group-by SUM aggregation: " + inputOrdinal.getType());
                    }

                }

                // No other possibilities due to constructor
            }

        }

        // Do not consume parent operator here, but in the produce method since the aggregation is a blocking operator
        return codeGenResult;
    }

    /**
     * Method to declare the aggregation state for the operator and store it in the
     * {@code this.aggregationStateMappings} field.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     */
    private void declareAggregationState(CodeGenContext cCtx) {
        List<AccessPath> om = cCtx.getCurrentOrdinalMapping();

        // Obtain the type of the group-by column if necessary
        if (this.groupByAggregation) {
            this.groupByKeyColumnPrimitiveScalarType =
                    primitiveType(om.get(this.groupByKeyColumnIndex).getType());
        }

        // Initialise the states
        for (int i = 0; i < this.aggregationFunctions.length; i++) {
            AggregationFunction currentFunction = this.aggregationFunctions[i];

            switch (currentFunction) {
                case NG_COUNT -> {
                    this.aggregationStateMappings.add(i, new AggregationStateMapping(
                            cCtx.defineVariable("agg_" + i + "_count"),
                            P_INT,
                            createIntegerLiteral(getLocation(), 0),
                            false // primitive variables need no release
                    ));
                }
                case G_SUM -> {
                    QueryVariableType valueType =
                            primitiveType(om.get(this.aggregationFunctionInputOrdinals[i][0]).getType());
                    if (groupByKeyColumnPrimitiveScalarType == P_INT && valueType == P_INT) {
                        // Key is of type int, and we upgrade the value type to long --> use Int_Long map
                        // We need to obtain the map from the allocation manager
                        this.aggregationStateMappings.add(i, new AggregationStateMapping(
                                cCtx.defineVariable("agg_" + i + "_sum_map"),
                                MAP_INT_LONG_SIMPLE,
                                // cCtx.getAllocationManager().getSimpleIntLongMap();
                                createMethodInvocation(
                                        getLocation(),
                                        createMethodInvocation(
                                                getLocation(),
                                                createAmbiguousNameRef(getLocation(), "cCtx"),
                                                "getAllocationManager"
                                        ),
                                        "getSimpleIntLongMap"
                                ),
                                true // mark this variable for release using the allocation manager, as we obtained it from there
                        ));

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.declareAggregationState: group-by sum aggregation does not key-value type combination: "
                                        + groupByKeyColumnPrimitiveScalarType + "-" + valueType);
                    }
                }
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

    /**
     * Method to release the aggregation state variables at the end of the query as configured in
     * {@code this.aggregationStateMappings}.
     */
    private List<Java.Statement> releaseAggregationStates() {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Release all required state variables via the allocation manager if configured to do so
        for (AggregationStateMapping currentMapping : this.aggregationStateMappings) {
            for (int i = 0; i < currentMapping.variableNames.length; i++) {
                if (currentMapping.variableNeedsRelease[i]) {
                    codeGenResult.add(
                            // cCtx.getAllocationManager().release(currentMapping.variableNames[i]);
                            createMethodInvocationStm(
                                    getLocation(),
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "cCtx"),
                                            "getAllocationManager"
                                    ),
                                    "release",
                                    new Java.Rvalue[] {
                                            createAmbiguousNameRef(getLocation(), currentMapping.variableNames[i])
                                    }
                            )
                    );
                }
            }
        }

        return codeGenResult;
    }

}
