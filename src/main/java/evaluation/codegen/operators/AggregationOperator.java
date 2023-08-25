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
import evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.general_support.hashmaps.Int_Hash_Function;
import evaluation.general_support.hashmaps.KeyValueMapGenerator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_VALIDITY_MASK;
import static evaluation.codegen.infrastructure.context.QueryVariableType.MAP_GENERATED;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_DOUBLE;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableType.S_A_FL_BIN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.S_FL_BIN;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrementStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableXorAssignmentStm;

/**
 * A {@link CodeGenOperator} which computes some aggregation function over the records.
 */
public class AggregationOperator extends CodeGenOperator<LogicalAggregate> {

    /**
     * The {@link CodeGenOperator} producing the records to be aggregated by {@code this}.
     */
    private final CodeGenOperator<?> child;

    /**
     * Whether this aggregation represents a group-by aggregation.
     */
    private final boolean groupByAggregation;

    /**
     * The input ordinals of the columns used as group-by key if this operator is a group-by
     * aggregation and null otherwise.
     */
    private final int[] groupByKeyColumnIndices;

    /**
     * The types of the group-by key columns if this operator is a group-by aggregation and null
     * otherwise. Note that this value will only be set after the {@code declareAggregationState}
     * method has been invoked.
     */
    private QueryVariableType[] groupByKeyColumnsTypes;

    /**
     * Stores the {@link AccessPath} to the aggregation state variable.
     */
    private AccessPath aggregationStateVariable;

    /**
     * In case of a group-by aggregation, this variable stores the {@link KeyValueMapGenerator} used
     * for generating the map that contains the aggregation state.
     */
    private KeyValueMapGenerator aggregationMapGenerator;

    /**
     * An enum indicating the internal type of each aggregation function of this operator.
     */
    private enum AggregationFunction {
        // Non-group-by functions
        NG_COUNT,

        // Group-by functions
        G_COUNT,
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

        // Check pre-conditions
        if (this.getLogicalSubplan().getGroupSets().size() != 1)
            throw new UnsupportedOperationException(
                    "AggregationOperator: We expect exactly one GroupSet to exist in the logical plan");

        for (AggregateCall call : this.getLogicalSubplan().getAggCallList())
            if (call.isDistinct())
                throw new UnsupportedOperationException(
                        "AggregationOperator does not support DISTINCT keyword");

        // Classify all aggregation functions
        ImmutableBitSet groupBySet = this.getLogicalSubplan().getGroupSet();
        int numberOfGroupByKeys = groupBySet.cardinality();
        this.groupByAggregation = numberOfGroupByKeys > 0;
        if (this.groupByAggregation)
            this.groupByKeyColumnIndices = groupBySet.toArray();
        else
            this.groupByKeyColumnIndices = null;

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

                } else if (aggregationFunction instanceof SqlCountAggFunction) {
                    this.aggregationFunctions[i] = AggregationFunction.G_COUNT;
                    this.aggregationFunctionInputOrdinals[i] = new int[0];

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
                    newOrdinalMapping.add(i, this.aggregationStateVariable);
                }

                // No other possibilities
            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

            // Have the parent operator consume the result
            codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

        } else {
            // Expose the result of the group-by aggregation
            // Since there is a single aggregation state variable, we can simply iterate over its keys
            ScalarVariableAccessPath keyIterationIndexVariable =
                    new ScalarVariableAccessPath(cCtx.defineVariable("key_i"), P_INT);
            // [numberOfKeys] = [this.aggregationStateVariable.read()].numberOfRecords;
            Java.Rvalue numberOfKeys = new Java.FieldAccessExpression(
                    getLocation(),
                    ((MapAccessPath) this.aggregationStateVariable).read(),
                    "numberOfRecords"
            );

            // Now generate a while loop to iterate over the keys
            // for (int key_i = 0; i < [numberOfKeys]; key_i++) { [forLoopBody] }
            Java.Block forLoopBody = createBlock(getLocation());
            codeGenResult.add(
                    createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), keyIterationIndexVariable.getType()),
                                    keyIterationIndexVariable.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            lt(getLocation(), keyIterationIndexVariable.read(), numberOfKeys),
                            postIncrement(getLocation(), keyIterationIndexVariable.write()),
                            forLoopBody
                    )
            );

            // Generate the value exposure within the forLoopBody and update the ordinal mapping
            // Currently, this operator only supports exposing the result in a non-SIMD fashion.
            // TODO: consider SIMD-compatible result exposure when required.

            // Property: group-by aggregates first expose the group keys and then the aggregate results
            List<AccessPath> newOrdinalMapping = new ArrayList<>(this.groupByKeyColumnIndices.length + this.aggregationFunctions.length);
            int currentOrdinalIndex = 0;

            // Simply expose each record as a set of scalar variables
            // First expose the group-by key values
            // [groupKey_j] = [this.aggregationStateVariable.read()].[this.agregationMapGenerator.keyVariableNames[j]][key_i];
            for (int j = 0; j < this.groupByKeyColumnIndices.length; j++) {
                ScalarVariableAccessPath groupKeyAP = new ScalarVariableAccessPath(
                        cCtx.defineVariable("groupKey_" + j),
                        this.groupByKeyColumnsTypes[j]);

                forLoopBody.addStatement(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), groupKeyAP.getType()),
                                groupKeyAP.getVariableName(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        new Java.FieldAccessExpression(
                                                getLocation(),
                                                ((MapAccessPath) this.aggregationStateVariable).read(),
                                                this.aggregationMapGenerator.keyVariableNames[j]
                                        ),
                                        keyIterationIndexVariable.read()
                                )
                        )
                );
                newOrdinalMapping.add(currentOrdinalIndex++, groupKeyAP);
            }

            // Then expose the aggregate results per aggregation function
            int currentMapValueOrdinalIndex = 0;
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_COUNT || currentFunction == AggregationFunction.G_SUM) {
                    // Simply need to return the value stored for the current function in the map
                    // [valueType] aggregation_[i]_value =
                    //         [this.aggregationStateVariable.read()].values_ord_[currentMapValueOrdinalIndex][key_i]
                    ScalarVariableAccessPath aggregationValue = new ScalarVariableAccessPath(
                            cCtx.defineVariable("aggregation_" + i + "_value"),
                            this.aggregationMapGenerator.valueTypes[currentMapValueOrdinalIndex]
                    );
                    forLoopBody.addStatement(
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), aggregationValue.getType()),
                                    aggregationValue.getVariableName(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            new Java.FieldAccessExpression(
                                                    getLocation(),
                                                    ((MapAccessPath) this.aggregationStateVariable).read(),
                                                    this.aggregationMapGenerator.valueVariableNames[currentMapValueOrdinalIndex]
                                            ),
                                            keyIterationIndexVariable.read()
                                    )
                            )
                    );

                    newOrdinalMapping.add(currentOrdinalIndex++, aggregationValue);
                    currentMapValueOrdinalIndex++;
                }

                // No other possibilities due to constructor
            }

            // Have the parent operator consume the result
            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);
            forLoopBody.addStatements(this.nonVecParentConsume(cCtx, oCtx));
        }

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
                                            ((ScalarVariableAccessPath) this.aggregationStateVariable).write()
                                    ));

                        } else if (!useSIMD && firstOrdinalAP instanceof IndexedArrowVectorElementAccessPath iaveap) {
                            // For an indexed arrow vector element access path ("for loop over arrow vector elements")
                            // simply increment the relevant count variable
                            codeGenResult.add(
                                    postIncrementStm(
                                            getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariable).write()
                                    ));

                        } else if (useSIMD && firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
                            // For a count aggregation over a SIMD loop access path, add the number of true entries in the valid mask
                            codeGenResult.add(
                                    createVariableAdditionAssignmentStm(
                                            getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariable).write(),
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
            // Idea: first perform the hashing of the key-columns, then perform the hash map maintenance
            AccessPath[] keyColumnAccessPaths;
            ScalarVariableAccessPath keyColumnPreHashAccessPath;
            Java.Rvalue[] aggregationValues = new Java.Rvalue[this.aggregationFunctions.length];
            Java.Block hashMapMaintenanceTarget;

            // Hashing of key-columns depends on whether we are in SIMD mode or not
            if (!this.useSIMDNonVec(cCtx)) {
                // Ensure we have "local" access paths for the key column values
                Java.Rvalue[] keyColumnRValues = new Java.Rvalue[this.groupByKeyColumnIndices.length];
                keyColumnAccessPaths = new AccessPath[keyColumnRValues.length];
                for (int i = 0; i < keyColumnRValues.length; i++) {
                    keyColumnRValues[i] = getRValueFromAccessPathNonVec(cCtx, this.groupByKeyColumnIndices[i], codeGenResult);
                    keyColumnAccessPaths[i] = cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndices[i]);
                }

                // Now compute the pre-hash value in a local variable
                keyColumnPreHashAccessPath =
                        new ScalarVariableAccessPath(cCtx.defineVariable("group_key_pre_hash"), P_LONG);
                for (int i = 0; i < keyColumnRValues.length; i++) {

                    Java.AmbiguousName hashFunctionContainer = switch (this.groupByKeyColumnsTypes[i]) {
                        case P_INT -> createAmbiguousNameRef(getLocation(), "Int_Hash_Function");
                        case S_FL_BIN -> createAmbiguousNameRef(getLocation(), "Char_Arr_Hash_Function");

                        default -> throw new UnsupportedOperationException("AggregationOperator.consumeNonVec does not support this group-by key type");
                    };

                    Java.MethodInvocation currentPreHashInvocation = createMethodInvocation(
                            getLocation(),
                            hashFunctionContainer,
                            "preHash",
                            new Java.Rvalue[]{ keyColumnRValues[i] }
                    );

                    if (i == 0) {
                        // On the first key column, need to declare and initialise the variable
                        codeGenResult.add(
                                createLocalVariable(
                                        getLocation(),
                                        toJavaType(getLocation(), keyColumnPreHashAccessPath.getType()),
                                        keyColumnPreHashAccessPath.getVariableName(),
                                        currentPreHashInvocation
                                ));

                    } else {
                        // On all others, we "extend" the pre-hash using the XOR operator
                        codeGenResult.add(
                            createVariableXorAssignmentStm(
                                    getLocation(),
                                    keyColumnPreHashAccessPath.write(),
                                    currentPreHashInvocation
                            ));
                    }

                }

                // Also obtain the values to insert into the hash-map later on
                for (int i = 0; i < this.aggregationFunctions.length; i++) {
                    AggregationFunction currentFunction = this.aggregationFunctions[i];

                    if (currentFunction == AggregationFunction.G_COUNT) {
                        aggregationValues[i] = createIntegerLiteral(getLocation(), 1);
                    } else if (currentFunction == AggregationFunction.G_SUM) {
                        aggregationValues[i] = this.getRValueFromAccessPathNonVec(cCtx, this.aggregationFunctionInputOrdinals[i][0], codeGenResult);
                    }

                    // No other possibilities due to the constructor

                }

                // Set the correct hashMapMaintenanceTarget
                hashMapMaintenanceTarget = createBlock(getLocation());
                codeGenResult.add(hashMapMaintenanceTarget);

            } else { // this.useSIMDNonVec(cCtx)

                // TODO: extend to multiple key version later on
                if (this.groupByKeyColumnIndices.length > 1)
                    throw new UnsupportedOperationException("AggregationOperator.consumeNonVec only supports a single key column in SIMD operation");

                // Perform the SIMD-ed pre-hashing and flattening
                var preHashAndFlattenResult = Int_Hash_Function.preHashAndFlattenSIMD(
                        cCtx,
                        cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndices[0])
                );
                keyColumnAccessPaths = new AccessPath[] { preHashAndFlattenResult.keyColumnAccessPath };
                keyColumnPreHashAccessPath = preHashAndFlattenResult.keyColumnPreHashAccessPath;
                codeGenResult.addAll(preHashAndFlattenResult.generatedCode);

                // Obtain the values to insert into the hash-map later on
                for (int i = 0; i < this.aggregationFunctions.length; i++) {
                    AggregationFunction currentFunction = this.aggregationFunctions[i];

                    if (currentFunction == AggregationFunction.G_COUNT) {
                        aggregationValues[i] = createIntegerLiteral(getLocation(), 1);

                    } else if (currentFunction == AggregationFunction.G_SUM) {

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

            // Now perform hash-table maintenance by collecting the correct arguments for the
            // incrementForKey method of the hash-table based on the aggregation functions
            Java.Rvalue[] incrementForKeyArgs = new Java.Rvalue[keyColumnAccessPaths.length + aggregationValues.length + 1];
            int currentArgumentIndex = 0;
            for (int i = 0; i < keyColumnAccessPaths.length; i++)
                incrementForKeyArgs[currentArgumentIndex++] = ((ScalarVariableAccessPath) keyColumnAccessPaths[i]).read();  // Key
            incrementForKeyArgs[currentArgumentIndex++] = keyColumnPreHashAccessPath.read();                                // Prehash
            System.arraycopy(aggregationValues, 0, incrementForKeyArgs, currentArgumentIndex, aggregationValues.length); // Values to increment by

            hashMapMaintenanceTarget.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            ((MapAccessPath) this.aggregationStateVariable).read(),
                            "incrementForKey",
                            incrementForKeyArgs
                    )
            );

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
                    newOrdinalMapping.add(i, this.aggregationStateVariable);
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
            List<AccessPath> updatedOrdinalMapping = new ArrayList<>(this.groupByKeyColumnIndices.length + this.aggregationFunctions.length);
            int currentUpdatedOrdinalMappingIndex = 0;

            // Since there is only one group-by variable, we can simply use the order in its state variables over the different columns
            // as it is shared. We first allocate a variable to store the length of result vectors that are exposed to the parent operator.

            // int aggregationResultVectorLength;
            ScalarVariableAccessPath aggregationResultVectorLengthAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("aggregationResultVectorLength"),
                    P_INT
            );
            codeGenResult.add(createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, aggregationResultVectorLengthAP.getVariableName()));

            // Next, we need to allocate the correct key vector types
            ArrayVectorAccessPath[] groupKeyVectorsAPs = new ArrayVectorAccessPath[this.groupByKeyColumnIndices.length];
            for (int i = 0; i < groupKeyVectorsAPs.length; i++) {
                QueryVariableType currentKeyPrimitiveType = this.groupByKeyColumnsTypes[i];

                // primitiveType[] groupKeyVector_i = cCtx.getAllocationManager().get[primitiveType]Vector();
                ArrayAccessPath groupKeyVectorArrayAP = new ArrayAccessPath(
                        cCtx.defineVariable("groupKeyVector_" + i),
                        (currentKeyPrimitiveType == S_FL_BIN) ? S_A_FL_BIN : primitiveArrayTypeForPrimitive(currentKeyPrimitiveType));

                String initMethodName = switch (currentKeyPrimitiveType) {
                    case P_INT -> "getIntVector";
                    case S_FL_BIN -> "getNestedByteVector";

                    default -> throw new UnsupportedOperationException("AggregationOperator.produceVec does not support key type " + currentKeyPrimitiveType);
                };

                codeGenResult.add(createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), groupKeyVectorArrayAP.getType()),
                        groupKeyVectorArrayAP.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"),"getAllocationManager"),
                                initMethodName
                        )
                ));

                groupKeyVectorsAPs[i] = new ArrayVectorAccessPath(
                        groupKeyVectorArrayAP,
                        aggregationResultVectorLengthAP,
                        vectorTypeForPrimitiveArrayType(groupKeyVectorArrayAP.getType()));
                updatedOrdinalMapping.add(currentUpdatedOrdinalMappingIndex++, groupKeyVectorsAPs[i]);
            }

            // Allocate the vectors for the aggregation functions based on the aggregation function type
            ArrayVectorAccessPath[] aggregationResultAPs = new ArrayVectorAccessPath[this.aggregationFunctions.length];
            int currentMapValueOrdinalIndex = 0;
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                // Define the vector name as "aggregation_[currentFunction]_[i]_vector"
                String aggregationResultVectorName = cCtx.defineVariable("agg_" + currentFunction + "_" + i + "_vector");

                // Expose the result based on the function type
                if (currentFunction == AggregationFunction.G_COUNT ||currentFunction == AggregationFunction.G_SUM) {
                    // The vector type simply corresponds to the value type stored in the hash-map
                    QueryVariableType resultType
                            = primitiveArrayTypeForPrimitive(this.aggregationMapGenerator.valueTypes[currentMapValueOrdinalIndex]);
                    ArrayAccessPath aggregationResultVectorAP = new ArrayAccessPath(aggregationResultVectorName, resultType);

                    String allocationManagerMethodName = switch (resultType) {
                        case P_A_DOUBLE -> "getDoubleVector";
                        case P_A_INT -> "getIntVector";
                        case P_A_LONG -> "getLongVector";

                        default -> throw new UnsupportedOperationException(
                                "AggregationOperator.produceVec cannot allocate a vector for the aggregation result of type " + resultType);
                    };

                    codeGenResult.add(createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), aggregationResultVectorAP.getType()),
                            aggregationResultVectorAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createMethodInvocation(getLocation(), createAmbiguousNameRef(getLocation(), "cCtx"), "getAllocationManager"),
                                    allocationManagerMethodName
                            )
                    ));

                    aggregationResultAPs[i] = new ArrayVectorAccessPath(
                            aggregationResultVectorAP,
                            aggregationResultVectorLengthAP,
                            vectorTypeForPrimitiveArrayType(resultType));
                    currentMapValueOrdinalIndex++;

                }

                updatedOrdinalMapping.add(currentUpdatedOrdinalMappingIndex++, aggregationResultAPs[i]);
            }

            // Set the updated ordinal mapping
            cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

            // Construct and expose the actual vectors
            // Construct the key and value vectors by iterating over the keys
            // int current_key_offset = 0;
            // int number_of_records = [this.aggregationStateVariable).read()].numberOfRecords;
            // while (current_key_offset < number_of_records) {
            //     $ for first key vector $
            //     [aggregationResultVectorLength] = VectorisedAggregationOperators.constructVector(
            //             [groupKeyVector_0], [this.aggregationStateVariable.read()].keys_0, number_of_records, current_key_offset);
            //     $ for each remaining key vector $
            //         VectorisedAggregationOperators.constructVector(
            //             [groupKeyVector_i], [this.aggregationStateVariable.read()].keys_[keyIndex], number_of_records, current_key_offset);
            //     $ for each aggregationResult $
            //         VectorisedAggregationOperators.constructVector(
            //             [aggCall_$i$_vector], [this.aggregationStateVariable.read()].values_ord_[currentMapValueOrdinalIndex], number_of_records, current_key_offset);
            //     current_key_offset += [aggregationResultVectorLength];
            //     [whileLoopBody]
            // }
            ScalarVariableAccessPath currentKeyOffsetVariable =
                    new ScalarVariableAccessPath(cCtx.defineVariable("current_key_offset"), P_INT);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), currentKeyOffsetVariable.getType()),
                            currentKeyOffsetVariable.getVariableName(),
                            createIntegerLiteral(getLocation(), 0)
                    )
            );

            ScalarVariableAccessPath numberOfRecordsAP =
                    new ScalarVariableAccessPath(cCtx.defineVariable("number_of_records"), P_INT);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), numberOfRecordsAP.getType()),
                            numberOfRecordsAP.getVariableName(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    ((MapAccessPath) this.aggregationStateVariable).read(),
                                    "numberOfRecords"
                            )
                    )
            );

            Java.Block whileLoopBody = createBlock(getLocation());
            codeGenResult.add(
                    createWhileLoop(
                            getLocation(),
                            lt(getLocation(), currentKeyOffsetVariable.read(), numberOfRecordsAP.read()),
                            whileLoopBody
                    )
            );

            // Construct the key vectors
            for (int i = 0; i < this.groupByKeyColumnIndices.length; i++) {
                Java.MethodInvocation vectorConstructionMethodInvocation = createMethodInvocation(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "VectorisedAggregationOperators"),
                        "constructVector",
                        new Java.Rvalue[] {
                                groupKeyVectorsAPs[i].getVectorVariable().read(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        ((MapAccessPath) this.aggregationStateVariable).read(),
                                        this.aggregationMapGenerator.keyVariableNames[i]
                                ),
                                numberOfRecordsAP.read(),
                                currentKeyOffsetVariable.read()
                        }
                );

                if (i == 0) {
                    // On the first key vector, we need to initialise the aggregation result vector length
                    whileLoopBody.addStatement(
                            createVariableAssignmentStm(
                                    getLocation(),
                                    aggregationResultVectorLengthAP.write(),
                                    vectorConstructionMethodInvocation
                            )
                    );

                } else {
                    // On the remaining key vectors, we only care that the method is invoked
                    try {
                        whileLoopBody.addStatement(
                                new Java.ExpressionStatement(vectorConstructionMethodInvocation));

                    } catch (CompileException e) {
                        throw new RuntimeException(
                                "AggregationOperator.produceVec: exception occurred while wrapping method invocation in statement", e);
                    }
                }
            }

            // Create each value vector based on the aggregation type
            currentMapValueOrdinalIndex = 0;
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_COUNT || currentFunction == AggregationFunction.G_SUM) {
                    // The value vector can simply be constructed by obtaining values from the aggregation state map
                    whileLoopBody.addStatement(
                            createMethodInvocationStm(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "VectorisedAggregationOperators"),
                                    "constructVector",
                                    new Java.Rvalue[] {
                                            aggregationResultAPs[i].getVectorVariable().read(),
                                            new Java.FieldAccessExpression(
                                                    getLocation(),
                                                    ((MapAccessPath) this.aggregationStateVariable).read(),
                                                    this.aggregationMapGenerator.valueVariableNames[currentMapValueOrdinalIndex]
                                            ),
                                            numberOfRecordsAP.read(),
                                            currentKeyOffsetVariable.read()
                                    }
                            )
                    );
                    currentMapValueOrdinalIndex++;

                }

                // No other possibilities due to constructor
            }

            // Store the next key offset
            whileLoopBody.addStatement(
                    createVariableAdditionAssignmentStm(
                            getLocation(),
                            currentKeyOffsetVariable.write(),
                            aggregationResultVectorLengthAP.read()
                    )
            );

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
                                        ((ScalarVariableAccessPath) this.aggregationStateVariable).write(),
                                        countIncrementRValue
                                )
                        );

                    }

                    // No other cases possible due to constructor
                }
            }
        } else { // this.groupByAggregation
            // Idea: first perform the hashing of the key-columns, then perform the hash-table maintenance
            AccessPath[] keyColumnsAccessPaths = new AccessPath[this.groupByKeyColumnIndices.length];
            QueryVariableType[] keyColumnsAccessPathTypes = new QueryVariableType[keyColumnsAccessPaths.length];
            for (int i = 0; i < keyColumnsAccessPaths.length; i++) {
                keyColumnsAccessPaths[i] = cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndices[i]);
                keyColumnsAccessPathTypes[i] = keyColumnsAccessPaths[i].getType();
            }

            boolean accountForFiltering = keyColumnsAccessPaths[0] instanceof ArrowVectorWithSelectionVectorAccessPath
                    || keyColumnsAccessPaths[0] instanceof ArrowVectorWithValidityMaskAccessPath;
            for (int i = 0; i < keyColumnsAccessPaths.length; i++) {
                // Hashing of the key-column depends on whether we are in SIMD mode or not
                String methodInvocationName = "constructPreHashKeyVector";
                if (this.useSIMDVec())
                    methodInvocationName += "SIMD";

                // Determine the arguments
                Java.Rvalue[] methodInvocationArguments = new Java.Rvalue[3 + (accountForFiltering ? 2 : 0)];
                int currentMethodInvocationArgumentIndex = 0;
                methodInvocationArguments[currentMethodInvocationArgumentIndex++] = this.groupKeyPreHashVector.read();
                methodInvocationArguments[currentMethodInvocationArgumentIndex++] = switch (keyColumnsAccessPathTypes[i]) {
                    case ARROW_FIXED_LENGTH_BINARY_VECTOR, ARROW_INT_VECTOR
                            -> ((ArrowVectorAccessPath) keyColumnsAccessPaths[i]).read();
                    case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR
                            -> ((ArrowVectorWithSelectionVectorAccessPath) keyColumnsAccessPaths[i]).readArrowVector();
                    case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK, ARROW_INT_VECTOR_W_VALIDITY_MASK
                            -> ((ArrowVectorWithValidityMaskAccessPath) keyColumnsAccessPaths[i]).readArrowVector();

                    default -> throw new UnsupportedOperationException("AggregationOperator.consumeVec does not support this key column access path type");
                };
                if (accountForFiltering) {
                    methodInvocationArguments[currentMethodInvocationArgumentIndex++] = switch (keyColumnsAccessPathTypes[i]) {
                        case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR
                                -> ((ArrowVectorWithSelectionVectorAccessPath) keyColumnsAccessPaths[i]).readSelectionVector();
                        case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK, ARROW_INT_VECTOR_W_VALIDITY_MASK
                                -> ((ArrowVectorWithValidityMaskAccessPath) keyColumnsAccessPaths[i]).readValidityMask();

                        default -> throw new UnsupportedOperationException("AggregationOperator.consumeVec does not support this filtering key column access path type");
                    };
                    methodInvocationArguments[currentMethodInvocationArgumentIndex++] = switch (keyColumnsAccessPathTypes[i]) {
                        case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR
                                -> ((ArrowVectorWithSelectionVectorAccessPath) keyColumnsAccessPaths[i]).readSelectionVectorLength();
                        case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK, ARROW_INT_VECTOR_W_VALIDITY_MASK
                                -> ((ArrowVectorWithValidityMaskAccessPath) keyColumnsAccessPaths[i]).readValidityMaskLength();

                        default -> throw new UnsupportedOperationException("AggregationOperator.consumeVec does not support this filtering key column access path type");
                    };
                }
                methodInvocationArguments[currentMethodInvocationArgumentIndex++] =
                        new Java.BooleanLiteral(getLocation(), (i == 0) ? "false" : "true");

                // Perform the actual hashing
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                methodInvocationName,
                                methodInvocationArguments
                        )
                );

            }

            // Flatten the vectorisation to perform the combined hash-table maintenance
            // First perform the general part of the flattening
            List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();

            // int recordCount = [AP specific code];
            // for (int aviv = 0; aviv < recordCount; aviv++) { [tableMaintenanceLoopBody] }
            ScalarVariableAccessPath recordCount =
                    new ScalarVariableAccessPath(cCtx.defineVariable("recordCount"), P_INT);
            Java.Rvalue recordCountInitValue;

            Java.Block tableMaintenanceLoopBody = new Java.Block(getLocation());
            ScalarVariableAccessPath aviv =
                    new ScalarVariableAccessPath(cCtx.defineVariable("aviv"), P_INT);
            ScalarVariableAccessPath recordIndexAP = aviv; // Variable to allow record accessing via a selection vector

            Java.Rvalue[] incrementForKeyArguments =
                    new Java.Rvalue[this.groupByKeyColumnIndices.length + 1 + this.aggregationMapGenerator.valueVariableNames.length];
            int currentArgumentIndex = 0;

            // Initialise the record count, perform filtering if necessary and update record indexing AP if needed
            if (currentOrdinalMapping.get(0) instanceof ArrowVectorAccessPath avap) {
                // int recordCount = [avap.read()].getValueCount();
                recordCountInitValue = createMethodInvocation(getLocation(), avap.read(), "getValueCount");
            } else if (currentOrdinalMapping.get(0) instanceof ArrayVectorAccessPath avap) {
                recordCountInitValue = avap.getVectorLengthVariable().read();
            } else if (currentOrdinalMapping.get(0) instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                recordCountInitValue = avwvmap.readValidityMaskLength();

                // In the table maintenance loop, skip records whose validity indicator are set to false
                tableMaintenanceLoopBody.addStatement(
                        createIfNotContinue(
                                getLocation(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        avwvmap.readValidityMask(),
                                        aviv.read()
                                )
                        )
                );

            } else if (currentOrdinalMapping.get(0) instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                recordCountInitValue = avwsvap.readSelectionVectorLength();

                // In the table maintenance loop, index the records via the selection vector
                // int aviv_record_index = [avwsvap.readSelectionVector()][aviv];
                recordIndexAP = new ScalarVariableAccessPath(cCtx.defineVariable("aviv_record_index"), P_INT);
                tableMaintenanceLoopBody.addStatement(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), recordIndexAP.getType()),
                                recordIndexAP.getVariableName(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        avwsvap.readSelectionVector(),
                                        aviv.read()
                                )
                        )
                );

            } else {
                throw new UnsupportedOperationException(
                        "AggregationOperator.ConsumeVec does not support hash-table maintenance using the provided access path");
            }

            // Obtain the key arguments
            for (int i = 0; i < this.groupByKeyColumnIndices.length; i++) {
                AccessPath currentKeyOrdinal = currentOrdinalMapping.get(this.groupByKeyColumnIndices[i]);
                if (currentKeyOrdinal instanceof ArrowVectorAccessPath avap) {
                    incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                            getLocation(),
                            avap.read(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );
                } else if (currentKeyOrdinal instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                    incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                            getLocation(),
                            avwsvap.readArrowVector(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );
                } else if (currentKeyOrdinal instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                    incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                            getLocation(),
                            avwvmap.readArrowVector(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );
                } else if (currentKeyOrdinal instanceof ArrayVectorAccessPath avap) {
                    incrementForKeyArguments[currentArgumentIndex++] = createArrayElementAccessExpr(
                            getLocation(),
                            avap.getVectorVariable().read(),
                            recordIndexAP.read()
                    );
                }
            }

            // Obtain the pre-hash argument
            incrementForKeyArguments[currentArgumentIndex++] = createArrayElementAccessExpr(
                    getLocation(),
                    this.groupKeyPreHashVector.read(),
                    recordIndexAP.read()
            );

            // Now get the value to insert based on the aggregation functions
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_COUNT) {
                    incrementForKeyArguments[currentArgumentIndex++] = createIntegerLiteral(getLocation(), 1);

                } else if (currentFunction == AggregationFunction.G_SUM) {
                    // We simply need to maintain the sum based on the input ordinal type
                    AccessPath inputOrdinal = cCtx.getCurrentOrdinalMapping().get(this.aggregationFunctionInputOrdinals[i][0]);
                    QueryVariableType inputOrdinalType = inputOrdinal.getType();

                    if (inputOrdinalType == ARROW_DOUBLE_VECTOR || inputOrdinalType == ARROW_INT_VECTOR) {
                        ArrowVectorAccessPath castInputOrdinal = (ArrowVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                                getLocation(),
                                castInputOrdinal.read(),
                                "get",
                                new Java.Rvalue[] { recordIndexAP.read() }
                        );

                    } else if (inputOrdinalType == ARRAY_DOUBLE_VECTOR || inputOrdinalType == ARRAY_INT_VECTOR) {
                        ArrayVectorAccessPath castInputOrdinal = (ArrayVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createArrayElementAccessExpr(
                                getLocation(),
                                castInputOrdinal.getVectorVariable().read(),
                                recordIndexAP.read()
                        );

                    } else if (inputOrdinalType == ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR || inputOrdinalType == ARROW_INT_VECTOR_W_SELECTION_VECTOR) {
                        ArrowVectorWithSelectionVectorAccessPath castInputOrdinal = (ArrowVectorWithSelectionVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                                getLocation(),
                                castInputOrdinal.readArrowVector(),
                                "get",
                                new Java.Rvalue[] { recordIndexAP.read() }
                        );


                    }  else if (inputOrdinalType == ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK || inputOrdinalType == ARROW_INT_VECTOR_W_VALIDITY_MASK) {
                        ArrowVectorWithValidityMaskAccessPath castInputOrdinal = (ArrowVectorWithValidityMaskAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                                getLocation(),
                                castInputOrdinal.readArrowVector(),
                                "get",
                                new Java.Rvalue[] { recordIndexAP.read() }
                        );

                    } else if (inputOrdinalType == ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR) {
                        ArrayVectorWithSelectionVectorAccessPath castInputOrdinal = (ArrayVectorWithSelectionVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createArrayElementAccessExpr(
                                getLocation(),
                                castInputOrdinal.getArrayVectorVariable().getVectorVariable().read(),
                                recordIndexAP.read()
                        );

                    } else if (inputOrdinalType == ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK) {
                        ArrayVectorWithValidityMaskAccessPath castInputOrdinal = (ArrayVectorWithValidityMaskAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createArrayElementAccessExpr(
                                getLocation(),
                                castInputOrdinal.getArrayVectorVariable().getVectorVariable().read(),
                                recordIndexAP.read()
                        );

                    } else {
                        throw new UnsupportedOperationException(
                                "AggregationOperator.consumeVec does not support this input ordinal type for the group-by SUM aggregation: " + inputOrdinal.getType());
                    }

                }

                // No other possibilities due to constructor
            }

            // Add the actual record count variable
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), recordCount.getType()),
                            recordCount.getVariableName(),
                            recordCountInitValue
                    )
            );

            // Add the actual hash-table maintenance loop
            codeGenResult.add(
                    createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), aviv.getType()),
                                    aviv.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            lt(getLocation(), aviv.read(), recordCount.read()),
                            postIncrement(getLocation(), aviv.write()),
                            tableMaintenanceLoopBody
                    )
            );

            // Now perform the actual hash-table maintenance by invoking the incrementForKey method
            tableMaintenanceLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            ((MapAccessPath) this.aggregationStateVariable).read(),
                            "incrementForKey",
                            incrementForKeyArguments
                    )
            );

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

        // Declare the aggregation state variable if we have a non-group-by count
        if (!this.groupByAggregation) {
            if (this.aggregationFunctions[0] == AggregationFunction.NG_COUNT) {
                this.aggregationStateVariable = new ScalarVariableAccessPath(
                        cCtx.defineVariable("count"),
                        P_LONG
                );
            }

            return;
        }

        // Otherwise, we have a group-by aggregation
        // Obtain the type of the group-by columns
        this.groupByKeyColumnsTypes = new QueryVariableType[this.groupByKeyColumnIndices.length];
        for (int i = 0; i < this.groupByKeyColumnsTypes.length; i++) {
            QueryVariableType ordinalType = om.get(this.groupByKeyColumnIndices[i]).getType();
            if (ordinalType == S_FL_BIN
                    || ordinalType == ARROW_FIXED_LENGTH_BINARY_VECTOR
                    || ordinalType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR
                    || ordinalType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK)
                ordinalType = S_FL_BIN;
            else
                ordinalType = primitiveType(ordinalType);
            this.groupByKeyColumnsTypes[i] = ordinalType;
        }

        // Initialise the map generator
        List<QueryVariableType> mapValueTypes = new ArrayList<>();
        for (int i = 0; i < this.aggregationFunctions.length; i++) {
            AggregationFunction currentFunction = this.aggregationFunctions[i];

            if (currentFunction == AggregationFunction.G_COUNT) {
                mapValueTypes.add(P_INT);

            } else if (currentFunction == AggregationFunction.G_SUM) {
                QueryVariableType valueType = primitiveType(om.get(this.aggregationFunctionInputOrdinals[i][0]).getType());

                if (valueType == P_INT) {
                    // We need to upgrade the value to long
                    mapValueTypes.add(P_LONG);

                } else if (valueType == P_DOUBLE) {
                    mapValueTypes.add(P_DOUBLE);

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator.declareAggregationState: group-by sum aggregation does not support key-value type combination: "
                                    + Arrays.toString(this.groupByKeyColumnsTypes) + "-" + valueType);
                }
            }

        }

        QueryVariableType[] mapValueTypesArray = new QueryVariableType[mapValueTypes.size()];
        mapValueTypes.toArray(mapValueTypesArray);
        this.aggregationMapGenerator = new KeyValueMapGenerator(
                this.groupByKeyColumnsTypes,
                mapValueTypesArray
        );

        // And finally declare the aggregation map
        this.aggregationStateVariable = new MapAccessPath(
                cCtx.defineVariable("aggregation_state_map"),
                MAP_GENERATED
        );
    }

    /**
     * Method to initialise the aggregation state variables per aggregation operator as stored in
     * {@code this.aggregationStateMappings}.
     */
    private List<Java.Statement> initialiseAggregationStates() {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Allocate all required state variable as local variables
        if (!this.groupByAggregation) {
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), this.aggregationStateVariable.getType()),
                            ((ScalarVariableAccessPath) this.aggregationStateVariable).getVariableName(),
                            createIntegerLiteral(getLocation(), 0)
                    )
            );
        } else {
            codeGenResult.add(
                    new Java.LocalClassDeclarationStatement(
                            this.aggregationMapGenerator.generate()
                    )
            );
            Java.Type generatedMapType = createReferenceType(
                    getLocation(),
                    this.aggregationMapGenerator.generate().getName());

            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            generatedMapType,
                            ((MapAccessPath) this.aggregationStateVariable).getVariableName(),
                            createClassInstance(
                                    getLocation(),
                                    generatedMapType
                            )
                    )
            );
        }

        return codeGenResult;
    }

}
