package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.IndexedArrowVectorElementAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoControlGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;
import AethraDB.evaluation.general_support.hashmaps.KeyValueMapGenerator;
import AethraDB.util.language.AethraExpression;
import AethraDB.util.language.function.AethraFunction;
import AethraDB.util.language.function.aggregation.AethraCountAggregation;
import AethraDB.util.language.function.aggregation.AethraSumAggregation;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_FIXED_LENGTH_BINARY_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_INT_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_VARCHAR_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_VALIDITY_MASK;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.MAP_GENERATED;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_DOUBLE;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_A_FL_BIN;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_A_VARCHAR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_FL_BIN;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_VARCHAR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createBlock;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableXorAssignmentStm;

/**
 * A {@link CodeGenOperator} which computes some aggregation function over the records.
 */
public class AggregationOperator extends CodeGenOperator {

    /**
     * The {@link CodeGenOperator} producing the records to be aggregated by {@code this}.
     */
    private final CodeGenOperator child;

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
     * Stores the {@link AccessPath} to the aggregation state variable(s).
     * For a group-by aggregation, there will be 1 aggregation state variable, while there will be
     * one aggregation state variable per aggregation function for a non-group-by aggregation.
     */
    private AccessPath[] aggregationStateVariables;

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
        NG_SUM,

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
     * @param child The {@link CodeGenOperator} producing the records to be aggregated.
     * @param groupByAggregation Whether this aggregation is a grouping aggregation.
     * @param groupByColumnIndices The columns to group the aggregations by if {@code groupByAggregation == true}.
     * @param aggregationExpressions The expressions specifying the aggregation to be performed.
     */
    public AggregationOperator(
            CodeGenOperator child,
            boolean groupByAggregation,
            int[] groupByColumnIndices,
            AethraExpression[] aggregationExpressions
    ) {
        this.child = child;
        this.child.setParent(this);

        // Pre-conditions checked by planner library
        // Classify all aggregation functions
        this.groupByAggregation = groupByAggregation;
        this.groupByKeyColumnIndices = groupByColumnIndices;

        this.aggregationFunctions = new AggregationFunction[aggregationExpressions.length];
        this.aggregationFunctionInputOrdinals = new int[this.aggregationFunctions.length][];
        for (int i = 0; i < this.aggregationFunctions.length; i++) {
            AethraExpression function = aggregationExpressions[i];
            if (!(function instanceof AethraFunction aggregationFunction))
                throw new IllegalStateException("AggregationOperator should only receive AethraFunction aggregation expressions");

            if (!this.groupByAggregation) { // Simple non-group-by aggregations
                if (aggregationFunction instanceof AethraCountAggregation) {
                    this.aggregationFunctions[i] = AggregationFunction.NG_COUNT;
                    this.aggregationFunctionInputOrdinals[i] = new int[0];

                } else if (aggregationFunction instanceof AethraSumAggregation asa) {
                    this.aggregationFunctions[i] = AggregationFunction.NG_SUM;
                    this.aggregationFunctionInputOrdinals[i] = new int[] { asa.summand.columnIndex };

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator does not support this non-group-by aggregation function " + aggregationFunction.getKind());
                }

            } else { // Group-by aggregations
                if (aggregationFunction instanceof AethraSumAggregation asa) {
                    this.aggregationFunctions[i] = AggregationFunction.G_SUM;
                    this.aggregationFunctionInputOrdinals[i] = new int[] { asa.summand.columnIndex };

                } else if (aggregationFunction instanceof AethraCountAggregation) {
                    this.aggregationFunctions[i] = AggregationFunction.G_COUNT;
                    this.aggregationFunctionInputOrdinals[i] = new int[0];

                } else {
                    throw new UnsupportedOperationException(
                            "AggregationOperator does not support this group-by aggregation function " + aggregationFunction.getKind());
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

                if (currentFunction == AggregationFunction.NG_COUNT
                        || currentFunction == AggregationFunction.NG_SUM) {
                    // Simply set the current ordinal to refer to the correct state variable
                    newOrdinalMapping.add(i, this.aggregationStateVariables[i]);
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
                    JaninoGeneralGen.getLocation(),
                    ((MapAccessPath) this.aggregationStateVariables[0]).read(),
                    "numberOfRecords"
            );

            // Now generate a while loop to iterate over the keys
            // for (int key_i = 0; i < [numberOfKeys]; key_i++) { [forLoopBody] }
            Java.Block forLoopBody = createBlock(JaninoGeneralGen.getLocation());
            codeGenResult.add(
                    JaninoControlGen.createForLoop(
                            JaninoGeneralGen.getLocation(),
                            createLocalVariable(
                                    JaninoGeneralGen.getLocation(),
                                    toJavaType(JaninoGeneralGen.getLocation(), keyIterationIndexVariable.getType()),
                                    keyIterationIndexVariable.getVariableName(),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                            ),
                            JaninoOperatorGen.lt(JaninoGeneralGen.getLocation(), keyIterationIndexVariable.read(), numberOfKeys),
                            JaninoOperatorGen.postIncrement(JaninoGeneralGen.getLocation(), keyIterationIndexVariable.write()),
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
                                JaninoGeneralGen.getLocation(),
                                toJavaType(JaninoGeneralGen.getLocation(), groupKeyAP.getType()),
                                groupKeyAP.getVariableName(),
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
                                        new Java.FieldAccessExpression(
                                                JaninoGeneralGen.getLocation(),
                                                ((MapAccessPath) this.aggregationStateVariables[0]).read(),
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
                                    JaninoGeneralGen.getLocation(),
                                    toJavaType(JaninoGeneralGen.getLocation(), aggregationValue.getType()),
                                    aggregationValue.getVariableName(),
                                    JaninoGeneralGen.createArrayElementAccessExpr(
                                            JaninoGeneralGen.getLocation(),
                                            new Java.FieldAccessExpression(
                                                    JaninoGeneralGen.getLocation(),
                                                    ((MapAccessPath) this.aggregationStateVariables[0]).read(),
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
                                    JaninoOperatorGen.postIncrementStm(
                                            JaninoGeneralGen.getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write()
                                    ));

                        } else if (!useSIMD && firstOrdinalAP instanceof IndexedArrowVectorElementAccessPath iaveap) {
                            // For an indexed arrow vector element access path ("for loop over arrow vector elements")
                            // simply increment the relevant count variable
                            codeGenResult.add(
                                    JaninoOperatorGen.postIncrementStm(
                                            JaninoGeneralGen.getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write()
                                    ));

                        } else if (useSIMD && firstOrdinalAP instanceof SIMDLoopAccessPath slap) {
                            // For a count aggregation over a SIMD loop access path, add the number of true entries in the valid mask
                            codeGenResult.add(
                                    createVariableAdditionAssignmentStm(
                                            JaninoGeneralGen.getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write(),
                                            createMethodInvocation(
                                                    JaninoGeneralGen.getLocation(),
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

                    case NG_SUM -> {
                        // Check the type of the input ordinal to be able to generate the correct count update statements
                        int inputOrdinal = this.aggregationFunctionInputOrdinals[i][0];
                        AccessPath inputOrdinalAP = cCtx.getCurrentOrdinalMapping().get(inputOrdinal);
                        if (!useSIMD && inputOrdinalAP instanceof ScalarVariableAccessPath svap) {
                            codeGenResult.add(
                                    createVariableAdditionAssignmentStm(
                                            JaninoGeneralGen.getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write(),
                                            svap.read()
                                    ));

                        } else if (!useSIMD && inputOrdinalAP instanceof IndexedArrowVectorElementAccessPath iaveap) {
                            codeGenResult.add(
                                    createVariableAdditionAssignmentStm(
                                            JaninoGeneralGen.getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write(),
                                            iaveap.readGeneric() // Read generic is applicable since this case will only occur for numeric columns
                                    ));

                        } else if (useSIMD && inputOrdinalAP instanceof SIMDLoopAccessPath slap) {
                            // Need to perform a SIMD reduce lane operation
                            // [SIMDVectorType] [SIMDVector] = IntVector.fromSegment(
                            //      [lhsAP.readVectorSpecies()],
                            //      [lhsAP.readMemorySegment()],
                            //      [lhsAP.readArrowVectorOffset()] * [lhsAP.readArrowVector().TYPE_WIDTH],
                            //      java.nio.ByteOrder.LITTLE_ENDIAN,
                            //      [lhsAP.readSIMDMask()]
                            // );
                            String SIMDVectorName = cCtx.defineVariable("SIMDVector");
                            String vectorCreationMethodName = switch (slap.getType()) {
                                case VECTOR_DOUBLE_MASKED -> "createDoubleVector";
                                case VECTOR_INT_MASKED -> "createIntVector";
                                default -> throw new UnsupportedOperationException(
                                        "AggregationOperator.consumeNonVec does not support the provided SIMD-Loop AP type " + slap.getType());
                            };

                            codeGenResult.add(
                                    createLocalVariable(
                                            JaninoGeneralGen.getLocation(),
                                            toJavaType(JaninoGeneralGen.getLocation(), slap.getType()),
                                            SIMDVectorName,
                                            createMethodInvocation(
                                                    JaninoGeneralGen.getLocation(),
                                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "oCtx"),
                                                    vectorCreationMethodName,
                                                    new Java.Rvalue[] {
                                                            slap.readVectorSpecies(),
                                                            slap.readMemorySegment(),
                                                            JaninoOperatorGen.mul(
                                                                    JaninoGeneralGen.getLocation(),
                                                                    slap.readArrowVectorOffset(),
                                                                    JaninoGeneralGen.createAmbiguousNameRef(
                                                                            JaninoGeneralGen.getLocation(),
                                                                            slap.getArrowVectorAccessPath().getVariableName() + ".TYPE_WIDTH"
                                                                    )
                                                            ),
                                                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "java.nio.ByteOrder.LITTLE_ENDIAN"),
                                                            slap.readSIMDMask()
                                                    }
                                            )
                                    )
                            );

                            // Now reduce the vector by summing all entries
                            // this.aggregationStateVariables[i] += [SIMDVector].reduceLanes(ADD, slap.readSIMDMask());
                            codeGenResult.add(
                                    createVariableAdditionAssignmentStm(
                                            JaninoGeneralGen.getLocation(),
                                            ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write(),
                                            createMethodInvocation(
                                                    JaninoGeneralGen.getLocation(),
                                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), SIMDVectorName),
                                                    "reduceLanes",
                                                    new Java.Rvalue[] {
                                                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "jdk.incubator.vector.VectorOperators.ADD"),
                                                            slap.readSIMDMask()
                                                    }
                                            )
                                    )
                            );

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.consumeNonVec does not support this AccessPath for the SUM aggregation while "
                                            + (useSIMD ? "" : "not ") + "using SIMD: "  + inputOrdinalAP);
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
                    keyColumnRValues[i] = getRValueFromOrdinalAccessPathNonVec(cCtx, this.groupByKeyColumnIndices[i], codeGenResult);
                    keyColumnAccessPaths[i] = cCtx.getCurrentOrdinalMapping().get(this.groupByKeyColumnIndices[i]);
                }

                // Now compute the pre-hash value in a local variable
                keyColumnPreHashAccessPath =
                        new ScalarVariableAccessPath(cCtx.defineVariable("group_key_pre_hash"), P_LONG);
                for (int i = 0; i < keyColumnRValues.length; i++) {

                    Java.AmbiguousName hashFunctionContainer = switch (this.groupByKeyColumnsTypes[i]) {
                        case P_DOUBLE -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Double_Hash_Function");
                        case P_INT, P_INT_DATE -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Int_Hash_Function");
                        case S_FL_BIN, S_VARCHAR -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Char_Arr_Hash_Function");

                        default -> throw new UnsupportedOperationException("AggregationOperator.consumeNonVec does not support this group-by key type");
                    };

                    Java.MethodInvocation currentPreHashInvocation = createMethodInvocation(
                            JaninoGeneralGen.getLocation(),
                            hashFunctionContainer,
                            "preHash",
                            new Java.Rvalue[]{ keyColumnRValues[i] }
                    );

                    if (i == 0) {
                        // On the first key column, need to declare and initialise the variable
                        codeGenResult.add(
                                createLocalVariable(
                                        JaninoGeneralGen.getLocation(),
                                        toJavaType(JaninoGeneralGen.getLocation(), keyColumnPreHashAccessPath.getType()),
                                        keyColumnPreHashAccessPath.getVariableName(),
                                        currentPreHashInvocation
                                ));

                    } else {
                        // On all others, we "extend" the pre-hash using the XOR operator
                        codeGenResult.add(
                            createVariableXorAssignmentStm(
                                    JaninoGeneralGen.getLocation(),
                                    keyColumnPreHashAccessPath.write(),
                                    currentPreHashInvocation
                            ));
                    }

                }

                // Also obtain the values to insert into the hash-map later on
                for (int i = 0; i < this.aggregationFunctions.length; i++) {
                    AggregationFunction currentFunction = this.aggregationFunctions[i];

                    if (currentFunction == AggregationFunction.G_COUNT) {
                        aggregationValues[i] = JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1);
                    } else if (currentFunction == AggregationFunction.G_SUM) {
                        aggregationValues[i] = this.getRValueFromOrdinalAccessPathNonVec(cCtx, this.aggregationFunctionInputOrdinals[i][0], codeGenResult);
                    }

                    // No other possibilities due to the constructor

                }

                // Set the correct hashMapMaintenanceTarget
                hashMapMaintenanceTarget = createBlock(JaninoGeneralGen.getLocation());
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
                        aggregationValues[i] = JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1);

                    } else if (currentFunction == AggregationFunction.G_SUM) {

                        AccessPath valueAP = cCtx.getCurrentOrdinalMapping().get(this.aggregationFunctionInputOrdinals[i][0]);
                        if (valueAP instanceof SIMDLoopAccessPath valueAP_slap) {
                            // aggregationValues[i] = [valueAP_slap.readArrowVector()].get([valueAP_slap.readArrowVectorOffset()] + [simd_vector_i]);
                            aggregationValues[i] = createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    valueAP_slap.readArrowVector(),
                                    "get",
                                    new Java.Rvalue[]{
                                            JaninoOperatorGen.plus(
                                                    JaninoGeneralGen.getLocation(),
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
                            JaninoGeneralGen.getLocation(),
                            ((MapAccessPath) this.aggregationStateVariables[0]).read(),
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
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), this.groupKeyPreHashVector.getType()),
                            this.groupKeyPreHashVector.getVariableName(),
                            createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    createMethodInvocation(JaninoGeneralGen.getLocation(), JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"),"getAllocationManager"),
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

                if (currentFunction == AggregationFunction.NG_COUNT
                        || currentFunction == AggregationFunction.NG_SUM) {
                    // Simply set the current ordinal to refer to the relevant aggregation variable
                    newOrdinalMapping.add(i, this.aggregationStateVariables[i]);
                }

                // No other possibilities
            }

            cCtx.setCurrentOrdinalMapping(newOrdinalMapping);

            // Have the parent operator consume the result
            // We do a nonVec consume here, since the result consists only of scalars
            codeGenResult.addAll(this.nonVecParentConsume(cCtx, oCtx));

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
            codeGenResult.add(createPrimitiveLocalVar(JaninoGeneralGen.getLocation(), Java.Primitive.INT, aggregationResultVectorLengthAP.getVariableName()));

            // Next, we need to allocate the correct key vector types
            ArrayVectorAccessPath[] groupKeyVectorsAPs = new ArrayVectorAccessPath[this.groupByKeyColumnIndices.length];
            for (int i = 0; i < groupKeyVectorsAPs.length; i++) {
                QueryVariableType currentKeyPrimitiveType = this.groupByKeyColumnsTypes[i];

                // primitiveType[] groupKeyVector_i = cCtx.getAllocationManager().get[primitiveType]Vector();
                ArrayAccessPath groupKeyVectorArrayAP = new ArrayAccessPath(
                        cCtx.defineVariable("groupKeyVector_" + i),
                        (currentKeyPrimitiveType == S_FL_BIN) ? S_A_FL_BIN :
                                (currentKeyPrimitiveType == S_VARCHAR) ? S_A_VARCHAR : primitiveArrayTypeForPrimitive(currentKeyPrimitiveType));

                String initMethodName = switch (currentKeyPrimitiveType) {
                    case P_DOUBLE -> "getDoubleVector";
                    case P_INT, P_INT_DATE -> "getIntVector";
                    case S_FL_BIN, S_VARCHAR -> "getNestedByteVector";

                    default -> throw new UnsupportedOperationException("AggregationOperator.produceVec does not support key type " + currentKeyPrimitiveType);
                };

                codeGenResult.add(createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), groupKeyVectorArrayAP.getType()),
                        groupKeyVectorArrayAP.getVariableName(),
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                createMethodInvocation(JaninoGeneralGen.getLocation(), JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"),"getAllocationManager"),
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
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), aggregationResultVectorAP.getType()),
                            aggregationResultVectorAP.getVariableName(),
                            createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    createMethodInvocation(JaninoGeneralGen.getLocation(), JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"), "getAllocationManager"),
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
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), currentKeyOffsetVariable.getType()),
                            currentKeyOffsetVariable.getVariableName(),
                            JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                    )
            );

            ScalarVariableAccessPath numberOfRecordsAP =
                    new ScalarVariableAccessPath(cCtx.defineVariable("number_of_records"), P_INT);
            codeGenResult.add(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getType()),
                            numberOfRecordsAP.getVariableName(),
                            new Java.FieldAccessExpression(
                                    JaninoGeneralGen.getLocation(),
                                    ((MapAccessPath) this.aggregationStateVariables[0]).read(),
                                    "numberOfRecords"
                            )
                    )
            );

            Java.Block whileLoopBody = createBlock(JaninoGeneralGen.getLocation());
            codeGenResult.add(
                    JaninoControlGen.createWhileLoop(
                            JaninoGeneralGen.getLocation(),
                            JaninoOperatorGen.lt(JaninoGeneralGen.getLocation(), currentKeyOffsetVariable.read(), numberOfRecordsAP.read()),
                            whileLoopBody
                    )
            );

            // Construct the key vectors
            for (int i = 0; i < this.groupByKeyColumnIndices.length; i++) {
                Java.MethodInvocation vectorConstructionMethodInvocation = createMethodInvocation(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedAggregationOperators"),
                        "constructVector",
                        new Java.Rvalue[] {
                                groupKeyVectorsAPs[i].getVectorVariable().read(),
                                new Java.FieldAccessExpression(
                                        JaninoGeneralGen.getLocation(),
                                        ((MapAccessPath) this.aggregationStateVariables[0]).read(),
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
                                    JaninoGeneralGen.getLocation(),
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
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedAggregationOperators"),
                                    "constructVector",
                                    new Java.Rvalue[] {
                                            aggregationResultAPs[i].getVectorVariable().read(),
                                            new Java.FieldAccessExpression(
                                                    JaninoGeneralGen.getLocation(),
                                                    ((MapAccessPath) this.aggregationStateVariables[0]).read(),
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
                            JaninoGeneralGen.getLocation(),
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
                                JaninoGeneralGen.getLocation(),
                                createMethodInvocation(JaninoGeneralGen.getLocation(), JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"), "getAllocationManager"),
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
                            JaninoGeneralGen.getLocation(),
                            createMethodInvocation(JaninoGeneralGen.getLocation(), JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"), "getAllocationManager"),
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
                            countIncrementRValue = createMethodInvocation(JaninoGeneralGen.getLocation(), avap.read(), "getValueCount");

                        } else if (firstOrdinalAP instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                            // count += avwsvap.readSelectionVectorLength();
                            countIncrementRValue = avwsvap.readSelectionVectorLength();

                        } else if (firstOrdinalAP instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                            // count += VectorisedAggregationOperators.count(avwvmap.readValidityMask(), avwvmap.readValidityMaskLength());
                            countIncrementRValue = createMethodInvocation(
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
                            countIncrementRValue = createMethodInvocation(
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
                                    "AggregationOperator.consumeVec does not support this access path for the count aggregation");

                        }

                        // Do the actual increment
                        // count += [countIncrementRValue];
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        JaninoGeneralGen.getLocation(),
                                        ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write(),
                                        countIncrementRValue
                                )
                        );

                    }

                    case NG_SUM -> {
                        // We simply need to add the cumulative sum of the current vector to the aggregation state variable
                        AccessPath inputOrdinalAP = cCtx.getCurrentOrdinalMapping().get(this.aggregationFunctionInputOrdinals[i][0]);
                        Java.Rvalue sumIncrementRValue;

                        if (inputOrdinalAP instanceof ArrayVectorWithSelectionVectorAccessPath avwsvap) {
                            // sum += VectorisedAggregationOperators.vectorSum(avwsvap.vector, avwsvap.vectorLength, avwsvap.selectionVector, avwsvap.selectionVectorLength);
                            sumIncrementRValue = createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedAggregationOperators"),
                                    "vectorSum",
                                    new Java.Rvalue[]{
                                            avwsvap.getArrayVectorVariable().getVectorVariable().read(),
                                            avwsvap.getArrayVectorVariable().getVectorLengthVariable().read(),
                                            avwsvap.readSelectionVector(),
                                            avwsvap.readSelectionVectorLength()
                                    }
                            );

                        } else if (inputOrdinalAP instanceof ArrayVectorWithValidityMaskAccessPath avwvmap) {
                            // sum += VectorisedAggregationOperators.vectorSum(avwvmap.vector, avwvmap.vectorLength, avwvmap.validityMask, avwvmap.validityMaskLength);
                            sumIncrementRValue = createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedAggregationOperators"),
                                    "vectorSum",
                                    new Java.Rvalue[]{
                                            avwvmap.getArrayVectorVariable().getVectorVariable().read(),
                                            avwvmap.getArrayVectorVariable().getVectorLengthVariable().read(),
                                            avwvmap.readValidityMask(),
                                            avwvmap.readValidityMaskLength()
                                    }
                            );

                        } else {
                            throw new UnsupportedOperationException(
                                    "AggregationOperator.consumeVec does not support this access path for the SUM aggregation");

                        }

                        // Do the actual increment
                        // count += [countIncrementRValue];
                        codeGenResult.add(
                                createVariableAdditionAssignmentStm(
                                        JaninoGeneralGen.getLocation(),
                                        ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).write(),
                                        sumIncrementRValue
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

                // The number of arguments depends on the current vector type
                boolean arrayVector = keyColumnsAccessPaths[i] instanceof ArrayVectorAccessPath
                        || keyColumnsAccessPaths[i] instanceof ArrayVectorWithSelectionVectorAccessPath
                        || keyColumnsAccessPaths[i] instanceof ArrayVectorWithValidityMaskAccessPath;

                // Determine the arguments
                int numberOfArguments = 3 + (arrayVector ? 1 : 0) + (accountForFiltering ? 2 : 0);
                Java.Rvalue[] methodInvocationArguments = new Java.Rvalue[numberOfArguments];
                int currentMethodInvocationArgumentIndex = 0;
                methodInvocationArguments[currentMethodInvocationArgumentIndex++] = this.groupKeyPreHashVector.read();
                methodInvocationArguments[currentMethodInvocationArgumentIndex++] = switch (keyColumnsAccessPathTypes[i]) {
                    case ARRAY_DOUBLE_VECTOR, ARRAY_FIXED_LENGTH_BINARY_VECTOR, ARRAY_INT_VECTOR, ARRAY_INT_DATE_VECTOR, ARRAY_VARCHAR_VECTOR
                            -> ((ArrayVectorAccessPath) keyColumnsAccessPaths[i]).getVectorVariable().read();
                    case ARROW_FIXED_LENGTH_BINARY_VECTOR, ARROW_INT_VECTOR
                            -> ((ArrowVectorAccessPath) keyColumnsAccessPaths[i]).read();
                    case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR, ARROW_INT_VECTOR_W_SELECTION_VECTOR
                            -> ((ArrowVectorWithSelectionVectorAccessPath) keyColumnsAccessPaths[i]).readArrowVector();
                    case ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK, ARROW_INT_VECTOR_W_VALIDITY_MASK
                            -> ((ArrowVectorWithValidityMaskAccessPath) keyColumnsAccessPaths[i]).readArrowVector();

                    default -> throw new UnsupportedOperationException("AggregationOperator.consumeVec does not support this key column access path type");
                };

                // Check if we need to add a length parameter
                if (arrayVector) {
                    methodInvocationArguments[currentMethodInvocationArgumentIndex++] = switch (keyColumnsAccessPathTypes[i]) {
                        case ARRAY_DOUBLE_VECTOR, ARRAY_FIXED_LENGTH_BINARY_VECTOR, ARRAY_INT_VECTOR, ARRAY_INT_DATE_VECTOR, ARRAY_VARCHAR_VECTOR
                                -> ((ArrayVectorAccessPath) keyColumnsAccessPaths[i]).getVectorLengthVariable().read();

                        default -> throw new UnsupportedOperationException("AggregationOperator.consumeVec does not support this key column access path type");
                    };
                }

                // Check if we need to add filtering arguments
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
                        new Java.BooleanLiteral(JaninoGeneralGen.getLocation(), (i == 0) ? "false" : "true");

                // Perform the actual hashing
                codeGenResult.add(
                        createMethodInvocationStm(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "VectorisedHashOperators"),
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

            Java.Block tableMaintenanceLoopBody = new Java.Block(JaninoGeneralGen.getLocation());
            ScalarVariableAccessPath aviv =
                    new ScalarVariableAccessPath(cCtx.defineVariable("aviv"), P_INT);
            ScalarVariableAccessPath recordIndexAP = aviv; // Variable to allow record accessing via a selection vector

            Java.Rvalue[] incrementForKeyArguments =
                    new Java.Rvalue[this.groupByKeyColumnIndices.length + 1 + this.aggregationMapGenerator.valueVariableNames.length];
            int currentArgumentIndex = 0;

            // Initialise the record count, perform filtering if necessary and update record indexing AP if needed
            if (currentOrdinalMapping.get(0) instanceof ArrowVectorAccessPath avap) {
                // int recordCount = [avap.read()].getValueCount();
                recordCountInitValue = createMethodInvocation(JaninoGeneralGen.getLocation(), avap.read(), "getValueCount");
            } else if (currentOrdinalMapping.get(0) instanceof ArrayVectorAccessPath avap) {
                recordCountInitValue = avap.getVectorLengthVariable().read();
            } else if (currentOrdinalMapping.get(0) instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                recordCountInitValue = avwvmap.readValidityMaskLength();

                // In the table maintenance loop, skip records whose validity indicator are set to false
                tableMaintenanceLoopBody.addStatement(
                        JaninoControlGen.createIfNotContinue(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
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
                                JaninoGeneralGen.getLocation(),
                                toJavaType(JaninoGeneralGen.getLocation(), recordIndexAP.getType()),
                                recordIndexAP.getVariableName(),
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
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
                            JaninoGeneralGen.getLocation(),
                            avap.read(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );
                } else if (currentKeyOrdinal instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                    incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                            JaninoGeneralGen.getLocation(),
                            avwsvap.readArrowVector(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );
                } else if (currentKeyOrdinal instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                    incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                            JaninoGeneralGen.getLocation(),
                            avwvmap.readArrowVector(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );
                } else if (currentKeyOrdinal instanceof ArrayVectorAccessPath avap) {
                    incrementForKeyArguments[currentArgumentIndex++] = JaninoGeneralGen.createArrayElementAccessExpr(
                            JaninoGeneralGen.getLocation(),
                            avap.getVectorVariable().read(),
                            recordIndexAP.read()
                    );
                }
            }

            // Obtain the pre-hash argument
            incrementForKeyArguments[currentArgumentIndex++] = JaninoGeneralGen.createArrayElementAccessExpr(
                    JaninoGeneralGen.getLocation(),
                    this.groupKeyPreHashVector.read(),
                    recordIndexAP.read()
            );

            // Now get the value to insert based on the aggregation functions
            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                AggregationFunction currentFunction = this.aggregationFunctions[i];

                if (currentFunction == AggregationFunction.G_COUNT) {
                    incrementForKeyArguments[currentArgumentIndex++] = JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1);

                } else if (currentFunction == AggregationFunction.G_SUM) {
                    // We simply need to maintain the sum based on the input ordinal type
                    AccessPath inputOrdinal = cCtx.getCurrentOrdinalMapping().get(this.aggregationFunctionInputOrdinals[i][0]);
                    QueryVariableType inputOrdinalType = inputOrdinal.getType();

                    if (inputOrdinalType == ARROW_DOUBLE_VECTOR || inputOrdinalType == ARROW_INT_VECTOR) {
                        ArrowVectorAccessPath castInputOrdinal = (ArrowVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                castInputOrdinal.read(),
                                "get",
                                new Java.Rvalue[] { recordIndexAP.read() }
                        );

                    } else if (inputOrdinalType == ARRAY_DOUBLE_VECTOR || inputOrdinalType == ARRAY_INT_VECTOR) {
                        ArrayVectorAccessPath castInputOrdinal = (ArrayVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                castInputOrdinal.getVectorVariable().read(),
                                recordIndexAP.read()
                        );

                    } else if (inputOrdinalType == ARROW_DOUBLE_VECTOR_W_SELECTION_VECTOR || inputOrdinalType == ARROW_INT_VECTOR_W_SELECTION_VECTOR) {
                        ArrowVectorWithSelectionVectorAccessPath castInputOrdinal = (ArrowVectorWithSelectionVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                castInputOrdinal.readArrowVector(),
                                "get",
                                new Java.Rvalue[] { recordIndexAP.read() }
                        );


                    }  else if (inputOrdinalType == ARROW_DOUBLE_VECTOR_W_VALIDITY_MASK || inputOrdinalType == ARROW_INT_VECTOR_W_VALIDITY_MASK) {
                        ArrowVectorWithValidityMaskAccessPath castInputOrdinal = (ArrowVectorWithValidityMaskAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                castInputOrdinal.readArrowVector(),
                                "get",
                                new Java.Rvalue[] { recordIndexAP.read() }
                        );

                    } else if (inputOrdinalType == ARRAY_DOUBLE_VECTOR_W_SELECTION_VECTOR) {
                        ArrayVectorWithSelectionVectorAccessPath castInputOrdinal = (ArrayVectorWithSelectionVectorAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                castInputOrdinal.getArrayVectorVariable().getVectorVariable().read(),
                                recordIndexAP.read()
                        );

                    } else if (inputOrdinalType == ARRAY_DOUBLE_VECTOR_W_VALIDITY_MASK) {
                        ArrayVectorWithValidityMaskAccessPath castInputOrdinal = (ArrayVectorWithValidityMaskAccessPath) inputOrdinal;

                        // Simply take the value indicated by the access path
                        incrementForKeyArguments[currentArgumentIndex++] = JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
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
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), recordCount.getType()),
                            recordCount.getVariableName(),
                            recordCountInitValue
                    )
            );

            // Add the actual hash-table maintenance loop
            codeGenResult.add(
                    JaninoControlGen.createForLoop(
                            JaninoGeneralGen.getLocation(),
                            createLocalVariable(
                                    JaninoGeneralGen.getLocation(),
                                    toJavaType(JaninoGeneralGen.getLocation(), aviv.getType()),
                                    aviv.getVariableName(),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                            ),
                            JaninoOperatorGen.lt(JaninoGeneralGen.getLocation(), aviv.read(), recordCount.read()),
                            JaninoOperatorGen.postIncrement(JaninoGeneralGen.getLocation(), aviv.write()),
                            tableMaintenanceLoopBody
                    )
            );

            // Now perform the actual hash-table maintenance by invoking the incrementForKey method
            tableMaintenanceLoopBody.addStatement(
                    createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            ((MapAccessPath) this.aggregationStateVariables[0]).read(),
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
            this.aggregationStateVariables = new AccessPath[this.aggregationFunctions.length];

            for (int i = 0; i < this.aggregationFunctions.length; i++) {
                if (this.aggregationFunctions[i] == AggregationFunction.NG_COUNT) {
                    this.aggregationStateVariables[i] = new ScalarVariableAccessPath(
                            cCtx.defineVariable("count"),
                            P_LONG
                    );

                } else if (this.aggregationFunctions[i] == AggregationFunction.NG_SUM) {
                    this.aggregationStateVariables[i] = new ScalarVariableAccessPath(
                            cCtx.defineVariable("sum"),
                            primitiveType(om.get(this.aggregationFunctionInputOrdinals[i][0]).getType())
                    );

                }
            }

            return;
        }

        // Otherwise, we have a group-by aggregation
        this.aggregationStateVariables = new AccessPath[1];

        // Obtain the type of the group-by columns
        this.groupByKeyColumnsTypes = new QueryVariableType[this.groupByKeyColumnIndices.length];
        for (int i = 0; i < this.groupByKeyColumnsTypes.length; i++) {
            QueryVariableType ordinalType = om.get(this.groupByKeyColumnIndices[i]).getType();
            if (ordinalType == S_FL_BIN
                    || ordinalType == ARRAY_FIXED_LENGTH_BINARY_VECTOR
                    || ordinalType == ARROW_FIXED_LENGTH_BINARY_VECTOR
                    || ordinalType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR
                    || ordinalType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_VALIDITY_MASK)
                ordinalType = S_FL_BIN;
            else if (ordinalType == S_VARCHAR
                    || ordinalType == ARRAY_VARCHAR_VECTOR)
                ordinalType = S_VARCHAR;
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
        this.aggregationStateVariables[0] = new MapAccessPath(
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
            for (int i = 0; i < this.aggregationStateVariables.length; i++) {
                codeGenResult.add(
                        createLocalVariable(
                                JaninoGeneralGen.getLocation(),
                                toJavaType(JaninoGeneralGen.getLocation(), this.aggregationStateVariables[i].getType()),
                                ((ScalarVariableAccessPath) this.aggregationStateVariables[i]).getVariableName(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                        )
                );
            }

        } else {
            codeGenResult.add(
                    new Java.LocalClassDeclarationStatement(
                            this.aggregationMapGenerator.generate()
                    )
            );
            Java.Type generatedMapType = JaninoGeneralGen.createReferenceType(
                    JaninoGeneralGen.getLocation(),
                    this.aggregationMapGenerator.generate().getName());

            codeGenResult.add(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            generatedMapType,
                            ((MapAccessPath) this.aggregationStateVariables[0]).getVariableName(),
                            JaninoClassGen.createClassInstance(
                                    JaninoGeneralGen.getLocation(),
                                    generatedMapType
                            )
                    )
            );
        }

        return codeGenResult;
    }

}
