package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.SIMDLoopAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import evaluation.general_support.hashmaps.Int_Hash_Function;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static evaluation.codegen.infrastructure.context.QueryVariableType.MAP_INT_MULTI_OBJECT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.queryVariableTypeFromJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclarationStm;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createPublicFinalFieldDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIf;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameter;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameters;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.gt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.not;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.plus;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrementStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.sub;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createSimpleVariableDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * {@link CodeGenOperator} which performs a join over two tables for a given join condition. All
 * joins implemented by this operator are currently hash-joins over a single equality predicate.
 */
public class JoinOperator extends CodeGenOperator<LogicalJoin> {

    /**
     * The {@link CodeGenOperator} producing the records to be joined by {@code this} on "left" side
     * of the join.
     */
    private final CodeGenOperator<?> leftChild;

    /**
     * The {@link CodeGenOperator} producing the records to be joined by {@code this} on "right" side
     * of the join.
     */
    private final CodeGenOperator<?> rightChild;

    /**
     * The variable holding the generated class type that will be used to store the records of the
     * left child in the hash-table during the build stage.
     */
    private Java.LocalClassDeclarationStatement leftChildRecordType;

    /**
     * The access path to the hash-map variable used for performing the join.
     */
    private MapAccessPath joinMapAP;

    /**
     * The access path to the pre-hash vector variable used for performing the join.
     */
    private ArrayAccessPath preHashVectorAP;

    /**
     * The names of the result vectors of this operator in the vectorised paradigm.
     */
    private String[] resultVectorNames;

    /**
     * The access paths indicating the vector types that should be exposed as the result of this
     * operator in the vectorised paradigm.
     */
    private ArrayAccessPath[] resultVectorDefinitions;

    /**
     * Boolean indicating if the consume method should perform a hash-table build or a hash-table probe.
     * Will be initialised as false to indicate a hash-table build first.
     */
    private boolean consumeInProbePhase;

    /**
     * Creates a new {@link JoinOperator} instance for a specific sub-query.
     * @param join The logical join (and sub-query) for which the operator is created.
     * @param simdEnabled Whether the operator is allowed to use SIMD for processing.
     * @param leftChild The {@link CodeGenOperator} producing the left input side of the join.
     * @param rightChild The {@link CodeGenOperator} producing the right input side of the join.
     */
    public JoinOperator(
            LogicalJoin join,
            boolean simdEnabled,
            CodeGenOperator<?> leftChild,
            CodeGenOperator<?> rightChild
    ) {
        super(join, simdEnabled);
        this.leftChild = leftChild;
        this.leftChild.setParent(this);
        this.rightChild = rightChild;
        this.rightChild.setParent(this);
        this.consumeInProbePhase = false;
        this.resultVectorNames = new String[join.getRowType().getFieldCount()];
        this.resultVectorDefinitions = new ArrayAccessPath[join.getRowType().getFieldCount()];

        // Check pre-conditions
        if (join.getJoinType() != JoinRelType.INNER)
            throw new UnsupportedOperationException("JoinOperator currently only supports inner joins");

        RexNode joinCondition = join.getCondition();
        if (!(joinCondition instanceof RexCall joinConditionCall))
            throw new UnsupportedOperationException("JoinOperator currently only supports join conditions wrapped in a RexCall");

        if (joinConditionCall.getOperator().getKind() != SqlKind.EQUALS)
            throw new UnsupportedOperationException("JoinOperator currently only supports equality join conditions");

        if (joinConditionCall.getOperands().size() != 2)
            throw new UnsupportedOperationException("JoinOperator currently only supports join conditions over two operands");

        List<RexNode> joinConditionOperands = joinConditionCall.getOperands();
        for (RexNode joinConditionOperand : joinConditionOperands) {
            if (!(joinConditionOperand instanceof RexInputRef))
                throw new UnsupportedOperationException("JoinOperator currently only supports join condition operands that refer to an input column");
        }

        if (joinConditionOperands.get(0).getType() != joinConditionOperands.get(1).getType())
            throw new UnsupportedOperationException("JoinOperator expects the join condition operands to be of the same type");
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

        // Reserve a name for the join map and set its access path
        this.joinMapAP = new MapAccessPath(cCtx.defineVariable("join_map"), MAP_INT_MULTI_OBJECT);

        // First build the hash-table by calling the produceNonVec method on the left child operator,
        // which will eventually invoke the consumeNonVec method on @this which should perform the
        // hash-table build.
        // Additionally, the consumeNonVec method will initialise the record type for the hash table
        // which will have to be added to the codeGenResult first, after which we initialise the
        // actual map used for the join.
        cCtx.pushCodeGenContext();
        List<Java.Statement> leftChildProduceResult = this.leftChild.produceNonVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        codeGenResult.add(this.leftChildRecordType);

        Java.Type joinMapType = createReferenceType(
                getLocation(),
                "Int_Multi_Object_Map",
                this.leftChildRecordType.lcd.getName());
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        joinMapType,
                        this.joinMapAP.getVariableName(),
                        createClassInstance(getLocation(), joinMapType)
                )
        );

        codeGenResult.addAll(leftChildProduceResult);

        // Next, call the produce method on the right child operator, which will eventually invoke
        // the consumeNonVec method on @this, which should perform the hash-table probe and call
        // the consumeNonVec method on the parent.
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.rightChild.produceNonVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        return this.consume(cCtx, oCtx, false);
    }

    /**
     * Method for generating the code that performs the hash-table build in the non-vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecBuild(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Check that we can handle the key type (casts valid due to pre-condition check in constructor)
        RexCall joinCondition = (RexCall) this.getLogicalSubplan().getCondition();
        RexInputRef buildKey = (RexInputRef) joinCondition.getOperands().get(0);
        int buildKeyOrdinal = buildKey.getIndex();
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        AccessPath buildKeyAP = currentOrdinalMapping.get(buildKeyOrdinal);
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecBuild only supports integer join key columns");

        // Idea: first perform hashing of the key-column, then perform hash-table inserts.
        // In the case of SIMD-enabled pre-hashing, the SIMD data is flattened afterwards
        // and hence the hash-table inserts can be done in a unified fashion.
        ScalarVariableAccessPath leftJoinKeyPreHash;
        Java.Rvalue[] recordColumnValues = new Java.Rvalue[currentOrdinalMapping.size()];
        Java.Block hashInsertTarget;

        if (!this.useSIMDNonVec(cCtx)) {
            // Obtain the local access path for the key column value
            Java.Rvalue keyColumnRvalue = getRValueFromAccessPathNonVec(cCtx, buildKeyOrdinal, codeGenResult);
            buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);

            // Compute the pre-hash value in a local variable
            leftJoinKeyPreHash = new ScalarVariableAccessPath(cCtx.defineVariable("left_join_key_prehash"), P_LONG);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), leftJoinKeyPreHash.getType()),
                            leftJoinKeyPreHash.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "Int_Hash_Function"),
                                    "preHash",
                                    new Java.Rvalue[] { keyColumnRvalue }
                            )
                    ));

            // Get the values for the current row
            currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
            for (int i = 0; i < currentOrdinalMapping.size(); i++)
                recordColumnValues[i] = getRValueFromAccessPathNonVec(cCtx, i, codeGenResult);

            // Set the correct target
            hashInsertTarget = new Java.Block(getLocation());
            codeGenResult.add(hashInsertTarget);

        } else { // this.useSIMDNonVec(cCtx)
            // Perform the SIMD-ed pre-hashing and flattening
            var preHashAndFlattenResult = Int_Hash_Function.preHashAndFlattenSIMD(
                    cCtx,
                    buildKeyAP
            );

            buildKeyAP = preHashAndFlattenResult.keyColumnAccessPath;
            leftJoinKeyPreHash = preHashAndFlattenResult.keyColumnPreHashAccessPath;
            codeGenResult.addAll(preHashAndFlattenResult.generatedCode);
            hashInsertTarget = preHashAndFlattenResult.flattenedForLoopBody;

            // Obtain the record values to insert into the hash-map
            currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
            for (int i = 0; i < currentOrdinalMapping.size(); i++) {
                AccessPath valueAP = currentOrdinalMapping.get(i);

                if (valueAP instanceof SIMDLoopAccessPath valueAP_slap) {
                    if (i == buildKeyOrdinal)
                        recordColumnValues[i] = ((ScalarVariableAccessPath) buildKeyAP).read();
                    else {
                        // recordColumnValues[i] = [valueAP_slap.readArrowVector()].get([valueAP_slap.readArrowVectorOffset()] + [simd_vector_i]);
                        recordColumnValues[i] = createMethodInvocation(
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
                    }

                } else {
                    throw new UnsupportedOperationException(
                            "JoinOperator.consumeNonVecBuild does not support this valueAP for hash-map insertion");
                }

            }

        }

        // Create the record for the current row
        String recordVariableName = cCtx.defineVariable("left_join_record");
        Java.Type recordType = createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName());
        hashInsertTarget.addStatement(
                createLocalVariable(
                        getLocation(),
                        recordType,
                        recordVariableName,
                        createClassInstance(
                                getLocation(),
                                recordType,
                                recordColumnValues
                        )
                )
        );

        // Insert the record into the join map
        hashInsertTarget.addStatement(
                createMethodInvocationStm(
                        getLocation(),
                        this.joinMapAP.read(),
                        "add",
                        new Java.Rvalue[] {
                                // Cast valid due to local access path and flattening
                                ((ScalarVariableAccessPath) buildKeyAP).read(),
                                leftJoinKeyPreHash.read(),
                                createAmbiguousNameRef(getLocation(), recordVariableName)
                        }
                )
        );

        return codeGenResult;
    }

    /**
     * Method for generating the code that performs the hash-table probe in the non-vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeNonVecProbe(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Check that we can handle the key type (casts valid due to pre-condition check in constructor)
        RexCall joinCondition = (RexCall) this.getLogicalSubplan().getCondition();
        RexInputRef buildKey = (RexInputRef) joinCondition.getOperands().get(1);
        // Need to convert the output ordinal to the right-input ordinal
        int buildKeyOrdinal = buildKey.getIndex() - this.getLogicalSubplan().getLeft().getRowType().getFieldCount();
        AccessPath buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecProbe only supports integer join key columns");

        // Idea: first perform hashing of the key-column, then look up the join records in the hash-table
        // and finally expose the resulting records
        // TODO: when SIMD processing is enabled, the result of this operator will not be constructed as a SIMD-enabled datastructure
        // TODO: consider implementing that functionality when it becomes required

        // We perform pre-hashing first, since in the case of SIMD-enabled pre-hashing, the SIMD data
        // is flattened afterwards and hence the hash-table probes and result exposure can be done in a unified fashion.
        // This way of unification also implies we need to prepare statements for obtaining the column values for the
        // right-hand columns of the join at the same time.
        ScalarVariableAccessPath rightJoinKeyPreHash;
        Java.Block resultExposureTarget;
        List<AccessPath> rightHandOrdinalMappings = cCtx.getCurrentOrdinalMapping();
        List<Java.Statement> localRightHandAPsInitialisationStatements = new ArrayList<>();
        List<AccessPath> localRightHandAPs = new ArrayList<>(rightHandOrdinalMappings.size());

        // Probe the hash-table based on the input type
        if (!this.useSIMDNonVec(cCtx)) {
            // Obtain the local access path for the key column value
            Java.Rvalue keyColumnRvalue = getRValueFromAccessPathNonVec(cCtx, buildKeyOrdinal, codeGenResult);
            buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);

            // Compute the pre-hash value in a local variable
            rightJoinKeyPreHash =
                    new ScalarVariableAccessPath(cCtx.defineVariable("right_join_key_prehash"), P_LONG);
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), rightJoinKeyPreHash.getType()),
                            rightJoinKeyPreHash.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "Int_Hash_Function"),
                                    "preHash",
                                    new Java.Rvalue[] { keyColumnRvalue }
                            )
                    ));

            // Prepare the statements for obtaining the local access paths for the right-hand join columns
            for (int i = 0; i < rightHandOrdinalMappings.size(); i++) {
                getRValueFromAccessPathNonVec(cCtx, i, localRightHandAPsInitialisationStatements);
                localRightHandAPs.add(i, cCtx.getCurrentOrdinalMapping().get(i));
            }

            // Set the correct result exposure target
            resultExposureTarget = new Java.Block(getLocation());
            codeGenResult.add(resultExposureTarget);

        } else { // this.useSIMDNonVec(cCtx)
            // Perform the SIMD-ed pre-hashing and flattening
            var preHashAndFlattenResult =
                    Int_Hash_Function.preHashAndFlattenSIMD(cCtx, buildKeyAP);

            buildKeyAP = preHashAndFlattenResult.keyColumnAccessPath;
            rightJoinKeyPreHash = preHashAndFlattenResult.keyColumnPreHashAccessPath;
            codeGenResult.addAll(preHashAndFlattenResult.generatedCode);
            resultExposureTarget = preHashAndFlattenResult.flattenedForLoopBody;

            // Prepare the statements for obtaining the local access paths for the right-hand join columns
            for (int i = 0; i < rightHandOrdinalMappings.size(); i++) {
                AccessPath valueAP = rightHandOrdinalMappings.get(i);

                if (valueAP instanceof SIMDLoopAccessPath valueAP_slap) {
                    if (i == buildKeyOrdinal)
                        // If we have the key ordinal, we know a local variable has already been created, so use it
                        localRightHandAPs.add(i, buildKeyAP);

                    else {
                        // Otherwise, we need to create the variable now
                        // [type] right_join_ord_[i] = [valueAP_slap.readArrowVector()].get([valueAP_slap.readArrowVectorOffset()] + [simd_vector_i]);
                        String rightJoinOrdIName = cCtx.defineVariable("right_join_ord_" + i);
                        QueryVariableType rightJoinOrdType = primitiveType(valueAP_slap.getType());

                        localRightHandAPsInitialisationStatements.add(
                                createLocalVariable(
                                        getLocation(),
                                        toJavaType(getLocation(), rightJoinOrdType),
                                        rightJoinOrdIName,
                                        createMethodInvocation(
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
                                        )
                                )
                        );

                        // And store the access path to it
                        localRightHandAPs.add(i, new ScalarVariableAccessPath(rightJoinOrdIName, rightJoinOrdType));

                    }

                } else {
                    throw new UnsupportedOperationException(
                            "JoinOperator.consumeNonVecProbe does not support this valueAP for hash-map probing");
                }

            }

        }

        // Check if the left side of the join contains records for the given key, otherwise continue
        // if (![this.joinMap.read()].contains(key, prehash)) continue;
        resultExposureTarget.addStatement(
                createIfNotContinue(
                        getLocation(),
                        createMethodInvocation(
                                getLocation(),
                                this.joinMapAP.read(),
                                "contains",
                                new Java.Rvalue[] {
                                        ((ScalarVariableAccessPath) buildKeyAP).read(), // Cast valid due to local access path
                                        rightJoinKeyPreHash.read()
                                }
                        )
                )
        );

        // Get the column values for the right-hand side of the join in local variables
        resultExposureTarget.addStatements(localRightHandAPsInitialisationStatements);

        // Obtain the values for the left side of the join for the current key value
        String joinRecordsListName = cCtx.defineVariable("records_to_join_list");
        resultExposureTarget.addStatement(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(getLocation(), "ArrayList", this.leftChildRecordType.lcd.getName()),
                        joinRecordsListName,
                        createMethodInvocation(
                                getLocation(),
                                this.joinMapAP.read(),
                                "get",
                                new Java.Rvalue[] {
                                        ((ScalarVariableAccessPath) buildKeyAP).read(), // Cast valid due to local access path
                                        rightJoinKeyPreHash.read()
                                }
                        )
                )
        );

        // Generate a for-loop to iterate over the join records from the left-hand side
        // int left_join_record_count = records_to_join_list.size();
        ScalarVariableAccessPath leftJoinRecordCount =
                new ScalarVariableAccessPath(cCtx.defineVariable("left_join_record_count"), P_INT);
        resultExposureTarget.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), leftJoinRecordCount.getType()),
                        leftJoinRecordCount.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), joinRecordsListName),
                                "size"
                        )
                )
        );

        // for (int i = 0; i < left_join_record_count; i++) {
        //     [RecordType] left_join_record = records_to_join_list.get(i);
        //     [joinLoopBody]
        // }
        ScalarVariableAccessPath joinLoopIndexVar =
                new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
        Java.Block joinLoopBody = new Java.Block(getLocation());
        resultExposureTarget.addStatement(
                createForLoop(
                        getLocation(),
                        createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, joinLoopIndexVar.getVariableName(), "0"),
                        lt(getLocation(), joinLoopIndexVar.read(), leftJoinRecordCount.read()),
                        postIncrement(getLocation(), joinLoopIndexVar.write()),
                        joinLoopBody
                )
        );

        String leftJoinRecordName = cCtx.defineVariable("left_join_record");
        Java.Type leftJoinRecordType = createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName());
        joinLoopBody.addStatement(
                createLocalVariable(
                        getLocation(),
                        leftJoinRecordType,
                        leftJoinRecordName,
                        createCast(
                                getLocation(),
                                leftJoinRecordType,
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), joinRecordsListName),
                                        "get",
                                        new Java.Rvalue[] {joinLoopIndexVar.read() }
                                )
                        )
                )
        );

        // In the for-loop, expose the left-hand join columns as local variables
        // Also start updating the ordinal mapping
        List<Java.FieldDeclarationOrInitializer> leftSideColumnFields =
                this.leftChildRecordType.lcd.getVariableDeclaratorsAndInitializers();
        List<AccessPath> updatedOrdinalMapping =
                new ArrayList<>(this.getLogicalSubplan().getRowType().getFieldCount());

        for (int i = 0; i < leftSideColumnFields.size(); i++) {
            // Cast valid due to generateRecordTypeForRelation(...)
            Java.FieldDeclaration currentLeftSideField = (Java.FieldDeclaration) leftSideColumnFields.get(i);

            // Generate an access path
            ScalarVariableAccessPath currentLeftSideColumnVar =
                    new ScalarVariableAccessPath(
                            "left_join_ord_" + i,
                            queryVariableTypeFromJavaType(currentLeftSideField.type)
                    );
            updatedOrdinalMapping.add(i, currentLeftSideColumnVar);

            // Create the variable referred to by the access path
            joinLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), currentLeftSideColumnVar.getType()),
                            currentLeftSideColumnVar.getVariableName(),
                            createAmbiguousNameRef(
                                    getLocation(),
                                    leftJoinRecordName + "." + currentLeftSideField.variableDeclarators[0].name
                            )
                    )
            );
        }

        // Update the right-hand part of the ordinal mapping
        for (int i = 0; i < localRightHandAPs.size(); i++) {
            updatedOrdinalMapping.add(leftSideColumnFields.size() + i, localRightHandAPs.get(i));
        }

        // Set the updated ordinal mapping
        cCtx.setCurrentOrdinalMapping(updatedOrdinalMapping);

        // Have the parent consume the result
        joinLoopBody.addStatements(this.nonVecParentConsume(cCtx, oCtx));

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Reserve a name for the join map and set its access path
        this.joinMapAP = new MapAccessPath(cCtx.defineVariable("join_map"), MAP_INT_MULTI_OBJECT);

        // Reserve the names for the result vectors
        for (int i = 0; i < this.resultVectorNames.length; i++)
            this.resultVectorNames[i] = cCtx.defineVariable("join_result_vector_ord_" + i);

        // For vectorised implementations, allocate a pre-hash vector
        this.preHashVectorAP = new ArrayAccessPath(cCtx.defineVariable("pre_hash_vector"), P_A_LONG);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        createPrimitiveArrayType(getLocation(), Java.Primitive.LONG),
                        this.preHashVectorAP.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                createMethodInvocation(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "cCtx"),
                                        "getAllocationManager"
                                ),
                                "getLongVector"
                        )
                )
        );

        // First build the hash-table by calling the produceVec method on the left child operator,
        // which will eventually invoke the consumeVec method on @this which should perform the
        // hash-table build.
        // Additionally, the consumeVec method will initialise the record type for the hash table and
        // prepare the left-hand side of the result vector type initialisation.
        cCtx.pushCodeGenContext();
        List<Java.Statement> leftChildProduceResult = this.leftChild.produceVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // We first add the join record type to the codegen result, and initialise the join map
        codeGenResult.add(this.leftChildRecordType);

        Java.Type joinMapType = createReferenceType(
                getLocation(),
                "Int_Multi_Object_Map",
                this.leftChildRecordType.lcd.getName());
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        joinMapType,
                        this.joinMapAP.getVariableName(),
                        createClassInstance(getLocation(), joinMapType)
                )
        );

        // Then we add the left-child production code
        codeGenResult.addAll(leftChildProduceResult);

        // Next, call the produce method on the right child operator, which will eventually invoke
        // the consumeVec method on @this, which should perform the hash-table probe, continue the
        // result vector type initialisation and call the consumeVec method on the parent.
        cCtx.pushCodeGenContext();
        List<Java.Statement> rightChildProduceResult = this.rightChild.produceVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // Allocate the result vectors first
        for (int i = 0; i < this.resultVectorDefinitions.length; i++) {
            ArrayAccessPath vectorDescription = this.resultVectorDefinitions[i];
            String instantiationMethod = switch (vectorDescription.getType()) {
                case P_A_BOOLEAN -> "getBooleanVector";
                case P_A_INT -> "getIntVector";
                case P_A_LONG -> "getLongVector";
                default -> throw new UnsupportedOperationException(
                        "JoinOperator.produceVec does not support allocating this result vector type");
            };

            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), vectorDescription.getType()),
                            vectorDescription.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "cCtx"),
                                            "getAllocationManager"
                                    ),
                                    instantiationMethod
                            )
                    )
            );
        }

        // Then add the right child production result
        codeGenResult.addAll(rightChildProduceResult);

        // For vectorised implementations, deallocate the pre-hash vector
        codeGenResult.add(
                createMethodInvocationStm(
                        getLocation(),
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "cCtx"),
                                "getAllocationManager"
                        ),
                        "release",
                        new Java.Rvalue[] { this.preHashVectorAP.read() }
                )
        );

        // And deallocate the result vectors
        for (int i = 0; i < this.resultVectorDefinitions.length; i++) {
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "release",
                            new Java.Rvalue[] { this.resultVectorDefinitions[i].read() }
                    )
            );
        }

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Set-up the current part of the result vector type initialisation
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        int vectorOffset = consumeInProbePhase ? this.leftChild.getLogicalSubplan().getRowType().getFieldCount()
                                               : 0;
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            int outputVectorIndex = vectorOffset + i;
            QueryVariableType primitiveOrdinalType =
                    primitiveType(currentOrdinalMapping.get(i).getType());
            this.resultVectorDefinitions[outputVectorIndex] =
                    new ArrayAccessPath(
                            this.resultVectorNames[outputVectorIndex],
                            primitiveArrayTypeForPrimitive(primitiveOrdinalType)
                    );
        }

        // Now to the actual consume task
        return this.consume(cCtx, oCtx, true);
    }

    /**
     * Method for generating code that perform the hash-table build in the vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeVecBuild(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Check that we can handle the key type (casts valid due to pre-condition check in constructor)
        RexCall joinCondition = (RexCall) this.getLogicalSubplan().getCondition();
        RexInputRef buildKey = (RexInputRef) joinCondition.getOperands().get(0);
        int buildKeyOrdinal = buildKey.getIndex();
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        AccessPath buildKeyAP = currentOrdinalMapping.get(buildKeyOrdinal);
        QueryVariableType buildKeyAPType = buildKeyAP.getType();
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeVecBuild only supports integer join key columns");

        // Idea: first perform pre-hashing of the key-column and then perform hash-table inserts
        if (buildKeyAPType == ARROW_INT_VECTOR) {
            // We know that all other vectors on the build side must be of some arrow vector type
            // without any validity mask/selection vector
            ArrowVectorAccessPath buildKeyAVAP = ((ArrowVectorAccessPath) buildKeyAP);

            if (!this.useSIMDVec()) {
                // VectorisedHashOperators.constructPreHashKeyVector([this.preHashVectorAP.read()], [buildKeyAVAP.read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                "constructPreHashKeyVector",
                                new Java.Rvalue[]{
                                        this.preHashVectorAP.read(),
                                        buildKeyAVAP.read()
                                }
                        ));
            } else {
                // VectorisedHashOperators.constructPreHashKeyVectorSIMD([this.preHashVectorAP.read()], [buildKeyAVAP.read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                "constructPreHashKeyVectorSIMD",
                                new Java.Rvalue[]{
                                        this.preHashVectorAP.read(),
                                        buildKeyAVAP.read()
                                }
                        ));
            }

            // Now iterate over the key vector to construct the hash-table records and insert them into the table
            // int recordCount = [buildKeyAVAP.read()].getValueCount();
            ScalarVariableAccessPath recordCountAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("recordCount"),
                    P_INT
            );
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveType(getLocation(), Java.Primitive.INT),
                            recordCountAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    buildKeyAVAP.read(),
                                    "getValueCount"
                            )
                    )
            );

            // for (int i = 0; i < recordCount; i++) { [insertionLoopBody] }
            ScalarVariableAccessPath insertionLoopIndexVariable =
                    new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
            Java.Block insertionLoopBody = new Java.Block(getLocation());
            codeGenResult.add(
                    createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), insertionLoopIndexVariable.getType()),
                                    insertionLoopIndexVariable.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            lt(
                                    getLocation(),
                                    insertionLoopIndexVariable.read(),
                                    recordCountAP.read()
                            ),
                            postIncrement(getLocation(), insertionLoopIndexVariable.write()),
                            insertionLoopBody
                    )
            );

            // Create the record in the loop body
            // int left_join_record_key = [buildKeyAVAP].get([i]);
            ScalarVariableAccessPath leftJoinRecordKeyAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("left_join_record_key"),
                    P_INT
            );
            insertionLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), leftJoinRecordKeyAP.getType()),
                            leftJoinRecordKeyAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    buildKeyAVAP.read(),
                                    "get",
                                    new Java.Rvalue[]{insertionLoopIndexVariable.read()}
                            )
                    )
            );

            // var left_join_record = new [leftRecordType]( [column values] )
            String recordVariableName = cCtx.defineVariable("left_join_record");
            Java.Type recordType = createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName());

            Java.Rvalue[] columnValues = new Java.Rvalue[currentOrdinalMapping.size()];
            for (int i = 0; i < columnValues.length; i++) {
                if (i == buildKeyOrdinal)
                    columnValues[i] = leftJoinRecordKeyAP.read();
                else
                    columnValues[i] = createMethodInvocation(
                            getLocation(),
                            ((ArrowVectorAccessPath) currentOrdinalMapping.get(i)).read(),
                            "get",
                            new Java.Rvalue[] { insertionLoopIndexVariable.read() }
                    );
            }

            insertionLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            recordType,
                            recordVariableName,
                            createClassInstance(
                                    getLocation(),
                                    recordType,
                                    columnValues
                            )
                    )
            );

            // Insert the record into the hash-table
            // [join_map].add([left_join_record_key], [preHashVector][[i]], [recordVariableName])
            insertionLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            this.joinMapAP.read(),
                            "add",
                            new Java.Rvalue[]{
                                    leftJoinRecordKeyAP.read(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            preHashVectorAP.read(),
                                            insertionLoopIndexVariable.read()
                                    ),
                                    createAmbiguousNameRef(getLocation(), recordVariableName)
                            }
                    )
            );

        } else if (buildKeyAPType == ARRAY_INT_VECTOR) {
            // We know that all other vectors on the build side must be of some array vector type
            // without any validity mask/selection vector
            ArrayVectorAccessPath buildKeyAVAP = ((ArrayVectorAccessPath) buildKeyAP);

            if (!this.useSIMDVec()) {
                // VectorisedHashOperators.constructPreHashKeyVector(
                //         [this.preHashVectorAP.read()],
                //         [buildKeyAVAP.getVectorVariable().read()]
                //         [buildKeyAVAP.getVectorLengthVariable().read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                "constructPreHashKeyVector",
                                new Java.Rvalue[]{
                                        this.preHashVectorAP.read(),
                                        buildKeyAVAP.getVectorVariable().read(),
                                        buildKeyAVAP.getVectorLengthVariable().read()
                                }
                        ));
            } else {
                // VectorisedHashOperators.constructPreHashKeyVectorSIMD(
                //         [this.preHashVectorAP.read()],
                //         [buildKeyAVAP.getVectorVariable().read()]
                //         [buildKeyAVAP.getVectorLengthVariable().read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                "constructPreHashKeyVectorSIMD",
                                new Java.Rvalue[]{
                                        this.preHashVectorAP.read(),
                                        buildKeyAVAP.getVectorVariable().read(),
                                        buildKeyAVAP.getVectorLengthVariable().read()
                                }
                        ));
            }

            // Now iterate over the key vector to construct the hash-table records and insert them into the table
            // for (int i = 0; i < [buildKeyAVAP.getVectorLengthVariable().read()]; i++) { [insertionLoopBody] }
            ScalarVariableAccessPath insertionLoopIndexVariable =
                    new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
            Java.Block insertionLoopBody = new Java.Block(getLocation());
            codeGenResult.add(
                    createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), insertionLoopIndexVariable.getType()),
                                    insertionLoopIndexVariable.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            lt(
                                    getLocation(),
                                    insertionLoopIndexVariable.read(),
                                    buildKeyAVAP.getVectorLengthVariable().read()
                            ),
                            postIncrement(getLocation(), insertionLoopIndexVariable.write()),
                            insertionLoopBody
                    )
            );

            // Create the record in the loop body
            // int left_join_record_key = [buildKeyAVAP.read()][i];
            ScalarVariableAccessPath leftJoinRecordKeyAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("left_join_record_key"),
                    P_INT
            );
            insertionLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), leftJoinRecordKeyAP.getType()),
                            leftJoinRecordKeyAP.getVariableName(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    buildKeyAVAP.getVectorVariable().read(),
                                    insertionLoopIndexVariable.read()
                            )
                    )
            );

            // var left_join_record = new [leftRecordType]( [column values] )
            String recordVariableName = cCtx.defineVariable("left_join_record");
            Java.Type recordType = createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName());

            Java.Rvalue[] columnValues = new Java.Rvalue[currentOrdinalMapping.size()];
            for (int i = 0; i < columnValues.length; i++) {
                ArrayVectorAccessPath columnAP = ((ArrayVectorAccessPath) currentOrdinalMapping.get(i));
                if (i == buildKeyOrdinal)
                    columnValues[i] = leftJoinRecordKeyAP.read();
                else
                    columnValues[i] = createArrayElementAccessExpr(
                            getLocation(),
                            columnAP.getVectorVariable().read(),
                            insertionLoopIndexVariable.read()
                    );
            }

            insertionLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            recordType,
                            recordVariableName,
                            createClassInstance(
                                    getLocation(),
                                    recordType,
                                    columnValues
                            )
                    )
            );

            // Insert the record into the hash-table
            // [join_map].add([left_join_record_key], [preHashVector][[i]], [recordVariableName])
            insertionLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            this.joinMapAP.read(),
                            "add",
                            new Java.Rvalue[] {
                                    leftJoinRecordKeyAP.read(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            preHashVectorAP.read(),
                                            insertionLoopIndexVariable.read()
                                    ),
                                    createAmbiguousNameRef(getLocation(), recordVariableName)
                            }
                    )
            );

        } else {
            throw new UnsupportedOperationException(
                    "JoinOperator.consumeVecBuild does not support this key column access path type: " + buildKeyAPType);
        }

        return codeGenResult;
    }

    /**
     * Method for generating the code that performs the hash-table probe in the vectorised paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @return The generated query code.
     */
    private List<Java.Statement> consumeVecProbe(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codeGenResult = new ArrayList<>();

        // Check that we can handle the key type (casts valid due to pre-condition check in constructor)
        RexCall joinCondition = (RexCall) this.getLogicalSubplan().getCondition();
        RexInputRef buildKey = (RexInputRef) joinCondition.getOperands().get(1);
        // Need to convert the output ordinal to the right-input ordinal
        int buildKeyOrdinal = buildKey.getIndex() - this.getLogicalSubplan().getLeft().getRowType().getFieldCount();
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        AccessPath buildKeyAP = currentOrdinalMapping.get(buildKeyOrdinal);
        QueryVariableType buildKeyAPType = buildKeyAP.getType();
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeVecProbe only supports integer join key columns");

        // First perform the pre-hashing of the key column and then perform hash-table inserts
        if (buildKeyAPType == ARROW_INT_VECTOR) {
            // We know that all other vectors on the probe side must be of some arrow vector type
            // without any validity mask/selection vector
            ArrowVectorAccessPath buildKeyAVAP = ((ArrowVectorAccessPath) buildKeyAP);

            if (!this.useSIMDVec()) {
                // VectorisedHashOperators.constructPreHashKeyVector([this.preHashVectorAP.read()], [buildKeyAVAP.read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                "constructPreHashKeyVector",
                                new Java.Rvalue[]{
                                        this.preHashVectorAP.read(),
                                        buildKeyAVAP.read()
                                }
                        ));
            } else {
                // VectorisedHashOperators.constructPreHashKeyVectorSIMD([this.preHashVectorAP.read()], [buildKeyAVAP.read()]);
                codeGenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                                "constructPreHashKeyVectorSIMD",
                                new Java.Rvalue[]{
                                        this.preHashVectorAP.read(),
                                        buildKeyAVAP.read()
                                }
                        ));
            }

            // Now we need to iterate over the key vector to construct the result vectors by probing the
            // hash table and reading the other vectors on the probe side
            // int recordCount = [buildKeyAVAP.read()].getValueCount();
            ScalarVariableAccessPath recordCountAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("recordCount"),
                    P_INT
            );
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveType(getLocation(), Java.Primitive.INT),
                            recordCountAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    buildKeyAVAP.read(),
                                    "getValueCount"
                            )
                    )
            );

            // int currentRecordIndex = 0;
            ScalarVariableAccessPath currentRecordIndexAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("currentRecordIndex"),
                    P_INT
            );
            codeGenResult.add(
                    createPrimitiveLocalVar(
                            getLocation(),
                            Java.Primitive.INT,
                            currentRecordIndexAP.getVariableName(),
                            "0"
                    )
            );

            // while (currentRecordIndex < recordCount) { [whileLoopBody] }
            Java.Block whileLoopBody = new Java.Block(getLocation());
            codeGenResult.add(
                    createWhileLoop(
                            getLocation(),
                            lt(getLocation(), currentRecordIndexAP.read(), recordCountAP.read()),
                            whileLoopBody
                    )
            );

            // In the while-loop body, iterate over the keys and insert as many join records while
            // there is still room left, via a nested loop
            // int currentResultIndex = 0;
            ScalarVariableAccessPath currentResultIndexAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("currentResultIndex"),
                    P_INT
            );
            whileLoopBody.addStatement(
                    createPrimitiveLocalVar(
                            getLocation(),
                            Java.Primitive.INT,
                            currentResultIndexAP.getVariableName(),
                            "0"
                    )
            );

            // Add the inner loop which will insert as many records as possible into the result vector
            // while (currentRecordIndex < recordCount) { [resultVectorConstructionLoop] }
            Java.Block resultVectorConstructionLoop = new Java.Block(getLocation());
            whileLoopBody.addStatement(
                    createWhileLoop(
                            getLocation(),
                            lt(getLocation(), currentRecordIndexAP.read(), recordCountAP.read()),
                            resultVectorConstructionLoop
                    )
            );

            // int right_join_key = [buildKeyAVAP.read()].get([currentRecordIndex.read()]);
            ScalarVariableAccessPath rightJoinKeyAP =
                    new ScalarVariableAccessPath(cCtx.defineVariable("right_join_key"), P_INT);
            resultVectorConstructionLoop.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), rightJoinKeyAP.getType()),
                            rightJoinKeyAP.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    buildKeyAVAP.read(),
                                    "get",
                                    new Java.Rvalue[] {
                                            currentRecordIndexAP.read()
                                    }
                            )
                    )
            );

            // long right_join_key_pre_hash = [this.preHashVectorAP.read()][[currentRecordIndex.read()]];
            ScalarVariableAccessPath rightJoinKeyPreHashAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable(rightJoinKeyAP.getVariableName() + "_pre_hash"),
                    P_LONG
            );
            resultVectorConstructionLoop.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), rightJoinKeyPreHashAP.getType()),
                            rightJoinKeyPreHashAP.getVariableName(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    this.preHashVectorAP.read(),
                                    currentRecordIndexAP.read()
                            )
                    )
            );

            // Check if the left side of the join contains records for the given key, otherwise continue
            // if (![this.joinMap.read()].contains(key, prehash)) {
            //     currentRecordIndex++;
            //     continue;
            // }
            Java.Block incrementAndContinueBlock = new Java.Block(getLocation());
            incrementAndContinueBlock.addStatement(
                    postIncrementStm(getLocation(), currentRecordIndexAP.write()));
            incrementAndContinueBlock.addStatement(new Java.ContinueStatement(getLocation(), null));

            resultVectorConstructionLoop.addStatement(
                    createIf(
                            getLocation(),
                            not(
                                    getLocation(),
                                    createMethodInvocation(
                                            getLocation(),
                                            this.joinMapAP.read(),
                                            "contains",
                                            new Java.Rvalue[] {
                                                    rightJoinKeyAP.read(),
                                                    rightJoinKeyPreHashAP.read()
                                            }
                                    )
                            ),
                            incrementAndContinueBlock
                    )
            );

            // Obtain the values for the left side of the join for the current key value
            String joinRecordsListName = cCtx.defineVariable("records_to_join_list");
            resultVectorConstructionLoop.addStatement(
                    createLocalVariable(
                            getLocation(),
                            createReferenceType(getLocation(), "ArrayList", this.leftChildRecordType.lcd.getName()),
                            joinRecordsListName,
                            createMethodInvocation(
                                    getLocation(),
                                    this.joinMapAP.read(),
                                    "get",
                                    new Java.Rvalue[] {
                                            rightJoinKeyAP.read(),
                                            rightJoinKeyPreHashAP.read()
                                    }
                            )
                    )
            );

            // int left_join_record_count = records_to_join_list.size();
            ScalarVariableAccessPath leftJoinRecordCount =
                    new ScalarVariableAccessPath(cCtx.defineVariable("left_join_record_count"), P_INT);
            resultVectorConstructionLoop.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), leftJoinRecordCount.getType()),
                            leftJoinRecordCount.getVariableName(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), joinRecordsListName),
                                    "size"
                            )
                    )
            );

            // Check if there is still room in the result vector and if not, break from the inner loop
            // if (leftJoinRecordCount > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))
            //     break;
            Java.Block breakBlock = new Java.Block(getLocation());
            breakBlock.addStatement(new Java.BreakStatement(getLocation(), null));
            resultVectorConstructionLoop.addStatement(
                    createIf(
                            getLocation(),
                            gt(
                                    getLocation(),
                                    leftJoinRecordCount.read(),
                                    sub(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "VectorisedOperators.VECTOR_LENGTH"),
                                            currentResultIndexAP.read()
                                    )
                            ),
                            breakBlock
                    )
            );

            // There is still room left, so add the records for the current key
            // Firstly, set-up local variables for the right-hand column values
            Java.Rvalue[] rightJoinOrdinalValues = new Java.Rvalue[currentOrdinalMapping.size()];
            for (int i = 0; i < rightJoinOrdinalValues.length; i++) {
                if (i == buildKeyOrdinal) {
                    rightJoinOrdinalValues[i] = rightJoinKeyAP.read();

                } else {
                    ArrowVectorAccessPath rightJoinColumnAP = (ArrowVectorAccessPath) currentOrdinalMapping.get(i);
                    ScalarVariableAccessPath rightJoinColumnValue = new ScalarVariableAccessPath(
                            cCtx.defineVariable("right_join_ord_" + i),
                            primitiveType(rightJoinColumnAP.getType())
                    );

                    resultVectorConstructionLoop.addStatement(
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), rightJoinColumnValue.getType()),
                                    rightJoinColumnValue.getVariableName(),
                                    createMethodInvocation(
                                            getLocation(),
                                            rightJoinColumnAP.read(),
                                            "get",
                                            new Java.Rvalue[] { currentRecordIndexAP.read() }
                                    )
                            )
                    );

                    rightJoinOrdinalValues[i] = rightJoinColumnValue.read();
                }
            }

            // for (int i = 0; i < left_join_record_count; i++) {
            //     [RecordType] left_join_record = records_to_join_list.get(i);
            //     [joinLoopBody]
            // }
            ScalarVariableAccessPath joinLoopIndexVar =
                    new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
            Java.Block joinLoopBody = new Java.Block(getLocation());
            resultVectorConstructionLoop.addStatement(
                    createForLoop(
                            getLocation(),
                            createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, joinLoopIndexVar.getVariableName(), "0"),
                            lt(getLocation(), joinLoopIndexVar.read(), leftJoinRecordCount.read()),
                            postIncrement(getLocation(), joinLoopIndexVar.write()),
                            joinLoopBody
                    )
            );

            String leftJoinRecordName = cCtx.defineVariable("left_join_record");
            Java.Type leftJoinRecordType = createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName());
            joinLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            leftJoinRecordType,
                            leftJoinRecordName,
                            createCast(
                                    getLocation(),
                                    leftJoinRecordType,
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), joinRecordsListName),
                                            "get",
                                            new Java.Rvalue[] {joinLoopIndexVar.read() }
                                    )
                            )
                    )
            );

            // In the loop over the left join records, first add the statements to set the correct
            // values in the result vectors for the left side join columns
            List< Java.FieldDeclarationOrInitializer> leftJoinFields =
                    this.leftChildRecordType.lcd.fieldDeclarationsAndInitializers;
            for (int i = 0; i < leftJoinFields.size(); i++) {
                Java.FieldDeclaration currentLeftSideField = (Java.FieldDeclaration) leftJoinFields.get(i);

                joinLoopBody.addStatement(
                        createVariableAssignmentStm(
                                getLocation(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        this.resultVectorDefinitions[i].read(),
                                        currentResultIndexAP.read()
                                ),
                                createAmbiguousNameRef(
                                        getLocation(),
                                        leftJoinRecordName + "." + currentLeftSideField.variableDeclarators[0].name
                                )
                        )
                );
            }

            // And then set the correct values for the right join columns
            for (int i = 0; i < rightJoinOrdinalValues.length; i++) {
                int resultIndex = leftJoinFields.size() + i;
                joinLoopBody.addStatement(
                        createVariableAssignmentStm(
                                getLocation(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        this.resultVectorDefinitions[resultIndex].read(),
                                        currentResultIndexAP.read()
                                ),
                                rightJoinOrdinalValues[i]
                        )
                );
            }

            // And move onto the next result index
            joinLoopBody.addStatement(postIncrementStm(getLocation(), currentResultIndexAP.write()));
            // End of the join-loop

            // Finally, move to the next record
            // currentRecordIndex++;
            resultVectorConstructionLoop.addStatement(
                    postIncrementStm(getLocation(), currentRecordIndexAP.write()));

            // End of the inner result vector construction loop
            // Set-up the correct ordinal mapping
            List<AccessPath> resultVectorAPs = new ArrayList<>(this.resultVectorDefinitions.length);
            for (int i = 0; i < this.resultVectorDefinitions.length; i++) {
                AccessPath resultVectorAP = new ArrayVectorAccessPath(
                        this.resultVectorDefinitions[i],
                        currentResultIndexAP,
                        vectorTypeForPrimitiveArrayType(this.resultVectorDefinitions[i].getType())
                );
                resultVectorAPs.add(i, resultVectorAP);
            }
            cCtx.setCurrentOrdinalMapping(resultVectorAPs);

            // Invoke the parent
            whileLoopBody.addStatements(this.vecParentConsume(cCtx, oCtx));

        }

        return codeGenResult;
    }

    /**
     * General consume method used by both the vectorised and non-vectorised execution paradigm.
     * @param cCtx The {@link CodeGenContext} to use during the generation.
     * @param oCtx The {@link OptimisationContext} to use during the generation and execution.
     * @param vectorised Whether to use the vectorised or non-vectorised paradigm.
     * @return The generated consumption code.
     */
    private List<Java.Statement> consume(CodeGenContext cCtx, OptimisationContext oCtx, boolean vectorised) {
        if (!this.consumeInProbePhase) {
            // Mark that on the next call, we are in the probe phase
            this.consumeInProbePhase = true;

            // Then introduce the class to store records of the left-child in the hash-table
            this.leftChildRecordType = generateRecordTypeForRelation(cCtx.getCurrentOrdinalMapping());

            // And build the hash table
            return vectorised ? this.consumeVecBuild(cCtx, oCtx) : this.consumeNonVecBuild(cCtx, oCtx);

        } else {
            // Perform the probe (which also has the parent operator consume the result)
            return vectorised ? this.consumeVecProbe(cCtx, oCtx) : this.consumeNonVecProbe(cCtx, oCtx);

        }
    }

    /**
     * Method to generate a "record-like" type for storing the elements of an input relation in a hash-table.
     * @param relationType The input relation for which the record type should be generated.
     * @return Code representing a record class for the provided {@code relationType}.
     */
    private Java.LocalClassDeclarationStatement generateRecordTypeForRelation(List<AccessPath> relationType) {
        // Create a class for the record type
        Java.LocalClassDeclaration recordTypeClassDeclaration = createLocalClassDeclaration(
                getLocation(),
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PRIVATE.toString(), getLocation()),
                        new Java.AccessModifier("final", getLocation())
                },
                "JRT_" + Math.abs(relationType.hashCode())
        );

        // Add a field to the class for each column in the relation type
        // Additionally, prepare the formal parameters and assignment statements for the constructor
        Java.FunctionDeclarator.FormalParameter[] constructorParameters = new Java.FunctionDeclarator.FormalParameter[relationType.size()];
        List<Java.Statement> constructorAssignmentStatements = new ArrayList<>(relationType.size());

        for (int i = 0; i < relationType.size(); i++) {
            AccessPath columnAP = relationType.get(i);

            QueryVariableType fieldPrimitiveType = primitiveType(columnAP.getType());
            String fieldName = "ord_" + i;

            recordTypeClassDeclaration.addFieldDeclaration(
                createPublicFinalFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), fieldPrimitiveType),
                        createSimpleVariableDeclaration(getLocation(), fieldName)
                )
            );

            constructorParameters[i] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), fieldPrimitiveType),
                    fieldName);

            constructorAssignmentStatements.add(
                    i,
                    createVariableAssignmentStm(
                            getLocation(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    new Java.ThisReference(getLocation()),
                                    fieldName
                            ),
                            createAmbiguousNameRef(getLocation(), fieldName)
                    )
            );

        }

        // Next, create the constructor to initialise each field
        JaninoMethodGen.createConstructor(
                getLocation(),
                recordTypeClassDeclaration,
                Access.PUBLIC,
                createFormalParameters(
                        getLocation(),
                        constructorParameters
                ),
                null,
                constructorAssignmentStatements
        );

        // Return the created class declaration statement
        return createLocalClassDeclarationStm(recordTypeClassDeclaration);
    }

}
