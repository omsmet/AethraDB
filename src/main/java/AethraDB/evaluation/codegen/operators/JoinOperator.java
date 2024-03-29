package AethraDB.evaluation.codegen.operators;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoControlGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen;
import AethraDB.evaluation.general_support.hashmaps.KeyMultiRecordMapGenerator;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_INT_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARRAY_VARCHAR_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_INT_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.ARROW_VARCHAR_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.LogicalType.ARRAY_FIXED_LENGTH_BINARY_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.LogicalType.ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.MAP_GENERATED;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_A_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_A_VARCHAR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_VARCHAR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.vectorTypeForPrimitiveArrayType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclarationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createPrimitiveLocalVar;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * {@link CodeGenOperator} which performs a join over two tables for a given join condition. All
 * joins implemented by this operator are currently hash-joins over a single equality predicate.
 */
public class JoinOperator extends CodeGenOperator {

    /**
     * The {@link CodeGenOperator} producing the records to be joined by {@code this} on "left" side
     * of the join.
     */
    private final CodeGenOperator leftChild;

    /**
     * The index (in the result) of the left child ordinal to use for the equi-join condition.
     */
    private final int leftChildEquijoinIndex;

    /**
     * The number of columns in the left-child records.
     */
    private int leftChildColumnCount;

    /**
     * The {@link CodeGenOperator} producing the records to be joined by {@code this} on "right" side
     * of the join.
     */
    private final CodeGenOperator rightChild;

    /**
     * The index (in the result) of the right child ordinal to use for the equi-join condition.
     */
    private final int rightChildEquijoinIndex;

    /**
     * The generator used for creating the hash-map type that will be used to store the
     * records of the left child during the build stage of the hash-join.
     */
    private KeyMultiRecordMapGenerator joinMapGenerator;

    /**
     * The access path to the hash-map variable used for performing the join.
     */
    private MapAccessPath joinMapAP;

    /**
     * The access path to the pre-hash vector variable used for performing the join.
     */
    private ArrayAccessPath preHashVectorAP;

    /**
     * The total number of columns in the result.
     */
    private int resultColumnCount;

    /**
     * The names of the result vectors of this operator in the vectorised paradigm.
     */
    private ArrayList<String> resultVectorNames;

    /**
     * The access paths indicating the vector types that should be exposed as the result of this
     * operator in the vectorised paradigm.
     */
    private ArrayList<ArrayAccessPath> resultVectorDefinitions;

    /**
     * Boolean indicating if the consume method should perform a hash-table build or a hash-table probe.
     * Will be initialised as false to indicate a hash-table build first.
     */
    private boolean consumeInProbePhase;

    /**
     * Creates a new {@link JoinOperator} instance for a specific sub-query.
     * @param leftChild The {@link CodeGenOperator} producing the left input side of the join.
     * @param rightChild The {@link CodeGenOperator} producing the right input side of the join.
     * @param leftJoinColumnIndex The index (in the result) of the left child to use in the equi-join condition.
     * @param rightJoinColumnIndex The index (in the result) of the right child to use in the equi-join condition.
     */
    public JoinOperator(
            CodeGenOperator leftChild,
            CodeGenOperator rightChild,
            int leftJoinColumnIndex,
            int rightJoinColumnIndex
    ) {
        // Pre-conditions checked by planner library
        this.leftChild = leftChild;
        this.leftChild.setParent(this);
        this.leftChildEquijoinIndex = leftJoinColumnIndex;
        this.rightChild = rightChild;
        this.rightChild.setParent(this);
        this.rightChildEquijoinIndex = rightJoinColumnIndex;
        this.consumeInProbePhase = false;

        this.resultVectorNames = new ArrayList<>();
        this.resultVectorDefinitions = new ArrayList<>();
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
        this.joinMapAP = new MapAccessPath(cCtx.claimGlobalVariableName("join_map"), MAP_GENERATED);

        // First build the hash-table by calling the produceNonVec method on the left child operator,
        // which will eventually invoke the consumeNonVec method on @this which should perform the
        // hash-table build.
        // Additionally, the consumeNonVec method will initialise the join map type for the hash table
        // which will have to be added to the codeGenResult first, after which we initialise the
        // actual map used for the join.
        cCtx.pushCodeGenContext();
        List<Java.Statement> leftChildProduceResult = this.leftChild.produceNonVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        codeGenResult.add(
                createLocalClassDeclarationStm(this.joinMapGenerator.generate()));
        Java.Type javaJoinMapType =
                createReferenceType(getLocation(), this.joinMapGenerator.generate().getName());
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        javaJoinMapType,
                        this.joinMapAP.getVariableName(),
                        createClassInstance(getLocation(), javaJoinMapType)
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
        int buildKeyOrdinal = this.leftChildEquijoinIndex;
        AccessPath buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecBuild only supports integer join key columns");

        if (this.useSIMDNonVec(cCtx))
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecBuild no longer supports SIMD");

        // Idea: first perform hashing of the key-column, then perform hash-table inserts.
        // Obtain the local access path for the key column value
        Java.Rvalue keyColumnRvalue = getRValueFromOrdinalAccessPathNonVec(cCtx, buildKeyOrdinal, codeGenResult);
        buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);

        // Compute the pre-hash value in a local variable
        ScalarVariableAccessPath leftJoinKeyPreHash = new ScalarVariableAccessPath(cCtx.defineVariable("left_join_key_prehash"), P_LONG);
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

        // Get the values for the current row (except for the key, as we dont need duplication)
        var currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        Java.Rvalue[] recordColumnValues = new Java.Rvalue[currentOrdinalMapping.size() - 1];
        int currentRecordColumnValueIndex = 0;
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            if (i == buildKeyOrdinal)
                continue;
            recordColumnValues[currentRecordColumnValueIndex++] = getRValueFromOrdinalAccessPathNonVec(cCtx, i, codeGenResult);
        }

        // Insert the record for the current row into the join map
        Java.Rvalue[] associateCallArguments = new Java.Rvalue[recordColumnValues.length + 2];
        associateCallArguments[0] = ((ScalarVariableAccessPath) buildKeyAP).read(); // Cast valid due to local access path and flattening
        associateCallArguments[1] = leftJoinKeyPreHash.read();
        System.arraycopy(recordColumnValues, 0, associateCallArguments, 2, recordColumnValues.length);

        codeGenResult.add(
                createMethodInvocationStm(
                        getLocation(),
                        this.joinMapAP.read(),
                        "associate",
                        associateCallArguments
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
        // Need to convert the output ordinal to the right-input ordinal
        int probeKeyOrdinal = this.rightChildEquijoinIndex - this.leftChildColumnCount;
        AccessPath probeKeyAP = cCtx.getCurrentOrdinalMapping().get(probeKeyOrdinal);
        QueryVariableType probeKeyOrdinalPrimitiveType = primitiveType(probeKeyAP.getType());

        if (probeKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecProbe only supports integer join key columns");

        if (this.useSIMDNonVec(cCtx))
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecProbe no longer supports SIMD");

        // Idea: first perform hashing of the key-column, then look up the join records in the hash-table
        // and finally expose the resulting records

        // Obtain the local access path for the key column value
        Java.Rvalue keyColumnRvalue = getRValueFromOrdinalAccessPathNonVec(cCtx, probeKeyOrdinal, codeGenResult);
        probeKeyAP = cCtx.getCurrentOrdinalMapping().get(probeKeyOrdinal);

        // Compute the pre-hash value in a local variable
        var rightJoinKeyPreHash = new ScalarVariableAccessPath(cCtx.defineVariable("right_join_key_prehash"), P_LONG);
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
        List<AccessPath> rightHandOrdinalMappings = cCtx.getCurrentOrdinalMapping();
        List<Java.Statement> localRightHandAPsInitialisationStatements = new ArrayList<>();
        List<AccessPath> localRightHandAPs = new ArrayList<>(rightHandOrdinalMappings.size());
        for (int i = 0; i < rightHandOrdinalMappings.size(); i++) {
            getRValueFromOrdinalAccessPathNonVec(cCtx, i, localRightHandAPsInitialisationStatements);
            localRightHandAPs.add(i, cCtx.getCurrentOrdinalMapping().get(i));
        }

        // Obtain the index for the values for the left side of the join for the current key value
        ScalarVariableAccessPath joinRecordIndex = new ScalarVariableAccessPath(
                cCtx.defineVariable("records_to_join_index"),
                P_INT);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), joinRecordIndex.getType()),
                        joinRecordIndex.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                this.joinMapAP.read(),
                                "getIndex",
                                new Java.Rvalue[] {
                                        ((ScalarVariableAccessPath) probeKeyAP).read(), // Cast valid due to local access path
                                        rightJoinKeyPreHash.read()
                                }
                        )
                )
        );

        // Use the index to check if the left side of the join contains records for the given key
        // otherwise continue
        // if ([records_to_join_index] == -1) continue;
        codeGenResult.add(
                JaninoControlGen.createIf(
                        getLocation(),
                        JaninoOperatorGen.eq(
                                getLocation(),
                                joinRecordIndex.read(),
                                createIntegerLiteral(getLocation(), -1)
                        ),
                        new Java.ContinueStatement(getLocation(), null)
                )
        );

        // Get the column values for the right-hand side of the join in local variables
        codeGenResult.addAll(localRightHandAPsInitialisationStatements);

        // int left_join_record_count = [joinMapAP].keysRecordCount[records_to_join_index];
        ScalarVariableAccessPath leftJoinRecordCount =
                new ScalarVariableAccessPath(cCtx.defineVariable("left_join_record_count"), P_INT);
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), leftJoinRecordCount.getType()),
                        leftJoinRecordCount.getVariableName(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        joinMapAP.read(),
                                        KeyMultiRecordMapGenerator.keysRecordCountAP.getVariableName()
                                ),
                                joinRecordIndex.read()
                        )
                )
        );

        // for (int i = 0; i < left_join_record_count; i++) {
        //     [joinLoopBody]
        // }
        ScalarVariableAccessPath joinLoopIndexVar =
                new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
        Java.Block joinLoopBody = new Java.Block(getLocation());
        codeGenResult.add(
                JaninoControlGen.createForLoop(
                        getLocation(),
                        createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, joinLoopIndexVar.getVariableName(), "0"),
                        JaninoOperatorGen.lt(getLocation(), joinLoopIndexVar.read(), leftJoinRecordCount.read()),
                        JaninoOperatorGen.postIncrement(getLocation(), joinLoopIndexVar.write()),
                        joinLoopBody
                )
        );

        // In the for-loop, expose the left-hand join columns as local variables
        // Also start updating the ordinal mapping
        List<AccessPath> updatedOrdinalMapping = new ArrayList<>(this.resultColumnCount);

        // JoinMapType.ValueRecordType left_join_rec = joinMap.records[joinRecordIndex][joinLoopIndexVar];
        String leftJoinRec = cCtx.defineVariable("left_join_rec");
        if (this.joinMapGenerator.valueFieldNames.length > 0) {
            joinLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            new Java.ReferenceType(
                                    getLocation(),
                                    new Java.Annotation[0],
                                    new String[] {
                                            this.joinMapGenerator.mapDeclaration.name,
                                            this.joinMapGenerator.valueRecordDeclaration.name
                                    },
                                    null
                            ),
                            leftJoinRec,
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            new Java.FieldAccessExpression(
                                                    getLocation(),
                                                    joinMapAP.read(),
                                                    KeyMultiRecordMapGenerator.valueRecordArrayName
                                            ),
                                            joinRecordIndex.read()
                                    ),
                                    joinLoopIndexVar.read()
                            )
                    )
            );
        }

        int numberOfLhsColumns = this.joinMapGenerator.valueFieldNames.length + 1; // add 1 for key column
        int currentLhsJoinMapValueColumnIndex = 0; // Need to account for the fact that the key is not duplicated in the map
        for (int i = 0; i < numberOfLhsColumns; i++) {

            // For the key, simply use the existing ordinal value
            if (i == this.leftChildEquijoinIndex) {
                updatedOrdinalMapping.add(i, probeKeyAP);
                continue;
            }

            // For a value column, we need to generate an access path
            ScalarVariableAccessPath currentLeftSideColumnVar =
                    new ScalarVariableAccessPath(
                            cCtx.defineVariable("left_join_ord_" + currentLhsJoinMapValueColumnIndex),
                            this.joinMapGenerator.valueTypes[currentLhsJoinMapValueColumnIndex]
                    );
            updatedOrdinalMapping.add(i, currentLeftSideColumnVar);

            // Create the variable referred to by the access path
            joinLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), currentLeftSideColumnVar.getType()),
                            currentLeftSideColumnVar.getVariableName(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), leftJoinRec),
                                    this.joinMapGenerator.valueFieldNames[currentLhsJoinMapValueColumnIndex]
                            )
                    )
            );

            // Go to the next value column
            currentLhsJoinMapValueColumnIndex++;
        }

        // Update the right-hand part of the ordinal mapping
        for (int i = 0; i < localRightHandAPs.size(); i++) {
            updatedOrdinalMapping.add(numberOfLhsColumns + i, localRightHandAPs.get(i));
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
        this.joinMapAP = new MapAccessPath(cCtx.claimGlobalVariableName("join_map"), MAP_GENERATED);

        // For vectorised implementations, allocate a pre-hash vector
        this.preHashVectorAP = new ArrayAccessPath(cCtx.claimGlobalVariableName("pre_hash_vector"), P_A_LONG);
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
        // Additionally, the consumeVec method will initialise the join map type for the hash table and
        // prepare the left-hand side of the result vector type initialisation.
        cCtx.pushCodeGenContext();
        List<Java.Statement> leftChildProduceResult = this.leftChild.produceVec(cCtx, oCtx);
        cCtx.popCodeGenContext();

        // We first add the join map type to the codegen result, and initialise an instance
        codeGenResult.add(createLocalClassDeclarationStm(this.joinMapGenerator.generate()));

        Java.Type javaJoinMapType =
                createReferenceType(getLocation(), this.joinMapGenerator.generate().getName());
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        javaJoinMapType,
                        this.joinMapAP.getVariableName(),
                        createClassInstance(getLocation(), javaJoinMapType)
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
        for (int i = 0; i < this.resultVectorDefinitions.size(); i++) {
            ArrayAccessPath vectorDescription = this.resultVectorDefinitions.get(i);
            String instantiationMethod = switch (vectorDescription.getType().logicalType) {
                case P_A_BOOLEAN -> "getBooleanVector";
                case P_A_DOUBLE -> "getDoubleVector";
                case P_A_INT, P_A_INT_DATE -> "getIntVector";
                case P_A_LONG -> "getLongVector";
                case S_A_FL_BIN, S_A_VARCHAR -> "getNestedByteVector";
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
        for (int i = 0; i < this.resultVectorDefinitions.size(); i++) {
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createMethodInvocation(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "release",
                            new Java.Rvalue[] { this.resultVectorDefinitions.get(i).read() }
                    )
            );
        }

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        // Set-up the current part of the result vector type initialisation
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        int vectorOffset = consumeInProbePhase ? this.leftChildColumnCount : 0;
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            int outputVectorIndex = vectorOffset + i;
            QueryVariableType ordinalType = currentOrdinalMapping.get(i).getType();

            QueryVariableType primitiveOrdinalType;
            QueryVariableType primitiveArrayType;
            if (ordinalType.logicalType == ARROW_FIXED_LENGTH_BINARY_VECTOR || ordinalType.logicalType == ARRAY_FIXED_LENGTH_BINARY_VECTOR
                || ordinalType.logicalType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR) {
                primitiveOrdinalType = new QueryVariableType(QueryVariableType.LogicalType.S_FL_BIN, ordinalType.byteWidth);
                primitiveArrayType = new QueryVariableType(QueryVariableType.LogicalType.S_A_FL_BIN, ordinalType.byteWidth);
            } else if (ordinalType == ARROW_VARCHAR_VECTOR || ordinalType == ARRAY_VARCHAR_VECTOR) {
                primitiveOrdinalType = S_VARCHAR;
                primitiveArrayType = S_A_VARCHAR;
            } else {
                primitiveOrdinalType = primitiveType(currentOrdinalMapping.get(i).getType());
                primitiveArrayType = primitiveArrayTypeForPrimitive(primitiveOrdinalType);
            }

            this.resultVectorNames.add(cCtx.claimGlobalVariableName("join_result_vector_ord_" + outputVectorIndex));
            this.resultVectorDefinitions.add(
                    new ArrayAccessPath(
                            this.resultVectorNames.get(outputVectorIndex),
                            primitiveArrayType
                    ));
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

        if (this.useSIMDVec())
            throw new UnsupportedOperationException("JoinOperator.consumeVecBuild no longer supports SIMD");

        // Check that we can handle the key type (casts valid due to pre-condition check in constructor)
        int buildKeyOrdinal = this.leftChildEquijoinIndex;
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        AccessPath buildKeyAP = currentOrdinalMapping.get(buildKeyOrdinal);
        QueryVariableType buildKeyAPType = buildKeyAP.getType();
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeVecBuild only supports integer join key columns");

        // Idea: first perform pre-hashing of the key-column and then perform hash-table inserts
        if (buildKeyAPType == ARROW_INT_VECTOR) {
            // We know that all other vectors on the build side must be of some arrow/array vector type
            // without any validity mask/selection vector
            ArrowVectorAccessPath buildKeyAVAP = ((ArrowVectorAccessPath) buildKeyAP);

            // VectorisedHashOperators.constructPreHashKeyVectorInit([this.preHashVectorAP.read()], [buildKeyAVAP.read()]);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVectorInit",
                            new Java.Rvalue[]{
                                    this.preHashVectorAP.read(),
                                    buildKeyAVAP.read()
                            }
                    ));

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
                    JaninoControlGen.createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), insertionLoopIndexVariable.getType()),
                                    insertionLoopIndexVariable.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            JaninoOperatorGen.lt(
                                    getLocation(),
                                    insertionLoopIndexVariable.read(),
                                    recordCountAP.read()
                            ),
                            JaninoOperatorGen.postIncrement(getLocation(), insertionLoopIndexVariable.write()),
                            insertionLoopBody
                    )
            );

            // Insert the record into the hash-table
            // Get the key value (no need to optimise for fixed-length binary values, since we only support ints here)
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
                                    new Java.Rvalue[] { insertionLoopIndexVariable.read() }
                            )
                    )
            );

            // Get the values for the remaining columns to prevent key duplication
            // Need to potentially optimise for fixed-length binary and varchar columns
            Java.Rvalue[] columnValues = new Java.Rvalue[currentOrdinalMapping.size() - 1];
            int currentValueColumnIndex = 0;
            for (int i = 0; i < currentOrdinalMapping.size(); i++) {
                if (i == buildKeyOrdinal)
                    continue;

                ArrowVectorAccessPath currentOrdinalAP = (ArrowVectorAccessPath) currentOrdinalMapping.get(i);
                QueryVariableType.LogicalType currentOrdinalAPLogicalType = currentOrdinalAP.getType().logicalType;

                if (currentOrdinalAPLogicalType == QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR) {
                    // Allocate a global byte array cache variable to avoid allocations
                    String byteArrayCacheName = cCtx.defineQueryGlobalVariable(
                            "byte_array_cache",
                            JaninoGeneralGen.createPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                            new Java.NewArray(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                                    new Java.Rvalue[] {
                                            JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), currentOrdinalAP.getType().byteWidth)
                                    },
                                    0
                            ),
                            false);

                    // Perform an optimised read (which will write the appropriate value into the cache array
                    insertionLoopBody.addStatement(
                            createMethodInvocationStm(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), "ArrowOptimisations"),
                                    "getFixedSizeBinaryValue",
                                    new Java.Rvalue[] {
                                            currentOrdinalAP.read(),
                                            insertionLoopIndexVariable.read(),
                                            createAmbiguousNameRef(getLocation(), byteArrayCacheName)
                                    }
                            ));

                    // Set the column value to read from the byte cache
                    columnValues[currentValueColumnIndex++] = createAmbiguousNameRef(
                            JaninoGeneralGen.getLocation(),
                            byteArrayCacheName
                    );

                } else if (currentOrdinalAPLogicalType == QueryVariableType.LogicalType.ARROW_VARCHAR_VECTOR) {
                    // Allocate an array of global byte array caches to avoid allocations as much as possible
                    // while keeping the var-charity. The array has space for one byte array cache upto the
                    // maximum var char length in the database
                    int maximumVarCharLength = 200; // TODO: find way to not hard-code this
                    String arrayOfCachesName = cCtx.defineQueryGlobalVariable(
                            "byte_array_caches",
                            JaninoGeneralGen.createNestedPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                            JaninoGeneralGen.createNew2DPrimitiveArray(
                                    JaninoGeneralGen.getLocation(),
                                    Java.Primitive.BYTE,
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), maximumVarCharLength)
                            ),
                            false
                    );

                    // Perform an optimised read and assign the returned byte array to a local reference variable
                    String localColumnVariableName = cCtx.defineVariable("var_char_cached_value");
                    insertionLoopBody.addStatement(
                            JaninoVariableGen.createLocalVariable(
                                    JaninoGeneralGen.getLocation(),
                                    QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), S_VARCHAR),
                                    localColumnVariableName,
                                    createMethodInvocation(
                                            getLocation(),
                                            createAmbiguousNameRef(getLocation(), "ArrowOptimisations"),
                                            "getVarCharBinaryValue",
                                            new Java.Rvalue[] {
                                                    currentOrdinalAP.read(),
                                                    insertionLoopIndexVariable.read(),
                                                    createAmbiguousNameRef(getLocation(), arrayOfCachesName)
                                            }
                                    )
                            )
                    );

                    // Set the column value to read from the byte cache
                    columnValues[currentValueColumnIndex++] = createAmbiguousNameRef(
                            JaninoGeneralGen.getLocation(),
                            localColumnVariableName
                    );

                } else {
                    columnValues[currentValueColumnIndex++] = createMethodInvocation(
                            getLocation(),
                            ((ArrowVectorAccessPath) currentOrdinalMapping.get(i)).read(),
                            "get",
                            new Java.Rvalue[] { insertionLoopIndexVariable.read() }
                    );

                }
            }

            // [join_map].associate([left_join_record_key], [preHashVector][[i]], [columnValues])
            Java.Rvalue[] associateCallArgs = new Java.Rvalue[columnValues.length + 2];
            associateCallArgs[0] = leftJoinRecordKeyAP.read();
            associateCallArgs[1] = createArrayElementAccessExpr(
                    getLocation(), preHashVectorAP.read(), insertionLoopIndexVariable.read());
            System.arraycopy(columnValues, 0, associateCallArgs, 2, columnValues.length);

            insertionLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            this.joinMapAP.read(),
                            "associate",
                            associateCallArgs
                    )
            );

        } else if (buildKeyAPType == ARROW_INT_VECTOR_W_SELECTION_VECTOR) {
            // We know that all other vectors on the build side must be of some arrow/array vector type
            // with a selection vector
            ArrowVectorWithSelectionVectorAccessPath buildKeyAVWSVAP = ((ArrowVectorWithSelectionVectorAccessPath) buildKeyAP);

            // VectorisedHashOperators."constructPreHashKeyVectorInit"(
            //         [this.preHashVectorAP.read()], [buildKeyAVWSVAP.read()], [buildKeyAVWSVAP.selectionVector], [buildKeyAVWSVAP.selectionVectorLength]);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVectorInit",
                            new Java.Rvalue[]{
                                    this.preHashVectorAP.read(),
                                    buildKeyAVWSVAP.readArrowVector(),
                                    buildKeyAVWSVAP.readSelectionVector(),
                                    buildKeyAVWSVAP.readSelectionVectorLength()
                            }
                    ));

            // Now iterate over the selection vector to construct the hash-table records and insert them into the table
            // int recordCount = [buildKeyAVWSVAP.readSelectionVectorLength()];
            ScalarVariableAccessPath recordCountAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("recordCount"),
                    P_INT
            );
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            createPrimitiveType(getLocation(), Java.Primitive.INT),
                            recordCountAP.getVariableName(),
                            buildKeyAVWSVAP.readSelectionVectorLength()
                    )
            );

            // for (int i = 0; i < recordCount; i++) { [insertionLoopBody] }
            ScalarVariableAccessPath insertionLoopIndexVariable =
                    new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
            Java.Block insertionLoopBody = new Java.Block(getLocation());
            codeGenResult.add(
                    JaninoControlGen.createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), insertionLoopIndexVariable.getType()),
                                    insertionLoopIndexVariable.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            JaninoOperatorGen.lt(
                                    getLocation(),
                                    insertionLoopIndexVariable.read(),
                                    recordCountAP.read()
                            ),
                            JaninoOperatorGen.postIncrement(getLocation(), insertionLoopIndexVariable.write()),
                            insertionLoopBody
                    )
            );

            // Obtain the selected record index
            // int selected_record_index = [buildKeyAVWSVAP.readSelectionVector()][i];
            ScalarVariableAccessPath selectedRecordIndexAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("selected_record_index"), P_INT);
            insertionLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), selectedRecordIndexAP.getType()),
                            selectedRecordIndexAP.getVariableName(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    buildKeyAVWSVAP.readSelectionVector(),
                                    insertionLoopIndexVariable.read()
                            )
                    )
            );

            // Insert the record into the hash-table
            // Get the key (no need to perform FixedSizeBinaryVector optimisation as we only support ints here)
            // int left_join_record_key = [buildKeyAVWSVAP].get([selected_record_index]);
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
                                    buildKeyAVWSVAP.readArrowVector(),
                                    "get",
                                    new Java.Rvalue[] { selectedRecordIndexAP.read() }
                            )
                    )
            );

            // Get the values for the remaining columns to prevent key duplication
            Java.Rvalue[] columnValues = new Java.Rvalue[currentOrdinalMapping.size() - 1];
            int currentValueColumnIndex = 0;
            for (int i = 0; i < currentOrdinalMapping.size(); i++) {
                if (i == buildKeyOrdinal)
                    continue;

                AccessPath currentOrdinalAP = currentOrdinalMapping.get(i);
                if (currentOrdinalAP instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                    // Potentially need to optimise for FixedLengthBinaryVector and VarCharVector
                    QueryVariableType.LogicalType logicalColumnType = avwsvap.getType().logicalType;
                    if (logicalColumnType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR) {
                        // Allocate a global byte array cache variable to avoid allocations
                        String byteArrayCacheName = cCtx.defineQueryGlobalVariable(
                                "byte_array_cache",
                                JaninoGeneralGen.createPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                                new Java.NewArray(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                                        new Java.Rvalue[] {
                                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), currentOrdinalAP.getType().byteWidth)
                                        },
                                        0
                                ),
                                false);

                        // Perform an optimised read (which will write the appropriate value into the cache array
                        insertionLoopBody.addStatement(
                                createMethodInvocationStm(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "ArrowOptimisations"),
                                        "getFixedSizeBinaryValue",
                                        new Java.Rvalue[] {
                                                avwsvap.readArrowVector(),
                                                selectedRecordIndexAP.read(),
                                                createAmbiguousNameRef(getLocation(), byteArrayCacheName)
                                        }
                                ));

                        // Set the column value to read from the byte cache
                        columnValues[currentValueColumnIndex++] = createAmbiguousNameRef(
                                JaninoGeneralGen.getLocation(),
                                byteArrayCacheName
                        );

                    } else if (logicalColumnType == ARROW_VARCHAR_VECTOR_W_SELECTION_VECTOR) {
                        // Allocate an array of global byte array caches to avoid allocations as much as possible
                        // while keeping the var-charity. The array has space for one byte array cache upto the
                        // maximum var char length in the database
                        int maximumVarCharLength = 200; // TODO: find way to not hard-code this
                        String arrayOfCachesName = cCtx.defineQueryGlobalVariable(
                                "byte_array_caches",
                                JaninoGeneralGen.createNestedPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE),
                                JaninoGeneralGen.createNew2DPrimitiveArray(
                                        JaninoGeneralGen.getLocation(),
                                        Java.Primitive.BYTE,
                                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), maximumVarCharLength)
                                ),
                                false
                        );

                        // Perform an optimised read and assign the returned byte array to a local reference variable
                        String localColumnVariableName = cCtx.defineVariable("var_char_cached_value");
                        insertionLoopBody.addStatement(
                                JaninoVariableGen.createLocalVariable(
                                        JaninoGeneralGen.getLocation(),
                                        QueryVariableTypeMethods.toJavaType(JaninoGeneralGen.getLocation(), S_VARCHAR),
                                        localColumnVariableName,
                                        createMethodInvocation(
                                                getLocation(),
                                                createAmbiguousNameRef(getLocation(), "ArrowOptimisations"),
                                                "getVarCharBinaryValue",
                                                new Java.Rvalue[] {
                                                        avwsvap.readArrowVector(),
                                                        selectedRecordIndexAP.read(),
                                                        createAmbiguousNameRef(getLocation(), arrayOfCachesName)
                                                }
                                        )
                                )
                        );

                        // Set the column value to read from the byte cache
                        columnValues[currentValueColumnIndex++] = createAmbiguousNameRef(
                                JaninoGeneralGen.getLocation(),
                                localColumnVariableName
                        );

                    } else {
                        columnValues[currentValueColumnIndex++] = createMethodInvocation(
                                getLocation(),
                                avwsvap.readArrowVector(),
                                "get",
                                new Java.Rvalue[] { selectedRecordIndexAP.read() }
                        );

                    }

                } else if (currentOrdinalAP instanceof ArrayVectorWithSelectionVectorAccessPath avwsvap) {
                    columnValues[currentValueColumnIndex++] = createArrayElementAccessExpr(
                            getLocation(),
                            avwsvap.getArrayVectorVariable().getVectorVariable().read(),
                           selectedRecordIndexAP.read()
                    );

                } else {
                    throw new UnsupportedOperationException("JoinOperator.consumeVecBuild encountered unexpected AccessPath type");

                }

            }

            // [join_map].associate([left_join_record_key], [preHashVector][[selectedRecordIndexAP]], [columnValues])
            Java.Rvalue[] associateCallArgs = new Java.Rvalue[columnValues.length + 2];
            associateCallArgs[0] = leftJoinRecordKeyAP.read();
            associateCallArgs[1] = createArrayElementAccessExpr(
                    getLocation(), preHashVectorAP.read(), selectedRecordIndexAP.read());
            System.arraycopy(columnValues, 0, associateCallArgs, 2, columnValues.length);

            insertionLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            this.joinMapAP.read(),
                            "associate",
                            associateCallArgs
                    )
            );

        }  else if (buildKeyAPType == ARRAY_INT_VECTOR) {
            // We know that all other vectors on the build side must be of some arrow/array vector type
            // without any validity mask/selection vector
            ArrayVectorAccessPath buildKeyAVAP = ((ArrayVectorAccessPath) buildKeyAP);

            // VectorisedHashOperators."constructPreHashKeyVectorInit"(
            //         [this.preHashVectorAP.read()],
            //         [buildKeyAVAP.getVectorVariable().read()]
            //         [buildKeyAVAP.getVectorLengthVariable().read()]);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVectorInit",
                            new Java.Rvalue[]{
                                    this.preHashVectorAP.read(),
                                    buildKeyAVAP.getVectorVariable().read(),
                                    buildKeyAVAP.getVectorLengthVariable().read()
                            }
                    ));

            // Now iterate over the key vector to construct the hash-table records and insert them into the table
            // for (int i = 0; i < [buildKeyAVAP.getVectorLengthVariable().read()]; i++) { [insertionLoopBody] }
            ScalarVariableAccessPath insertionLoopIndexVariable =
                    new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
            Java.Block insertionLoopBody = new Java.Block(getLocation());
            codeGenResult.add(
                    JaninoControlGen.createForLoop(
                            getLocation(),
                            createLocalVariable(
                                    getLocation(),
                                    toJavaType(getLocation(), insertionLoopIndexVariable.getType()),
                                    insertionLoopIndexVariable.getVariableName(),
                                    createIntegerLiteral(getLocation(), 0)
                            ),
                            JaninoOperatorGen.lt(
                                    getLocation(),
                                    insertionLoopIndexVariable.read(),
                                    buildKeyAVAP.getVectorLengthVariable().read()
                            ),
                            JaninoOperatorGen.postIncrement(getLocation(), insertionLoopIndexVariable.write()),
                            insertionLoopBody
                    )
            );

            // Insert the record into the hash-table
            // Get the key
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

            // Get the values for the remaining columns to prevent key duplication
            Java.Rvalue[] columnValues = new Java.Rvalue[currentOrdinalMapping.size() - 1];
            int currentValueColumnIndex = 0;
            for (int i = 0; i < currentOrdinalMapping.size(); i++) {
                ArrayVectorAccessPath columnAP = ((ArrayVectorAccessPath) currentOrdinalMapping.get(i));
                if (i == buildKeyOrdinal)
                    continue;

                columnValues[currentValueColumnIndex++] = createArrayElementAccessExpr(
                        getLocation(),
                        columnAP.getVectorVariable().read(),
                        insertionLoopIndexVariable.read()
                );
            }

            // [join_map].associate([left_join_record_key], [preHashVector][[i]], [columnValues])
            Java.Rvalue[] associateCallArgs = new Java.Rvalue[columnValues.length + 2];
            associateCallArgs[0] = leftJoinRecordKeyAP.read();
            associateCallArgs[1] =
                    createArrayElementAccessExpr(getLocation(), preHashVectorAP.read(), insertionLoopIndexVariable.read());
            System.arraycopy(columnValues, 0, associateCallArgs, 2, columnValues.length);

            insertionLoopBody.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            this.joinMapAP.read(),
                            "associate",
                            associateCallArgs
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

        if (this.useSIMDVec())
            throw new UnsupportedOperationException("JoinOperator.consumeVecProbe no longer supports SIMD");

        // Check that we can handle the key type (casts valid due to pre-condition check in constructor)
        int probeKey = this.rightChildEquijoinIndex;
        // Need to convert the output ordinal to the right-input ordinal
        int probeKeyOrdinal = probeKey - this.leftChildColumnCount;
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        AccessPath buildKeyAP = currentOrdinalMapping.get(probeKeyOrdinal);
        QueryVariableType buildKeyAPType = buildKeyAP.getType();
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeVecProbe only supports integer join key columns");

        // We need to handle some functionality based on the input access path:
        //  - Computation of the pre-hash vector
        //  - Initialisation of the recordCount variable
        //  - Initialisation of the recordIndexVariable (and potential initialisation code required by it)
        //  - The statement required to obtain the right_join_key variable
        // That is done below
        Java.Rvalue recordCountInitialisation;
        ScalarVariableAccessPath recordIndexAP;
        Java.Statement recordIndexAPRelatedStatements = null;
        Java.Rvalue rightJoinKeyReadCode;

        // Definition of access path for the loop variable which iterates from 0 to recordCount
        // This allows for indirection via a selection vector
        // int currentLoopIndex;
        ScalarVariableAccessPath currentLoopIndexAP = new ScalarVariableAccessPath(
                cCtx.defineVariable("currentLoopIndex"),
                P_INT
        );

        // Set the default value for the recordIndexAP to the currentLoopIndexAP
        recordIndexAP = currentLoopIndexAP;

        if (buildKeyAPType == ARROW_INT_VECTOR) {
            // We know that all other vectors on the probe side must be of some arrow vector type
            // without any validity mask/selection vector
            ArrowVectorAccessPath buildKeyAVAP = ((ArrowVectorAccessPath) buildKeyAP);

            // VectorisedHashOperators.constructPreHashKeyVectorIni"([this.preHashVectorAP.read()], [buildKeyAVAP.read()]);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVectorInit",
                            new Java.Rvalue[]{
                                    this.preHashVectorAP.read(),
                                    buildKeyAVAP.read()
                            }
                    ));

            // Setup the initialisation code for the recordCount variable
            recordCountInitialisation = createMethodInvocation(
                    getLocation(),
                    buildKeyAVAP.read(),
                    "getValueCount"
            );

            // Setup the code for obtaining the right_join_key variable value
            // int right_join_key = [buildKeyAVAP.read()].get([recordIndexAP.read()]);
            rightJoinKeyReadCode = createMethodInvocation(
                    getLocation(),
                    buildKeyAVAP.read(),
                    "get",
                    new Java.Rvalue[] {
                            recordIndexAP.read()
                    }
            );

        } else if (buildKeyAPType == ARROW_INT_VECTOR_W_SELECTION_VECTOR) {
            // We know that all other vectors on the probe side must be of some arrow vector type
            // with a selection vector
            ArrowVectorWithSelectionVectorAccessPath buildKeyAVWSVAP = ((ArrowVectorWithSelectionVectorAccessPath) buildKeyAP);

            // VectorisedHashOperators.constructPreHashKeyVectorInit(
            // [this.preHashVectorAP.read()], [buildKeyAVWSVAP.arrowVector], [buildKeyAVWSVAP.selectionVector], [buildKeyAVWSVAP.selectionVectorLength]);
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVectorInit",
                            new Java.Rvalue[]{
                                    this.preHashVectorAP.read(),
                                    buildKeyAVWSVAP.readArrowVector(),
                                    buildKeyAVWSVAP.readSelectionVector(),
                                    buildKeyAVWSVAP.readSelectionVectorLength()
                            }
                    ));

            // Setup the initialisation code for the recordCount variable
            recordCountInitialisation = buildKeyAVWSVAP.readSelectionVectorLength();

            // Override the recordIndexVariable, which needs to iterate over the selection vector
            recordIndexAP = new ScalarVariableAccessPath(
                    cCtx.defineVariable("selected_record_index"), P_INT);
            // Schedule this variable for creation/instantiation
            recordIndexAPRelatedStatements = createLocalVariable(
                    getLocation(),
                    toJavaType(getLocation(), recordIndexAP.getType()),
                    recordIndexAP.getVariableName(),
                    createArrayElementAccessExpr(
                            getLocation(),
                            buildKeyAVWSVAP.readSelectionVector(),
                            currentLoopIndexAP.read()
                    )
            );

            // Setup the code for obtaining the right_join_key variable value
            // int right_join_key = [buildKeyAVWSVAP.readArrowVector()].get([recordIndexAP.read()]);
            rightJoinKeyReadCode = createMethodInvocation(
                    getLocation(),
                    buildKeyAVWSVAP.readArrowVector(),
                    "get",
                    new Java.Rvalue[] {
                            recordIndexAP.read()
                    }
            );

        }  else if (buildKeyAPType == ARRAY_INT_VECTOR) {
            // We know that all other vectors on the probe side must be of some array vector type
            // without any validity mask/selection vector
            ArrayVectorAccessPath buildKeyAVAP = ((ArrayVectorAccessPath) buildKeyAP);

            // VectorisedHashOperators.constructPreHashKeyVectorInit(
            //     [this.preHashVectorAP.read()],
            //     [buildKeyAVAP.getVectorVariable().read()],
            //     [buildKeyAVAP.getVectorLengthVariable().read()]
            // );
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "VectorisedHashOperators"),
                            "constructPreHashKeyVectorInit",
                            new Java.Rvalue[]{
                                    this.preHashVectorAP.read(),
                                    buildKeyAVAP.getVectorVariable().read(),
                                    buildKeyAVAP.getVectorLengthVariable().read()
                            }
                    ));

            // Setup the initialisation code for the record count variable
            recordCountInitialisation = buildKeyAVAP.getVectorLengthVariable().read();

            // int right_join_key = [buildKeyAVAP.getVectorVariable().read()][recordIndexAP.read()];
            rightJoinKeyReadCode =  createArrayElementAccessExpr(
                    getLocation(),
                    buildKeyAVAP.getVectorVariable().read(),
                    recordIndexAP.read()
            );

        } else {
            throw new UnsupportedOperationException(
                    "JoinOperator.consumeVecProbe does not support the provided AccessPath for the build key " + buildKeyAPType);
        }

        // Now we need to iterate over the key vector to construct the result vectors by probing the
        // hash table and reading the other vectors on the probe side
        // int recordCount = [accessPathSpecificInitialisation];
        ScalarVariableAccessPath recordCountAP = new ScalarVariableAccessPath(
                cCtx.defineVariable("recordCount"),
                P_INT
        );
        codeGenResult.add(
                createLocalVariable(
                        getLocation(),
                        createPrimitiveType(getLocation(), Java.Primitive.INT),
                        recordCountAP.getVariableName(),
                        recordCountInitialisation
                )
        );

        // Add the actual currentRecordIndex variable
        // int currentLoopIndex = 0;
        codeGenResult.add(
                createPrimitiveLocalVar(
                        getLocation(),
                        Java.Primitive.INT,
                        currentLoopIndexAP.getVariableName(),
                        "0"
                )
        );

        // while (currentRecordIndex < recordCount) { [whileLoopBody] }
        Java.Block whileLoopBody = new Java.Block(getLocation());
        codeGenResult.add(
                JaninoControlGen.createWhileLoop(
                        getLocation(),
                        JaninoOperatorGen.lt(getLocation(), currentLoopIndexAP.read(), recordCountAP.read()),
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
        // while (currentLoopIndex < recordCount) { [resultVectorConstructionLoop] }
        Java.Block resultVectorConstructionLoop = new Java.Block(getLocation());
        whileLoopBody.addStatement(
                JaninoControlGen.createWhileLoop(
                        getLocation(),
                        JaninoOperatorGen.lt(getLocation(), currentLoopIndexAP.read(), recordCountAP.read()),
                        resultVectorConstructionLoop
                )
        );

        // Add recordIndexAP related code if required
        if (recordIndexAPRelatedStatements != null) {
            resultVectorConstructionLoop.addStatement(recordIndexAPRelatedStatements);
        }

        // int right_join_key = [AccessPathSpecificCode];
        ScalarVariableAccessPath rightJoinKeyAP =
                new ScalarVariableAccessPath(cCtx.defineVariable("right_join_key"), P_INT);
        resultVectorConstructionLoop.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), rightJoinKeyAP.getType()),
                        rightJoinKeyAP.getVariableName(),
                        rightJoinKeyReadCode
                )
        );

        // long right_join_key_pre_hash = [this.preHashVectorAP.read()][[recordIndexAP.read()]];
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
                                recordIndexAP.read()
                        )
                )
        );

        // Obtain the index for the values for the left side of the join for the current key value
        ScalarVariableAccessPath joinRecordIndex = new ScalarVariableAccessPath(
                cCtx.defineVariable("records_to_join_index"),
                P_INT);
        resultVectorConstructionLoop.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), joinRecordIndex.getType()),
                        joinRecordIndex.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                this.joinMapAP.read(),
                                "getIndex",
                                new Java.Rvalue[] {
                                        rightJoinKeyAP.read(),
                                        rightJoinKeyPreHashAP.read()
                                }
                        )
                )
        );

        // Use the index to check if the left side of the join contains records for the given key
        // otherwise continue
        // if ([records_to_join_index] == -1) {
        //     currentLoopIndex++;
        //     continue;
        // }
        Java.Block incrementAndContinueBlock = new Java.Block(getLocation());
        incrementAndContinueBlock.addStatement(
                JaninoOperatorGen.postIncrementStm(getLocation(), currentLoopIndexAP.write()));
        incrementAndContinueBlock.addStatement(new Java.ContinueStatement(getLocation(), null));

        resultVectorConstructionLoop.addStatement(
                JaninoControlGen.createIf(
                        getLocation(),
                        JaninoOperatorGen.eq(
                                getLocation(),
                                joinRecordIndex.read(),
                                createIntegerLiteral(getLocation(), -1)
                        ),
                        incrementAndContinueBlock
                )
        );

        // int left_join_record_count = [joinMapAP].keysRecordCount[records_to_join_index];
        ScalarVariableAccessPath leftJoinRecordCount =
                new ScalarVariableAccessPath(cCtx.defineVariable("left_join_record_count"), P_INT);
        resultVectorConstructionLoop.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), leftJoinRecordCount.getType()),
                        leftJoinRecordCount.getVariableName(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        joinMapAP.read(),
                                        KeyMultiRecordMapGenerator.keysRecordCountAP.getVariableName()
                                ),
                                joinRecordIndex.read()
                        )
                )
        );

        // Check if there is still room in the result vector and if not, break from the inner loop
        // if (leftJoinRecordCount > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))
        //     break;
        Java.Block breakBlock = new Java.Block(getLocation());
        breakBlock.addStatement(new Java.BreakStatement(getLocation(), null));
        resultVectorConstructionLoop.addStatement(
                JaninoControlGen.createIf(
                        getLocation(),
                        JaninoOperatorGen.gt(
                                getLocation(),
                                leftJoinRecordCount.read(),
                                JaninoOperatorGen.sub(
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
            if (i == probeKeyOrdinal) {
                rightJoinOrdinalValues[i] = rightJoinKeyAP.read();

            } else {
                // Need to distinguish the access path
                AccessPath rightJoinColumnAP = currentOrdinalMapping.get(i);
                QueryVariableType.LogicalType rightJoinColumnAPLogicalType = rightJoinColumnAP.getType().logicalType;
                Java.Rvalue rightJoinColumnRValue;
                if (rightJoinColumnAP instanceof ArrowVectorAccessPath avap) {
                    // No need to optimise for FixedLengthBinaryVector and VarCharVector, since we need copies anyhow
                    // to store in the result vector array
                    rightJoinColumnRValue = createMethodInvocation(
                            getLocation(),
                            avap.read(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );

                } else if (rightJoinColumnAP instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                    // No need to optimise for FixedLengthBinaryVector and VarCharVector, since we need copies anyhow
                    // to store in the result vector array
                    rightJoinColumnRValue = createMethodInvocation(
                            getLocation(),
                            avwsvap.readArrowVector(),
                            "get",
                            new Java.Rvalue[] { recordIndexAP.read() }
                    );

                } else if (rightJoinColumnAP instanceof ArrayVectorAccessPath avap) {
                    rightJoinColumnRValue = createArrayElementAccessExpr(
                            getLocation(),
                            avap.getVectorVariable().read(),
                            recordIndexAP.read()
                    );

                } else if (rightJoinColumnAP instanceof ArrayVectorWithSelectionVectorAccessPath avwsvap) {
                    rightJoinColumnRValue = createArrayElementAccessExpr(
                            getLocation(),
                            avwsvap.getArrayVectorVariable().getVectorVariable().read(),
                            recordIndexAP.read()
                    );

                } else if (rightJoinColumnAP instanceof ArrayVectorWithValidityMaskAccessPath avwvmap) {
                    rightJoinColumnRValue = createArrayElementAccessExpr(
                            getLocation(),
                            avwvmap.getArrayVectorVariable().getVectorVariable().read(),
                            recordIndexAP.read()
                    );

                } else {
                    throw new UnsupportedOperationException(
                            "JoinOperator.consumeVecProbe does not support obtaining values for the provided AccessPath");
                }

                QueryVariableType rightJoinColumnType;
                if (rightJoinColumnAP.getType().logicalType == ARROW_FIXED_LENGTH_BINARY_VECTOR || rightJoinColumnAP.getType().logicalType == ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR)
                    rightJoinColumnType = new QueryVariableType(QueryVariableType.LogicalType.S_FL_BIN, rightJoinColumnAP.getType().byteWidth);
                else if (rightJoinColumnAP.getType() == ARROW_VARCHAR_VECTOR)
                    rightJoinColumnType = S_VARCHAR;
                else
                    rightJoinColumnType = primitiveType(primitiveType(rightJoinColumnAP.getType()));

                ScalarVariableAccessPath rightJoinColumnValue = new ScalarVariableAccessPath(
                        cCtx.defineVariable("right_join_ord_" + i),
                        rightJoinColumnType
                );

                resultVectorConstructionLoop.addStatement(
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), rightJoinColumnValue.getType()),
                                rightJoinColumnValue.getVariableName(),
                                rightJoinColumnRValue
                        )
                );

                rightJoinOrdinalValues[i] = rightJoinColumnValue.read();
            }
        }

        // for (int i = 0; i < left_join_record_count; i++) {
        //     [joinLoopBody]
        // }
        ScalarVariableAccessPath joinLoopIndexVar =
                new ScalarVariableAccessPath(cCtx.defineVariable("i"), P_INT);
        Java.Block joinLoopBody = new Java.Block(getLocation());
        resultVectorConstructionLoop.addStatement(
                JaninoControlGen.createForLoop(
                        getLocation(),
                        createPrimitiveLocalVar(getLocation(), Java.Primitive.INT, joinLoopIndexVar.getVariableName(), "0"),
                        JaninoOperatorGen.lt(getLocation(), joinLoopIndexVar.read(), leftJoinRecordCount.read()),
                        JaninoOperatorGen.postIncrement(getLocation(), joinLoopIndexVar.write()),
                        joinLoopBody
                )
        );

        // In the loop over the left join records, first add the statements to set the correct
        // values in the result vectors for the left side join columns
        int numberOfLhsColumns = this.joinMapGenerator.valueFieldNames.length + 1; // Add 1 for key column

        // JoinMapType.ValueRecordType left_join_rec = joinMap.records[joinRecordIndex][joinLoopIndexVar];String leftJoinRec = cCtx.defineVariable("left_join_rec");
        String leftJoinRec = cCtx.defineVariable("left_join_rec");
        if (this.joinMapGenerator.valueFieldNames.length > 0) {
            joinLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            new Java.ReferenceType(
                                    getLocation(),
                                    new Java.Annotation[0],
                                    new String[] {
                                            this.joinMapGenerator.mapDeclaration.name,
                                            this.joinMapGenerator.valueRecordDeclaration.name
                                    },
                                    null
                            ),
                            leftJoinRec,
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            new Java.FieldAccessExpression(
                                                    getLocation(),
                                                    joinMapAP.read(),
                                                    KeyMultiRecordMapGenerator.valueRecordArrayName
                                            ),
                                            joinRecordIndex.read()
                                    ),
                                    joinLoopIndexVar.read()
                            )
                    )
            );
        }

        // Need to account for the join key de-duplication:
        // no need to construct the LHS key vector too, since it is a duplicate of the RHS key vector
        int leftKeyIndex = this.leftChildEquijoinIndex;
        int currentLhsValueColumnIndex = 0;
        for (int i = 0; i < numberOfLhsColumns; i++) {
            // Skip LHS key vector creation
            if (i == leftKeyIndex)
                continue;

            joinLoopBody.addStatement(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    this.resultVectorDefinitions.get(i).read(),
                                    currentResultIndexAP.read()
                            ),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    createAmbiguousNameRef(getLocation(), leftJoinRec),
                                    this.joinMapGenerator.valueFieldNames[currentLhsValueColumnIndex++]
                            )
                    )
            );
        }

        // And then set the correct values for the right join columns
        for (int i = 0; i < rightJoinOrdinalValues.length; i++) {
            int resultIndex = numberOfLhsColumns + i;
            joinLoopBody.addStatement(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    this.resultVectorDefinitions.get(resultIndex).read(),
                                    currentResultIndexAP.read()
                            ),
                            rightJoinOrdinalValues[i]
                    )
            );
        }

        // And move onto the next result index
        joinLoopBody.addStatement(JaninoOperatorGen.postIncrementStm(getLocation(), currentResultIndexAP.write()));
        // End of the join-loop

        // Finally, move to the next record
        // currentLoopIndex++;
        resultVectorConstructionLoop.addStatement(
                JaninoOperatorGen.postIncrementStm(getLocation(), currentLoopIndexAP.write()));

        // End of the inner result vector construction loop
        // Set-up the correct ordinal mapping, link the LHS key column to the RHS key column to avoid duplication
        List<AccessPath> resultVectorAPs = new ArrayList<>(this.resultVectorDefinitions.size());
        for (int i = 0; i < this.resultVectorDefinitions.size(); i++) {
            int resultVectorIndex = i;
            if (i == leftKeyIndex) {
                resultVectorIndex = probeKey;
            }

            AccessPath resultVectorAP = new ArrayVectorAccessPath(
                    this.resultVectorDefinitions.get(resultVectorIndex),
                    currentResultIndexAP,
                    vectorTypeForPrimitiveArrayType(this.resultVectorDefinitions.get(resultVectorIndex).getType())
            );
            resultVectorAPs.add(i, resultVectorAP);
        }
        cCtx.setCurrentOrdinalMapping(resultVectorAPs);

        // Invoke the parent
        whileLoopBody.addStatements(this.vecParentConsume(cCtx, oCtx));

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
            this.joinMapGenerator = joinMapGeneratorForRelation(
                    cCtx.getCurrentOrdinalMapping(),
                    this.leftChildEquijoinIndex
            );

            // Store the number of columns in the left-child records
            this.leftChildColumnCount = cCtx.getCurrentOrdinalMapping().size();

            // And build the hash table
            return vectorised ? this.consumeVecBuild(cCtx, oCtx) : this.consumeNonVecBuild(cCtx, oCtx);

        } else {
            // Store the number of records in the right-child records
            int rightChildColumnCount = cCtx.getCurrentOrdinalMapping().size();

            // Initialise result structures
            this.resultColumnCount = rightChildColumnCount + this.leftChildColumnCount;

            // Perform the probe (which also has the parent operator consume the result)
            return vectorised ? this.consumeVecProbe(cCtx, oCtx) : this.consumeNonVecProbe(cCtx, oCtx);

        }
    }

    /**
     * Method to instantiate a generator for a custom join map for storing the elements of an input
     * relation in a hash-table.
     * @param relationType The input relation that should be supported by the join map.
     * @param keyIndex The index of the key column that should be used for the join.
     * @return The requested {@link KeyMultiRecordMapGenerator} instance.
     */
    private KeyMultiRecordMapGenerator joinMapGeneratorForRelation(
            List<AccessPath> relationType, int keyIndex) {
        // Obtain the types of all the columns in the relation
        QueryVariableType[] primitiveColumnTypes = new QueryVariableType[relationType.size()];
        for (int i = 0; i < primitiveColumnTypes.length; i++) {
            QueryVariableType relationColumnType = relationType.get(i).getType();
            if (relationColumnType == S_VARCHAR
                    || relationColumnType == ARROW_VARCHAR_VECTOR || relationColumnType == ARRAY_VARCHAR_VECTOR)
                primitiveColumnTypes[i] = S_VARCHAR;
            else if (relationColumnType.logicalType == QueryVariableType.LogicalType.S_FL_BIN
                    || relationColumnType.logicalType == QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR
                    || relationColumnType.logicalType == QueryVariableType.LogicalType.ARROW_FIXED_LENGTH_BINARY_VECTOR_W_SELECTION_VECTOR
                    || relationColumnType.logicalType == QueryVariableType.LogicalType.ARRAY_FIXED_LENGTH_BINARY_VECTOR)
                primitiveColumnTypes[i] = new QueryVariableType(QueryVariableType.LogicalType.S_FL_BIN, relationColumnType.byteWidth);
            else
                primitiveColumnTypes[i] = primitiveType(relationColumnType);
        }

        // Extract the key column type
        QueryVariableType keyColumnType = primitiveColumnTypes[keyIndex];

        // Extract the value column types (so that the key is not included twice in the map)
        QueryVariableType[] valueColumnTypes = new QueryVariableType[relationType.size() - 1];
        int valueColumnIndex = 0;
        for (int i = 0; i < primitiveColumnTypes.length; i++) {
            if (i == keyIndex)
                continue;

            valueColumnTypes[valueColumnIndex++] = primitiveColumnTypes[i];
        }

        return new KeyMultiRecordMapGenerator(
                keyColumnType,
                valueColumnTypes
        );
    }

}
