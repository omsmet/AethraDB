package evaluation.codegen.operators;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.MapAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import evaluation.codegen.infrastructure.janino.JaninoMethodGen;
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

import static evaluation.codegen.infrastructure.context.QueryVariableType.MAP_INT_MULTI_OBJECT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.queryVariableTypeFromJavaType;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclarationStm;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createPublicFinalFieldDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForEachLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIfNotContinue;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameter;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameters;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
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
        // which will have to be added to the codeGenResult first, after which we initialise the actual
        // map used for the join.
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
        // the consumeNonvec method on @this, which should perform the hash-table probe and call
        // the consumeNonVec method on the parent.
        cCtx.pushCodeGenContext();
        codeGenResult.addAll(this.rightChild.produceNonVec(cCtx, oCtx));
        cCtx.popCodeGenContext();

        return codeGenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        if (!this.consumeInProbePhase) {
            // Mark that on the next call, we are in the probe phase
            this.consumeInProbePhase = true;

            // Then introduce the class to store records of the left-child in the hash-table
            this.leftChildRecordType = generateRecordTypeForRelation(cCtx.getCurrentOrdinalMapping());

            // And build the hash table
            return this.consumeNonVecBuild(cCtx, oCtx);

        } else {
            // Perform the probe (which also has the parent operator consume the result)
            return this.consumeNonVecProbe(cCtx, oCtx);

        }
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
        AccessPath buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);
        QueryVariableType buildKeyOrdinalPrimitiveType = primitiveType(buildKeyAP.getType());

        if (buildKeyOrdinalPrimitiveType != P_INT)
            throw new UnsupportedOperationException("JoinOperator.consumeNonVecBuild only supports integer join key columns");

        // Simply insert the records into the hash-table based on the input type
        if (!this.simdEnabled) {
            // Obtain the local access path for the key column value
            Java.Rvalue keyColumnRvalue = getRValueFromAccessPathNonVec(cCtx, buildKeyOrdinal, codeGenResult);
            buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);

            // Compute the pre-hash value in a local variable
            ScalarVariableAccessPath leftJoinKeyPreHash =
                    new ScalarVariableAccessPath(cCtx.defineVariable("left_join_key_prehash"), P_LONG);
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

            // Create the record for the current row
            List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
            Java.Rvalue[] currentRowRValues = new Java.Rvalue[currentOrdinalMapping.size()];
            for (int i = 0; i < currentOrdinalMapping.size(); i++)
                currentRowRValues[i] = getRValueFromAccessPathNonVec(cCtx, i, codeGenResult);

            String recordVariableName = cCtx.defineVariable("left_join_record");
            Java.Type recordType = createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName());
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            recordType,
                            recordVariableName,
                            createClassInstance(
                                    getLocation(),
                                    recordType,
                                    currentRowRValues
                            )
                    )
            );

            // Insert the record into the join map
            codeGenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            this.joinMapAP.read(),
                            "add",
                            new Java.Rvalue[] {
                                    ((ScalarVariableAccessPath) buildKeyAP).read(), // Cast valid due to local access path
                                    leftJoinKeyPreHash.read(),
                                    createAmbiguousNameRef(getLocation(), recordVariableName)
                            }
                    )
            );

        } else { // this.simdEnabled
            throw new UnsupportedOperationException("Need to implement");
        }


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

        // Probe the hash-table based on the input type
        if (!this.simdEnabled) {
            // Obtain the local access path for the key column value
            Java.Rvalue keyColumnRvalue = getRValueFromAccessPathNonVec(cCtx, buildKeyOrdinal, codeGenResult);
            buildKeyAP = cCtx.getCurrentOrdinalMapping().get(buildKeyOrdinal);

            // Compute the pre-hash value in a local variable
            ScalarVariableAccessPath rightJoinKeyPreHash =
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

            // Check if the left side of the join contains records for the given key, otherwise continue
            // if (![this.joinMap.read()].contains(key, prehash)) continue;
            codeGenResult.add(
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

            // Obtain the values for the left side of the join for the current key value
            String joinRecordsListName = cCtx.defineVariable("records_to_join_list");
            codeGenResult.add(
                    createLocalVariable(
                            getLocation(),
                            createReferenceType(getLocation(), "List", this.leftChildRecordType.lcd.getName()),
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

            // Get the local access paths for the right-hand join columns
            List<AccessPath> rightHandOrdinalMappings = cCtx.getCurrentOrdinalMapping();
            List<AccessPath> localRightHandAPs = new ArrayList<>(rightHandOrdinalMappings.size());
            for (int i = 0; i < rightHandOrdinalMappings.size(); i++) {
                getRValueFromAccessPathNonVec(cCtx, i, codeGenResult);
                localRightHandAPs.add(i, cCtx.getCurrentOrdinalMapping().get(i));
            }

            // Generate a for-loop to iterate over the join records from the left-hand side
            // foreach ([RecordType] left_join_record : records_to_join_list) { [joinLoopBody] }
            String leftJoinRecordName = cCtx.defineVariable("left_join_record");
            Java.Block joinLoopBody = new Java.Block(getLocation());
            codeGenResult.add(
                    createForEachLoop(
                            getLocation(),
                            createFormalParameter(
                                    getLocation(),
                                    createReferenceType(getLocation(), this.leftChildRecordType.lcd.getName()),
                                    leftJoinRecordName
                            ),
                            createAmbiguousNameRef(getLocation(), joinRecordsListName),
                            joinLoopBody
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

        } else { // this.simdEnabled
            throw new UnsupportedOperationException("IMPLEMENT");
        }

        return codeGenResult;
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
