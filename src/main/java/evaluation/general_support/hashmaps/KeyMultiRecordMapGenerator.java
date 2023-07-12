package evaluation.general_support.hashmaps;

import evaluation.codegen.infrastructure.context.QueryVariableType;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_BOOLEAN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.isPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveMemberTypeForArray;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaPrimitive;
import static evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createLocalClassDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createPrivateFieldDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoClassGen.createPublicFieldDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createForLoop;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createIf;
import static evaluation.codegen.infrastructure.janino.JaninoControlGen.createWhileLoop;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createCast;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNestedPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNew2DPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createStringLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createThisFieldAccess;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createConstructor;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameter;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameters;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethod;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createReturnStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.and;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.binAnd;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.div;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.eq;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.gt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lShift;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.le;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.neq;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.not;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrementStm;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.sub;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createSimpleVariableDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;

/**
 * This class provides methods to generate a hash-map implementation for mapping some primitive type
 * keys to multiple objects of some record-like type, where each field of the record is also a
 * primitive type. It thus mimics the interface of
 * {@code Map<Primitive Type, Record(Primitive Type ...)>}.
 * To hash keys, a generated map relies on the appropriate hash function definitions for the given
 * key type:
 *  - For int keys, the map uses the {@link Int_Hash_Function}.
 *  - Other key types are currently not yet supported.
 */
public class KeyMultiRecordMapGenerator {

    /**
     * The {@link QueryVariableType} indicating the primitive key type of the map to be generated.
     */
    public final QueryVariableType keyType;

    /**
     * The {@link QueryVariableType}s indicating the primitive type of each record value.
     */
    public final QueryVariableType[] valueTypes;

    /**
     * Boolean keeping track of whether generation has already been performed.
     */
    private boolean generationFinished;

    /**
     * The {@link Java.LocalClassDeclaration} generated by {@code this} if {@code this.generationFinished == true}.
     */
    private Java.LocalClassDeclaration mapDeclaration;

    /**
     * The {@link AccessPath} to the variable storing the number of records in the map.
     */
    private static final ScalarVariableAccessPath numberOfRecordsAP =
            new ScalarVariableAccessPath("numberOfRecords", P_INT);

    /**
     * The {@link AccessPath} to the array storing the keys in the map.
     */
    private final ArrayAccessPath keysAP;

    /**
     * The {@link AccessPath} to the array storing the number of records for each key.
     */
    private static final ArrayAccessPath keysRecordCountAP =
            new ArrayAccessPath("keysRecordCount", P_A_INT);

    /**
     * The names of the arrays storing the record field values in the map.
     */
    public final String[] valueVariableNames;

    /**
     * The {@link AccessPath} to the array storing the hash-table.
     */
    private static final ArrayAccessPath hashTableAP =
            new ArrayAccessPath("hashTable", P_A_INT);

    /**
     * The {@link AccessPath} to the array storing the collision chains.
     */
    private static final ArrayAccessPath nextArrayAP =
            new ArrayAccessPath("next", P_A_INT);

    /**
     * Some helper definitions to enhance consistency.
     */
    private static final String ASSOCIATE_METHOD_NAME = "associate";
    private static final String FIND_METHOD_NAME = "find";
    private static final String GROW_ARRAYS_METHOD_NAME = "growArrays";
    private static final String PUT_HASH_ENTRY_METHOD_NAME = "putHashEntry";
    private static final String REHASH_METHOD_NAME = "rehash";
    private static final String GET_INDEX_METHOD_NAME = "getIndex";
    private static final String RESET_METHOD_NAME = "reset";

    /**
     * Instantiate a {@link KeyMultiRecordMapGenerator} to generate a map type for specific key and
     * value types.
     * @param keyType The key type that is to be used by the generated map.
     * @param valueTypes The value types that records in the map should be built up of.
     */
    public KeyMultiRecordMapGenerator(QueryVariableType keyType, QueryVariableType[] valueTypes) {
        if (!isPrimitive(keyType))
            throw new IllegalArgumentException("KeyMultiRecordMapGenerator expects a primitive key type, not " + keyType);
        for (int i = 0; i < valueTypes.length; i++) {
            QueryVariableType valueType = valueTypes[i];
            if (!isPrimitive(valueType))
                throw new IllegalArgumentException("KeyMultiRecordMapGenerator expects primitive value types, not " + valueType);
        }

        this.keyType = keyType;
        this.valueTypes = valueTypes;

        this.generationFinished = false;

        this.keysAP = new ArrayAccessPath("keys", primitiveArrayTypeForPrimitive(this.keyType));
        this.valueVariableNames = new String[valueTypes.length];
        for (int i = 0; i < valueVariableNames.length; i++)
            this.valueVariableNames[i] = "values_record_ord_" + i;
    }

    /**
     * Method to generate the actual map type for the provided specification.
     * @return A {@link org.codehaus.janino.Java.ClassDeclaration} defining the configured
     * key-record map type.
     */
    public Java.LocalClassDeclaration generate() {
        // If the type was already generated, return it immediately
        if (generationFinished)
            return this.mapDeclaration;

        // Generate the class declaration that will represent the type
        this.mapDeclaration = createLocalClassDeclaration(
                getLocation(),
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PRIVATE.toString(), getLocation()),
                        new Java.AccessModifier("final", getLocation())
                },
                "KeyMultiRecordMap_" + this.hashCode()
        );

        // Now generate the class body in a step-by-step fashion
        this.generateFieldDeclarations();
        this.generateConstructors();
        this.generateAssociateMethod();
        this.generateFindMethod();
        this.generateGrowArraysMethod();
        this.generatePutHashEntryMethod();
        this.generateRehashMethod();
        this.generateGetIndexMethod();
        this.generateResetMethod();

        // Mark that generation was finished and return the generated type
        this.generationFinished = true;
        return this.mapDeclaration;
    }

    /**
     * Method to generate all the required fields for the generated map type.
     */
    private void generateFieldDeclarations() {
        // Add the variable indicating the number of records in the map
        this.mapDeclaration.addFieldDeclaration(
                createPrivateFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), numberOfRecordsAP.getType()),
                        createSimpleVariableDeclaration(
                                getLocation(),
                                numberOfRecordsAP.getVariableName()
                        )
                )
        );

        // Add the variable storing the keys in the map
        this.mapDeclaration.addFieldDeclaration(
                createPrivateFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), this.keysAP.getType()),
                        createSimpleVariableDeclaration(
                                getLocation(),
                                this.keysAP.getVariableName()
                        )
                )
        );

        // Add the variable storing the record count per key in the map
        this.mapDeclaration.addFieldDeclaration(
                createPublicFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), keysRecordCountAP.getType()),
                        createSimpleVariableDeclaration(
                                getLocation(),
                                keysRecordCountAP.getVariableName()
                        )
                )
        );

        // For each field of the value types, create a nested array
        for (int i = 0; i < this.valueVariableNames.length; i++) {
            this.mapDeclaration.addFieldDeclaration(
                    createPublicFieldDeclaration(
                            getLocation(),
                            createNestedPrimitiveArrayType(
                                    getLocation(),
                                    toJavaPrimitive(this.valueTypes[i])
                            ),
                            createSimpleVariableDeclaration(
                                    getLocation(),
                                    this.valueVariableNames[i]
                            )
                    )
            );
        }

        // Add the field storing the hash table
        this.mapDeclaration.addFieldDeclaration(
                createPrivateFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), hashTableAP.getType()),
                        createSimpleVariableDeclaration(
                                getLocation(),
                                hashTableAP.getVariableName()
                        )
                )
        );

        // Add the field storing the collision chains
        this.mapDeclaration.addFieldDeclaration(
                createPrivateFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), nextArrayAP.getType()),
                        createSimpleVariableDeclaration(
                                getLocation(),
                                nextArrayAP.getVariableName()
                        )
                )
        );
    }

    /**
     * Method to generate the constructors for the generated map type.
     */
    private void generateConstructors() {

        // Start by generating the no-argument constructor which calls the real constructor with
        // a default map-size of 4.
        createConstructor(
                getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createFormalParameters(getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
                new Java.AlternateConstructorInvocation(
                        getLocation(),
                        new Java.Rvalue[] {
                                createIntegerLiteral(getLocation(), 4)
                        }
                ),
                new ArrayList<>()
        );


        // Add the real constructor which initialises all fields
        List<Java.Statement> constructorBody = new ArrayList<>();
        ScalarVariableAccessPath capacityParameterAP = new ScalarVariableAccessPath("capacity", P_INT);

        // Check pre-condition that the capacity needs to be a power of 2 larger than 1
        // Required for efficient hashing
        // if (!(capacity > 1 && (capacity & (capacity - 1)) == 0))
        //     throw new IllegalArgumentException("The map capacity is required to be a power of two");
        constructorBody.add(
                createIf(
                        getLocation(),
                        not(
                                getLocation(),
                                and(
                                        getLocation(),
                                        gt(
                                                getLocation(),
                                                capacityParameterAP.read(),
                                                createIntegerLiteral(getLocation(), 1)
                                        ),
                                        eq(
                                                getLocation(),
                                                binAnd(
                                                        getLocation(),
                                                        capacityParameterAP.read(),
                                                        sub(
                                                                getLocation(),
                                                                capacityParameterAP.read(),
                                                                createIntegerLiteral(getLocation(), 1)
                                                        )
                                                ),
                                                createIntegerLiteral(getLocation(), 0)
                                        )
                                )
                        ),
                        new Java.ThrowStatement(
                                getLocation(),
                                createClassInstance(
                                        getLocation(),
                                        createReferenceType(
                                                getLocation(),
                                                "java.lang.IllegalArgumentException"
                                        ),
                                        new Java.Rvalue[] {
                                                createStringLiteral(getLocation(), "\"The map capacity is required to be a power of two\"")
                                        }
                                )
                        )
                )
        );

        // Initialise the numberOfRecords field
        constructorBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                numberOfRecordsAP.getVariableName()
                        ),
                        createIntegerLiteral(getLocation(), 0)
                )
        );

        // Initialise the keys array
        constructorBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                keysAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(keyType),
                                capacityParameterAP.read()
                        )
                )
        );

        constructorBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        new Java.ThisReference(getLocation()),
                                        keysAP.getVariableName()
                                ),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Initialise the record count per key
        constructorBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                keysRecordCountAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(keysRecordCountAP.getType())),
                                capacityParameterAP.read()
                        )
                )
        );

        // Initialise each of the value arrays
        for (int i = 0; i < this.valueVariableNames.length; i++) {
            constructorBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(
                                    getLocation(),
                                    this.valueVariableNames[i]
                            ),
                            createNew2DPrimitiveArray(
                                    getLocation(),
                                    toJavaPrimitive(valueTypes[i]),
                                    capacityParameterAP.read(),
                                    createIntegerLiteral(getLocation(), 2)
                            )
                    )
            );
        }

        // Initialise the hash table
        constructorBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                hashTableAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(hashTableAP.getType())),
                                capacityParameterAP.read()
                        )
                )
        );

        constructorBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        new Java.ThisReference(getLocation()),
                                        hashTableAP.getVariableName()
                                ),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Initialise the collision chain array
        constructorBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                nextArrayAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(nextArrayAP.getType())),
                                capacityParameterAP.read()
                        )
                )
        );

        constructorBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        new Java.ThisReference(getLocation()),
                                        nextArrayAP.getVariableName()
                                ),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Add the actual constructor
        createConstructor(
                getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createFormalParameters(
                        getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[] {
                                createFormalParameter(
                                        getLocation(),
                                        toJavaType(getLocation(), capacityParameterAP.getType()),
                                        capacityParameterAP.getVariableName()
                                )
                        }
                ),
                null,
                constructorBody
        );
    }

    /**
     * Method to generate the "associate" method for the generated map type, which associates a
     * record to a key.
     */
    private void generateAssociateMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[this.valueTypes.length + 2];

        formalParameters[0] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_LONG),
                "preHash"
        );

        for (int i = 0; i < this.valueTypes.length; i++) {
            formalParameters[i + 2] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), this.valueTypes[i]),
                    "record_ord_" + i
            );
        }

        // Create the method body
        List<Java.Statement> associateMethodBody = new ArrayList<>();

        // Create the non-negative key check
        associateMethodBody.add(
                generateNonNegativeCheck(
                        createAmbiguousNameRef(
                                getLocation(),
                                formalParameters[0].name
                        )
                )
        );

        // Declare the index variable and check whether the key is already contained in the map
        // int index = find(key, preHash);
        ScalarVariableAccessPath indexAP = new ScalarVariableAccessPath("index", P_INT);
        associateMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), indexAP.getType()),
                        indexAP.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                FIND_METHOD_NAME,
                                new Java.Rvalue[] {
                                        createAmbiguousNameRef(
                                                getLocation(),
                                                formalParameters[0].name
                                        ),
                                        createAmbiguousNameRef(
                                                getLocation(),
                                                formalParameters[1].name
                                        )
                                }
                        )
                )
        );

        // If so, set the index variable to the existing entry, otherwise, allocate a new index
        // boolean newEntry = false;
        // if (index == -1) {
        //     newEntry = true;
        //     index = this.numberOfRecords++;
        //     if (this.keys.length == index)
        //         growArrays();
        //     this.keys[index] = key;
        // }
        ScalarVariableAccessPath newEntryAP = new ScalarVariableAccessPath("newEntry", P_BOOLEAN);
        associateMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), newEntryAP.getType()),
                        newEntryAP.getVariableName(),
                        new Java.BooleanLiteral(getLocation(), "false")
                )
        );

        Java.Block allocateIndexBody = new Java.Block(getLocation());
        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        getLocation(),
                        newEntryAP.write(),
                        new Java.BooleanLiteral(getLocation(), "true")
                )
        );

        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        getLocation(),
                        indexAP.write(),
                        postIncrement(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        new Java.ThisReference(getLocation()),
                                        numberOfRecordsAP.getVariableName()
                                )
                        )
                )
        );
        allocateIndexBody.addStatement(
                createIf(
                        getLocation(),
                        eq(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        new Java.FieldAccessExpression(
                                                getLocation(),
                                                new Java.ThisReference(getLocation()),
                                                this.keysAP.getVariableName()
                                        ),
                                        "length"
                                ),
                                indexAP.read()
                        ),
                        createMethodInvocationStm(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                GROW_ARRAYS_METHOD_NAME
                        )
                )
        );
        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(),
                                        this.keysAP.getVariableName()
                                ),
                                indexAP.read()
                        ),
                        createAmbiguousNameRef(getLocation(), formalParameters[0].name)
                )
        );

        associateMethodBody.add(
                createIf(
                        getLocation(),
                        eq(
                                getLocation(),
                                indexAP.read(),
                                createIntegerLiteral(getLocation(), -1)
                        ),
                        allocateIndexBody
                )
        );

        // Check if there is enough room left in this key's value arrays to store the new record
        // int insertionIndex = this.keysRecordCount[index];
        // if (!(insertionIndex < this.values_record_ord_0[index].length)) [recordStoreExtendArrays]
        ScalarVariableAccessPath insertionIndexAP =
                new ScalarVariableAccessPath("insertionIndex", P_INT);
        associateMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), insertionIndexAP.getType()),
                        insertionIndexAP.getVariableName(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), keysRecordCountAP.getVariableName()),
                                indexAP.read()
                        )
                )
        );

        Java.Block recordStoreExtendArrays = new Java.Block(getLocation());
        associateMethodBody.add(
                createIf(
                        getLocation(),
                        not(
                                getLocation(),
                                lt(
                                        getLocation(),
                                        insertionIndexAP.read(),
                                        new Java.FieldAccessExpression(
                                                getLocation(),
                                                createArrayElementAccessExpr(
                                                        getLocation(),
                                                        createThisFieldAccess(getLocation(), valueVariableNames[0]),
                                                        indexAP.read()
                                                ),
                                                "length"
                                        )
                                )
                        ),
                        recordStoreExtendArrays
                )
        );

        // Extend the value arrays for the current index by doubling their size
        // int currentValueArraysSize = this.values_record_ord_0[index].length;
        ScalarVariableAccessPath currentValueArraysSize =
                new ScalarVariableAccessPath("currentValueArraysSize", P_INT);
        recordStoreExtendArrays.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), currentValueArraysSize.getType()),
                        currentValueArraysSize.getVariableName(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        createThisFieldAccess(getLocation(), this.valueVariableNames[0]),
                                        indexAP.read()
                                ),
                                "length"
                        )
                )
        );

        // int newValueArraysSize = 2 * currentValueArraysSize;
        ScalarVariableAccessPath newValueArraysSize =
                new ScalarVariableAccessPath("newValueArraysSize", P_INT);
        recordStoreExtendArrays.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), newValueArraysSize.getType()),
                        newValueArraysSize.getVariableName(),
                        mul(
                                getLocation(),
                                createIntegerLiteral(getLocation(), 2),
                                currentValueArraysSize.read()
                        )
                )
        );

        for (int i = 0; i < this.valueVariableNames.length; i++) {
            String valueVariableName = this.valueVariableNames[i];
            String tempVarName = "temp_" + valueVariableName;

            recordStoreExtendArrays.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), primitiveArrayTypeForPrimitive(this.valueTypes[i])),
                            tempVarName,
                            createNewPrimitiveArray(
                                    getLocation(),
                                    toJavaPrimitive(this.valueTypes[i]),
                                    newValueArraysSize.read()
                            )
                    )
            );

            recordStoreExtendArrays.addStatement(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[] {
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            createThisFieldAccess(getLocation(), valueVariableName),
                                            indexAP.read()
                                    ),
                                    createIntegerLiteral(getLocation(), 0),
                                    createAmbiguousNameRef(getLocation(), tempVarName),
                                    createIntegerLiteral(getLocation(), 0),
                                    currentValueArraysSize.read()
                            }
                    )
            );

            recordStoreExtendArrays.addStatement(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createThisFieldAccess(getLocation(), valueVariableName),
                                    indexAP.read()
                            ),
                            createAmbiguousNameRef(getLocation(), tempVarName)
                    )
            );
        }

        // Insert the record values and increment the correct record count
        // this.values_record_ord_i[index][insertion_index] = record_ord_i;
        for (int i = 0; i < valueVariableNames.length; i++) {
            associateMethodBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            createThisFieldAccess(getLocation(), valueVariableNames[i]),
                                            indexAP.read()
                                    ),
                                    insertionIndexAP.read()
                            ),
                            createAmbiguousNameRef(getLocation(), formalParameters[i + 2].name)
                    )
            );
        }

        // this.keysRecordCount[index]++;
        associateMethodBody.add(
                postIncrementStm(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), keysRecordCountAP.getVariableName()),
                                indexAP.read()
                        )
                )
        );

        // Invoke the putHashEntry method if the records was a new record
        // if (newEntry) {
        //     boolean rehashOnCollision = this.numberOfRecords > (3 * this.hashTable.length) / 4;
        //     putHashEntry(key, preHash, newIndex, rehashOnCollision);
        // }
        Java.Block hashMaintentanceBlock = new Java.Block(getLocation());

        hashMaintentanceBlock.addStatement(
                createLocalVariable(
                        getLocation(),
                        createPrimitiveType(getLocation(), Java.Primitive.BOOLEAN),
                        "rehashOnCollision",
                        gt(
                                getLocation(),
                                createThisFieldAccess(getLocation(), numberOfRecordsAP.getVariableName()),
                                div(
                                        getLocation(),
                                        mul(
                                                getLocation(),
                                                createIntegerLiteral(getLocation(), 3),
                                                new Java.FieldAccessExpression(
                                                        getLocation(),
                                                        createThisFieldAccess(
                                                                getLocation(),
                                                                hashTableAP.getVariableName()
                                                        ),
                                                        "length"
                                                )
                                        ),
                                        createIntegerLiteral(getLocation(), 4)
                                )
                        )
                )
        );

        hashMaintentanceBlock.addStatement(
                createMethodInvocationStm(
                        getLocation(),
                        new Java.ThisReference(getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        new Java.Rvalue[] {
                                createAmbiguousNameRef(getLocation(), formalParameters[0].name),
                                createAmbiguousNameRef(getLocation(), formalParameters[1].name),
                                indexAP.read(),
                                createAmbiguousNameRef(getLocation(), "rehashOnCollision")
                        }
                )
        );

        associateMethodBody.add(
                createIf(
                        getLocation(),
                        newEntryAP.read(),
                        hashMaintentanceBlock
                )
        );


        // public void associate([keyType] key, long preHash, [values ...])
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                ASSOCIATE_METHOD_NAME,
                createFormalParameters(getLocation(), formalParameters),
                associateMethodBody
        );
    }

    /**
     * Method to generate the "find" method for the generated map type, which finds the index
     * at which the records for a given key are stored.
     */
    private void generateFindMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[2];

        formalParameters[0] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_LONG),
                "preHash"
        );

        // Create the method body
        List<Java.Statement> findMethodBody = new ArrayList<>();

        // int htIndex = "hash"(preHash);
        ScalarVariableAccessPath htIndex = new ScalarVariableAccessPath("htIndex", P_INT);
        findMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), htIndex.getType()),
                        htIndex.getVariableName(),
                        generatePreHashToHashStatement(
                                createAmbiguousNameRef(getLocation(), formalParameters[1].name))
                )
        );

        // int initialIndex = this.hashTable[htIndex];
        ScalarVariableAccessPath initialIndex = new ScalarVariableAccessPath("initialIndex", P_INT);
        findMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), initialIndex.getType()),
                        initialIndex.getVariableName(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(),
                                        hashTableAP.getVariableName()
                                ),
                                htIndex.read()
                        )
                )
        );

        // Check if the hash-table contains an entry for the initial index, otherwise we know the key
        // can never be in the map
        // if (initialIndex == -1) return -1;
        findMethodBody.add(
                createIf(
                        getLocation(),
                        eq(
                                getLocation(),
                                initialIndex.read(),
                                createIntegerLiteral(getLocation(), -1)
                        ),
                        createReturnStm(
                                getLocation(),
                                createIntegerLiteral(getLocation(), -1)
                        )
                )
        );

        // Otherwise follow next-pointers while necessary
        // int currentIndex = initialIndex;
        // while (this.keys[currentIndex] != key) {
        //     int potentialNextIndex = this.next[currentIndex];
        //     if (potentialNextIndex == -1)
        //         return -1;
        //     else
        //         currentIndex = potentialNextIndex;
        // }
        ScalarVariableAccessPath currentIndex = new ScalarVariableAccessPath("currentIndex", P_INT);
        findMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), currentIndex.getType()),
                        currentIndex.getVariableName(),
                        initialIndex.read()
                )
        );

        Java.Block nextPointerLoopBody = new Java.Block(getLocation());
        findMethodBody.add(
                createWhileLoop(
                        getLocation(),
                        neq(
                                getLocation(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                                        currentIndex.read()
                                ),
                                createAmbiguousNameRef(getLocation(), formalParameters[0].name)
                        ),
                        nextPointerLoopBody
                )
        );

        ScalarVariableAccessPath pni = new ScalarVariableAccessPath("potentialNextIndex", P_INT);
        nextPointerLoopBody.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), pni.getType()),
                        pni.getVariableName(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                                currentIndex.read()
                        )
                )
        );

        nextPointerLoopBody.addStatement(
                createIf(
                        getLocation(),
                        eq(getLocation(), pni.read(), createIntegerLiteral(getLocation(), -1)),
                        createReturnStm(getLocation(), createIntegerLiteral(getLocation(), -1)),
                        createVariableAssignmentStm(getLocation(), currentIndex.write(), pni.read())
                )
        );

        // We found the index: return currentIndex;
        findMethodBody.add(createReturnStm(getLocation(), currentIndex.read()));

        // private int find([keyType key], long preHash)
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                createPrimitiveType(getLocation(), Java.Primitive.INT),
                FIND_METHOD_NAME,
                createFormalParameters(getLocation(), formalParameters),
                findMethodBody
        );

    }

    /**
     * Method to generate the "growArrays" method to grow the "key", "next" and "value" arrays when
     * they become too small.
     */
    private void generateGrowArraysMethod() {
        List<Java.Statement> growArraysMethodBody = new ArrayList<>();

        // int currentSize = this.keys.length;
        ScalarVariableAccessPath currentSize = new ScalarVariableAccessPath("currentSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), currentSize.getType()),
                        currentSize.getVariableName(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(), keysAP.getVariableName()
                                ),
                                "length"
                        )
                )
        );

        // int newSize = currentSize << 1;
        ScalarVariableAccessPath newSize = new ScalarVariableAccessPath("newSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), newSize.getType()),
                        newSize.getVariableName(),
                        lShift(getLocation(), currentSize.read(), createIntegerLiteral(getLocation(), 1))
                )
        );

        // if (newSize > Integer.MAX_VALUE - 1)
        //     throw new UnsupportedOperationException("Map has grown too large");
        growArraysMethodBody.add(
                createIf(
                        getLocation(),
                        gt(
                                getLocation(),
                                newSize.read(),
                                sub(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "Integer.MAX_VALUE"),
                                        createIntegerLiteral(getLocation(), 1)
                                )
                        ),
                        new Java.ThrowStatement(
                                getLocation(),
                                createClassInstance(
                                        getLocation(),
                                        createReferenceType(getLocation(), "java.lang.UnsupportedOperationException"),
                                        new Java.Rvalue[] {
                                                createStringLiteral(
                                                        getLocation(),
                                                        "\"Map has grown too large\""
                                                )
                                        }
                                )
                        )
                )
        );

        // Grow, copy and fill the keys array
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), primitiveArrayTypeForPrimitive(this.keyType)),
                        "newKeys",
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(this.keyType),
                                newSize.read()
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                                createIntegerLiteral(getLocation(), 0),
                                createAmbiguousNameRef(getLocation(), "newKeys"),
                                createIntegerLiteral(getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createAmbiguousNameRef(getLocation(), "newKeys"),
                                currentSize.read(),
                                newSize.read(),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                        createAmbiguousNameRef(getLocation(), "newKeys")
                )
        );

        // Grow and copy the keysRecordCount array
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), keysRecordCountAP.getType()),
                        "newKeysRecordCount",
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(keysRecordCountAP.getType())),
                                newSize.read()
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), keysRecordCountAP.getVariableName()),
                                createIntegerLiteral(getLocation(), 0),
                                createAmbiguousNameRef(getLocation(), "newKeysRecordCount"),
                                createIntegerLiteral(getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createThisFieldAccess(getLocation(), keysRecordCountAP.getVariableName()),
                        createAmbiguousNameRef(getLocation(), "newKeysRecordCount")
                )
        );

        // Grow, copy and fill the next array
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), primitiveArrayTypeForPrimitive(this.keyType)),
                        "newNext",
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(this.keyType),
                                newSize.read()
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                                createIntegerLiteral(getLocation(), 0),
                                createAmbiguousNameRef(getLocation(), "newNext"),
                                createIntegerLiteral(getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createAmbiguousNameRef(getLocation(), "newNext"),
                                currentSize.read(),
                                newSize.read(),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                        createAmbiguousNameRef(getLocation(), "newNext")
                )
        );

        // Grow and copy all the value arrays
        for (int i = 0; i < this.valueVariableNames.length; i++) {
            String name = this.valueVariableNames[i];
            String newName = "new_" + name;

            growArraysMethodBody.add(
                    createLocalVariable(
                            getLocation(),
                            createNestedPrimitiveArrayType(
                                    getLocation(),
                                    toJavaPrimitive(this.valueTypes[i])
                            ),
                            newName,
                            createNew2DPrimitiveArray(
                                    getLocation(),
                                    toJavaPrimitive(this.valueTypes[i]),
                                    newSize.read(),
                                    createIntegerLiteral(getLocation(), 2)
                            )
                    )
            );

            growArraysMethodBody.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[] {
                                    createThisFieldAccess(getLocation(), name),
                                    createIntegerLiteral(getLocation(), 0),
                                    createAmbiguousNameRef(getLocation(), newName),
                                    createIntegerLiteral(getLocation(), 0),
                                    currentSize.read()
                            }
                    )
            );

            growArraysMethodBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(getLocation(), name),
                            createAmbiguousNameRef(getLocation(), newName)
                    )
            );
        }

        // private void growArrays()
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                GROW_ARRAYS_METHOD_NAME,
                createFormalParameters(getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
                growArraysMethodBody
        );
    }

    /**
     * Method to generate the "putHashEntry" method, which associates indices of the keys/values
     * arras to specific keys.
     */
    private void generatePutHashEntryMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[4];

        formalParameters[0] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_LONG),
                "preHash"
        );

        formalParameters[2] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_INT),
                "index"
        );

        formalParameters[3] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_BOOLEAN),
                "rehashOnCollision"
        );

        // Generate the method body
        List<Java.Statement> putHEMethodBody = new ArrayList<>();

        // int htIndex = "hash"(preHash);
        ScalarVariableAccessPath htIndex = new ScalarVariableAccessPath("htIndex", P_INT);
        putHEMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), htIndex.getType()),
                        htIndex.getVariableName(),
                        this.generatePreHashToHashStatement(
                                createAmbiguousNameRef(getLocation(), formalParameters[1].name)
                        )
                )
        );

        // int initialIndex = this.hashTable[htIndex];
        ScalarVariableAccessPath initialIndex = new ScalarVariableAccessPath("initialIndex", P_INT);
        putHEMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), initialIndex.getType()),
                        initialIndex.getVariableName(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                                htIndex.read()
                        )
                )
        );

        // If there is a free entry, simply store the new index there
        // if (initialIndex == -1) {
        //     this.hashTable[htIndex] = index;
        //     return;
        // }
        Java.Block addIndexToFreeEntryBody = new Java.Block(getLocation());
        addIndexToFreeEntryBody.addStatement(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                                htIndex.read()
                        ),
                        createAmbiguousNameRef(getLocation(), formalParameters[2].name)
                )
        );

        addIndexToFreeEntryBody.addStatement(createReturnStm(getLocation(), null));

        putHEMethodBody.add(
                createIf(
                        getLocation(),
                        eq(getLocation(), initialIndex.read(), createIntegerLiteral(getLocation(), -1)),
                        addIndexToFreeEntryBody
                )
        );

        // Otherwise we have a collision, so rehash if we were instructed to do so
        Java.Block rehashOnCollisionBody = new Java.Block(getLocation());
        rehashOnCollisionBody.addStatement(
                createMethodInvocationStm(
                        getLocation(),
                        new Java.ThisReference(getLocation()),
                        REHASH_METHOD_NAME
                )
        );

        rehashOnCollisionBody.addStatement(createReturnStm(getLocation(), null));

        putHEMethodBody.add(
                createIf(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), formalParameters[3].name),
                        rehashOnCollisionBody
                )
        );

        // Otherwise store the index in the first free "next" entry in the probe sequence
        // int currentIndex = initialIndex;
        ScalarVariableAccessPath currentIndex = new ScalarVariableAccessPath("currentIndex", P_INT);
        putHEMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), currentIndex.getType()),
                        currentIndex.getVariableName(),
                        initialIndex.read()
                )
        );

        // while (this.keys[currentIndex] != key && this.next[currentIndex] != -1) {
        //     currentIndex = this.next[currentIndex];
        // }
        putHEMethodBody.add(
                createWhileLoop(
                        getLocation(),
                        and(
                                getLocation(),
                                neq(
                                        getLocation(),
                                        createArrayElementAccessExpr(
                                                getLocation(),
                                                createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                                                currentIndex.read()
                                        ),
                                        createAmbiguousNameRef(getLocation(), formalParameters[0].name)
                                ),
                                neq(
                                        getLocation(),
                                        createArrayElementAccessExpr(
                                                getLocation(),
                                                createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                                                currentIndex.read()
                                        ),
                                        createIntegerLiteral(getLocation(), -1)
                                )
                        ),
                        createVariableAssignmentStm(
                                getLocation(),
                                currentIndex.write(),
                                createArrayElementAccessExpr(
                                        getLocation(),
                                        createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                                        currentIndex.read()
                                )
                        )
                )
        );

        // At this point CurrentIndex has property that next[currentIndex] == -1, so we update it to
        // point to index to ensure the collision list is correct
        // this.next[currentIndex] = index;
        putHEMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(),
                                        nextArrayAP.getVariableName()
                                ),
                                currentIndex.read()
                        ),
                        createAmbiguousNameRef(getLocation(), formalParameters[2].name)
                )
        );

        // private void putHashEntry([keyType key], long preHash, int index, boolean rehashOnCollision)
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                PUT_HASH_ENTRY_METHOD_NAME,
                createFormalParameters(getLocation(), formalParameters),
                putHEMethodBody
        );
    }

    /**
     * Method to generate the "rehash" method, which constructs a completely new hash-table at a
     * larger size to avoid future hash collisions.
     */
    private void generateRehashMethod() {
        List<Java.Statement> rehashMethodBody = new ArrayList<>();

        // Compute the new hash-table size as the smallest power of 2 greater than this.numberOfRecords
        // int size = this.hashTable.length;
        ScalarVariableAccessPath size = new ScalarVariableAccessPath("size", P_INT);
        rehashMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), size.getType()),
                        size.getVariableName(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                                "length"
                        )
                )
        );

        // while (size <= this.numberOfRecords) size = (size << 1);
        rehashMethodBody.add(
                createWhileLoop(
                        getLocation(),
                        le(
                                getLocation(),
                                size.read(),
                                createThisFieldAccess(getLocation(), numberOfRecordsAP.getVariableName())
                        ),
                        createVariableAssignmentStm(
                                getLocation(),
                                size.write(),
                                lShift(getLocation(), size.read(), createIntegerLiteral(getLocation(), 1))
                        )
                )
        );

        // Add some additional size to prevent collisions
        // size = size << 1;
        rehashMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        size.write(),
                        lShift(getLocation(), size.read(), createIntegerLiteral(getLocation(), 1))
                )
        );

        // Initialise the new hash-table and next array
        // this.hashTable = new int[size];
        // Arrays.fill(this.hashTable, -1);
        // Arrays.fill(this.next, -1);
        rehashMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                        createNewPrimitiveArray(getLocation(), Java.Primitive.INT, size.read())
                )
        );

        rehashMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        rehashMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Finally insert all key-value associations in the new hash-table
        // for (int i = 0; i < this.numberOfRecords; i++) {
        //     [keyType] key = this.keys[i];
        //     long preHash = [hash_function_container].hash(key);
        //     this.putHashEntry(key, preHash, i, false);
        // }
        Java.Block hashAssociationLoopBody = new Java.Block(getLocation());

        ScalarVariableAccessPath indexVar = new ScalarVariableAccessPath("i", P_INT);
        rehashMethodBody.add(
                createForLoop(
                        getLocation(),
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), indexVar.getType()),
                                indexVar.getVariableName(),
                                createIntegerLiteral(getLocation(), 0)
                        ),
                        lt(
                                getLocation(),
                                indexVar.read(),
                                createThisFieldAccess(getLocation(), numberOfRecordsAP.getVariableName())
                        ),
                        postIncrement(getLocation(), indexVar.write()),
                        hashAssociationLoopBody
                )
        );

        hashAssociationLoopBody.addStatement(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), keyType),
                        "key",
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                                indexVar.read()
                        )
                )
        );

        hashAssociationLoopBody.addStatement(
                createLocalVariable(
                        getLocation(),
                        createPrimitiveType(getLocation(), Java.Primitive.LONG),
                        "preHash",
                        createMethodInvocation(
                                getLocation(),
                                switch (keyType) {
                                    case P_INT -> createAmbiguousNameRef(getLocation(), "Int_Hash_Function");
                                    default -> throw new UnsupportedOperationException(
                                            "This key-type is currently not supported by the KeyMultiRecordMapGenerator");
                                },
                                "preHash",
                                new Java.Rvalue[] {
                                        createAmbiguousNameRef(getLocation(), "key")
                                }
                        )
                )
        );

        hashAssociationLoopBody.addStatement(
                createMethodInvocationStm(
                        getLocation(),
                        new Java.ThisReference(getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        new Java.Rvalue[] {
                                createAmbiguousNameRef(getLocation(), "key"),
                                createAmbiguousNameRef(getLocation(), "preHash"),
                                indexVar.read(),
                                new Java.BooleanLiteral(getLocation(), "false")
                        }
                )
        );

        // private void rehash()
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                REHASH_METHOD_NAME,
                createFormalParameters(getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
                rehashMethodBody
        );
    }

    /**
     * Method to generate the public "getIndex" method, which returns the index in the map that
     * contains the values for a given key, or -1 if the map does not contain the key.
     */
    private void generateGetIndexMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[2];

        formalParameters[0] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_LONG),
                "preHash"
        );

        // Generate the method body
        List<Java.Statement> getIndexMethodBody = new ArrayList<>();

        // Check that the provided key is non-negative
        getIndexMethodBody.add(
                generateNonNegativeCheck(
                        createAmbiguousNameRef(getLocation(), formalParameters[0].name)
                )
        );

        // Return the result of the find method
        getIndexMethodBody.add(
                createReturnStm(
                        getLocation(),
                        createMethodInvocation(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                FIND_METHOD_NAME,
                                new Java.Rvalue[] {
                                        createAmbiguousNameRef(getLocation(), formalParameters[0].name),
                                        createAmbiguousNameRef(getLocation(), formalParameters[1].name)
                                }
                        )
                )
        );

        // public int getIndex([keyType key], long preHash)
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createPrimitiveType(getLocation(), Java.Primitive.INT),
                GET_INDEX_METHOD_NAME,
                createFormalParameters(getLocation(), formalParameters),
                getIndexMethodBody
        );
    }

    /**
     * Method to generate the reset method, which "clears" the generated map. This is achieved
     * by simply resetting the keys, keyrsRecordCount and next arrays, as well as the hash-table and
     * the numberOfRecords value.
     * Values thus remain stored in the map, but are inaccessible as a result.
     */
    private void generateResetMethod() {
        List<Java.Statement> resetMethodBody = new ArrayList<>();

        // this.numberOfRecords = 0;
        resetMethodBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createThisFieldAccess(
                                getLocation(),
                                numberOfRecordsAP.getVariableName()
                        ),
                        createIntegerLiteral(getLocation(), 0)
                )
        );

        // Arrays.fill(this.keys, -1);
        resetMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Arrays.fill(this.keysRecordCount, 0);
        resetMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), keysRecordCountAP.getVariableName()),
                                createIntegerLiteral(getLocation(), 0)
                        }
                )
        );

        // Arrays.fill(this.hashTable, -1);
        resetMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Arrays.fill(this.next, -1);
        resetMethodBody.add(
                createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                createThisFieldAccess(getLocation(), nextArrayAP.getVariableName()),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // public void reset()
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                RESET_METHOD_NAME,
                createFormalParameters(getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
                resetMethodBody
        );
    }

    /**
     * Method to generate statements that check that a given {@link Java.Rvalue}
     * is non-negative.
     * @param rValueToCheck The {@link Java.Rvalue} to check for non-negativity.
     * @return The generated check, which throws an exception on a negative key.
     */
    private Java.Statement generateNonNegativeCheck(Java.Rvalue rValueToCheck) {
        // if ([rvalueToCheck] < 0)
        //     throw new IllegalArgumentException("The map expects non-negative keys");
        return createIf(
                getLocation(),
                lt(
                        getLocation(),
                        rValueToCheck,
                        createIntegerLiteral(getLocation(), 0)
                ),
                new Java.ThrowStatement(
                        getLocation(),
                        createClassInstance(
                                getLocation(),
                                createReferenceType(
                                        getLocation(),
                                        "java.lang.IllegalArgumentException"
                                ),
                                new Java.Rvalue[] {
                                        createStringLiteral(getLocation(), "\"The map expects non-negative keys\"")
                                }
                        )
                )
        );
    }

    /**
     * Method to generate the statements that convert a pre-hash value into the actual hash value.
     * @param preHashRValue The pre-hash value to convert.
     * @return The actual hash-value as an r-value.
     */
    private Java.Rvalue generatePreHashToHashStatement(Java.Rvalue preHashRValue) {
        // (int) (preHash & (this.hashTable.length - 1))
        return createCast(
                getLocation(),
                createPrimitiveType(getLocation(), Java.Primitive.INT),
                binAnd(
                        getLocation(),
                        preHashRValue,
                        sub(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        createThisFieldAccess(getLocation(), hashTableAP.getVariableName()),
                                        "length"
                                ),
                                createIntegerLiteral(getLocation(), 1)
                        )
                )
        );

    }

}
