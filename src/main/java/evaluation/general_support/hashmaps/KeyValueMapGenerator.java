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
import static evaluation.codegen.infrastructure.context.QueryVariableType.S_FL_BIN;
import static evaluation.codegen.infrastructure.context.QueryVariableType.S_VARCHAR;
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
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createFloatingPointLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNestedPrimitiveArrayType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNew2DPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveArrayType;
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
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.or;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static evaluation.codegen.infrastructure.janino.JaninoOperatorGen.sub;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createSimpleVariableDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableXorAssignmentStm;
import static evaluation.general_support.hashmaps.CommonMapGenerator.createMapAssignmentRValue;

/**
 * This class provides methods to generate a hash-map implementation for mapping some primitive type
 * keys to a single record-like type containing multiple values, where each field of the record is
 * also a primitive type. To hash keys, a generated map relies on the appropriate hash function
 * definitions for the given key type:
 *  - For int keys, the map uses the {@link Int_Hash_Function}.
 *  - Other key types are currently not yet supported.
 */
public class KeyValueMapGenerator {

    /**
     * The {@link QueryVariableType} indicating the primitive key type of the map to be generated.
     */
    public final QueryVariableType[] keyTypes;

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
     * The names of the arrays storing the record field keys in the map.
     */
    public final String[] keyVariableNames;

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
    private static final String INCREMENT_FOR_KEY_METHOD_NAME = "incrementForKey";
    private static final String FIND_METHOD_NAME = "find";
    private static final String GROW_ARRAYS_METHOD_NAME = "growArrays";
    private static final String PUT_HASH_ENTRY_METHOD_NAME = "putHashEntry";
    private static final String REHASH_METHOD_NAME = "rehash";
    private static final String RESET_METHOD_NAME = "reset";

    /**
     * Instantiate a {@link KeyValueMapGenerator} to generate a map type for specific key and
     * value types.
     * @param keyTypes The key types that are to be used by the generated map.
     * @param valueTypes The value types that records in the map should be built up of.
     */
    public KeyValueMapGenerator(QueryVariableType[] keyTypes, QueryVariableType[] valueTypes) {
        for (QueryVariableType keyType : keyTypes) {
            if (!isPrimitive(keyType) && keyType != S_FL_BIN && keyType != S_VARCHAR)
                throw new IllegalArgumentException("KeyValueMapGenerator expects a primitive key type, not " + keyType);
        }

        for (QueryVariableType valueType : valueTypes) {
            if (!isPrimitive(valueType))
                throw new IllegalArgumentException("KeyValueMapGenerator expects primitive value types, not " + valueType);
        }

        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;

        this.generationFinished = false;

        this.keyVariableNames = new String[keyTypes.length];
        for (int i = 0; i < keyVariableNames.length; i++)
            this.keyVariableNames[i] = "keys_ord_" + i;

        this.valueVariableNames = new String[valueTypes.length];
        for (int i = 0; i < valueVariableNames.length; i++)
            this.valueVariableNames[i] = "values_ord_" + i;
    }

    /**
     * Method to generate the actual map type for the provided specification.
     * @return A {@link Java.ClassDeclaration} defining the configured
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
                "KeyValueMap_" + this.hashCode()
        );

        // Now generate the class body in a step-by-step fashion
        this.generateFieldDeclarations();
        this.generateConstructors();
        this.generateIncrementForKeyMethod();
        this.generateFindMethod();
        this.generateGrowArraysMethod();
        this.generatePutHashEntryMethod();
        this.generateRehashMethod();
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
                createPublicFieldDeclaration(
                        getLocation(),
                        toJavaType(getLocation(), numberOfRecordsAP.getType()),
                        createSimpleVariableDeclaration(
                                getLocation(),
                                numberOfRecordsAP.getVariableName()
                        )
                )
        );

        // For each field of the key types, create an array
        for (int i = 0; i < this.keyVariableNames.length; i++) {
            Java.Type arrayType;
            if (this.keyTypes[i] == S_FL_BIN || this.keyTypes[i] == S_VARCHAR)
                arrayType = createNestedPrimitiveArrayType(getLocation(), Java.Primitive.BYTE);
            else
                arrayType = createPrimitiveArrayType(getLocation(), toJavaPrimitive(this.keyTypes[i]));

            this.mapDeclaration.addFieldDeclaration(
                    createPublicFieldDeclaration(
                            getLocation(),
                            arrayType,
                            createSimpleVariableDeclaration(
                                    getLocation(),
                                    this.keyVariableNames[i]
                            )
                    )
            );
        }

        // For each field of the value types, create an array
        for (int i = 0; i < this.valueVariableNames.length; i++) {
            this.mapDeclaration.addFieldDeclaration(
                    createPublicFieldDeclaration(
                            getLocation(),
                            createPrimitiveArrayType(
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

        // Initialise the key arrays
        for (int i = 0; i < this.keyVariableNames.length; i++) {
            String keyVarName = this.keyVariableNames[i];
            QueryVariableType keyVarPrimType = this.keyTypes[i];

            Java.NewArray theArray;
            if (keyVarPrimType == S_FL_BIN || keyVarPrimType == S_VARCHAR)
                theArray = createNew2DPrimitiveArray(getLocation(), Java.Primitive.BYTE, capacityParameterAP.read());
            else
                theArray = createNewPrimitiveArray(getLocation(), toJavaPrimitive(keyVarPrimType), capacityParameterAP.read());

            constructorBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            new Java.FieldAccessExpression(
                                    getLocation(),
                                    new Java.ThisReference(getLocation()),
                                    keyVarName
                            ),
                            theArray
                    )
            );

            Java.Rvalue initialisationLiteral = switch (keyVarPrimType) {
                case P_DOUBLE -> createFloatingPointLiteral(getLocation(), -1d);
                case P_INT, P_INT_DATE -> createIntegerLiteral(getLocation(), -1);
                case S_FL_BIN, S_VARCHAR -> new Java.NullLiteral(getLocation());

                default -> throw new UnsupportedOperationException(
                        "KeyValueMapGenerator.generateConstructors does not support this key type: " + keyVarPrimType);
            };

            constructorBody.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "Arrays"),
                            "fill",
                            new Java.Rvalue[] {
                                    new Java.FieldAccessExpression(
                                            getLocation(),
                                            new Java.ThisReference(getLocation()),
                                            keyVarName
                                    ),
                                    initialisationLiteral
                            }
                    )
            );
        }

        // Initialise each of the value arrays
        for (int i = 0; i < this.valueVariableNames.length; i++) {
            constructorBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(
                                    getLocation(),
                                    this.valueVariableNames[i]
                            ),
                            createNewPrimitiveArray(
                                    getLocation(),
                                    toJavaPrimitive(valueTypes[i]),
                                    capacityParameterAP.read()
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
     * Method to generate the "incrementForKey" method for the generated map type, which increments
     * all values associated to a specific key by a certain value (or associates a value if the
     * key is not yet present in the map).
     */
    private void generateIncrementForKeyMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[this.keyTypes.length + 1 + this.valueTypes.length];
        int currentFormalParamIndex = 0;

        for (int i = 0; i < this.keyTypes.length; i++) {
            formalParameters[currentFormalParamIndex++] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), this.keyTypes[i]),
                    "key_ord_" + i
            );
        }

        formalParameters[currentFormalParamIndex++] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_LONG),
                "preHash"
        );

        int valueOrdsFormalParametersBaseIndex = currentFormalParamIndex;
        for (int i = 0; i < this.valueTypes.length; i++) {
            formalParameters[currentFormalParamIndex++] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), this.valueTypes[i]),
                    "value_ord_" + i
            );
        }

        // Create the method body
        List<Java.Statement> incrementForKeyMethodBody = new ArrayList<>();

        // Create the first key to be non-negative
        if (this.keyTypes[0] == S_FL_BIN || this.keyTypes[0] == S_VARCHAR) {
            incrementForKeyMethodBody.add(
                    generateNonNullCheck(
                            createAmbiguousNameRef(
                                    getLocation(),
                                    formalParameters[0].name
                            )
                    )
            );
        } else {
            incrementForKeyMethodBody.add(
                    generateNonNegativeCheck(
                            createAmbiguousNameRef(
                                    getLocation(),
                                    formalParameters[0].name
                            )
                    )
            );
        }

        // Declare the index variable and check whether the key is already contained in the map
        // int index = find(keys ..., preHash);
        ScalarVariableAccessPath indexAP = new ScalarVariableAccessPath("index", P_INT);
        Java.Rvalue[] findMethodArguments = new Java.Rvalue[this.keyVariableNames.length + 1];
        for (int i = 0; i < findMethodArguments.length; i++)
            findMethodArguments[i] = createAmbiguousNameRef(
                    getLocation(),
                    formalParameters[i].name
            );

        incrementForKeyMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), indexAP.getType()),
                        indexAP.getVariableName(),
                        createMethodInvocation(
                                getLocation(),
                                new Java.ThisReference(getLocation()),
                                FIND_METHOD_NAME,
                                findMethodArguments
                        )
                )
        );

        // If so, set the index variable to the existing entry, otherwise, allocate a new index
        // boolean newEntry = false;
        // if (index == -1) {
        //     newEntry = true;
        //     index = this.numberOfRecords++;
        //     if (this.keys_ord_0.length == index)
        //         growArrays();
        //     $ for each key ord j $
        //     this.keys_ord_j[index] = key_j; <-- Note this assignment is slightly different for byte[]
        // }
        ScalarVariableAccessPath newEntryAP = new ScalarVariableAccessPath("newEntry", P_BOOLEAN);
        incrementForKeyMethodBody.add(
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
                                                this.keyVariableNames[0]
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
        for (int i = 0; i < this.keyVariableNames.length; i++) {
            allocateIndexBody.addStatement(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createThisFieldAccess(
                                            getLocation(),
                                            this.keyVariableNames[i]
                                    ),
                                    indexAP.read()
                            ),
                            createMapAssignmentRValue( this.keyTypes[i], formalParameters[i].name, allocateIndexBody)
                    )
            );
        }

        incrementForKeyMethodBody.add(
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

        // Increment this key's values by the provided amounts
        // this.values_ord_i[index] += value_ord_i;
        for (int i = 0; i < valueVariableNames.length; i++) {
            incrementForKeyMethodBody.add(
                    createVariableAdditionAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createThisFieldAccess(getLocation(), valueVariableNames[i]),
                                    indexAP.read()
                            ),
                            createAmbiguousNameRef(getLocation(), formalParameters[valueOrdsFormalParametersBaseIndex + i].name)
                    )
            );
        }

        // Invoke the putHashEntry method if the records was a new record
        // if (newEntry) {
        //     boolean rehashOnCollision = this.numberOfRecords > (3 * this.hashTable.length) / 4;
        //     putHashEntry(keys ..., preHash, newIndex, rehashOnCollision);
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

        Java.Rvalue[] putHashEntryArguments = new Java.Rvalue[this.keyVariableNames.length + 3];
        int currentPutHashEntryArgumentIndex = 0;
        for (int i = 0; i < this.keyVariableNames.length + 1; i++) // Keys and pre-hash value
            putHashEntryArguments[currentPutHashEntryArgumentIndex++] = createAmbiguousNameRef(getLocation(), formalParameters[i].name);
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] = indexAP.read();
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] = createAmbiguousNameRef(getLocation(), "rehashOnCollision");

        hashMaintentanceBlock.addStatement(
                createMethodInvocationStm(
                        getLocation(),
                        new Java.ThisReference(getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        putHashEntryArguments
                )
        );

        incrementForKeyMethodBody.add(
                createIf(
                        getLocation(),
                        newEntryAP.read(),
                        hashMaintentanceBlock
                )
        );

        // public void incrementForKey([keys ...], long preHash, [values ...])
        createMethod(
                getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                INCREMENT_FOR_KEY_METHOD_NAME,
                createFormalParameters(getLocation(), formalParameters),
                incrementForKeyMethodBody
        );
    }

    /**
     * Method to generate the "find" method for the generated map type, which finds the index
     * at which the records for a given key are stored.
     */
    private void generateFindMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[this.keyTypes.length + 1];
        int currentFormalParameterIndex = 0;

        for (int i = 0; i < this.keyTypes.length; i++) {
            formalParameters[currentFormalParameterIndex++] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), this.keyTypes[i]),
                    "key_ord_" + i
            );
        }

        int preHashFormalParamIndex = currentFormalParameterIndex++;
        formalParameters[preHashFormalParamIndex] = createFormalParameter(
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
                                createAmbiguousNameRef(getLocation(), formalParameters[preHashFormalParamIndex].name))
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
        // while ($ disjunction of key ords $ this.keys_ord_i[currentIndex] != key_ord_i) {
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

        // Generate the disjunction for the while-loop guard
        Java.Rvalue nextLoopDisjunction;
        if (this.keyTypes[0] == S_FL_BIN || this.keyTypes[0] == S_VARCHAR) {
            nextLoopDisjunction = not(
                    getLocation(),
                    createMethodInvocation(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "Arrays"),
                            "equals",
                            new Java.Rvalue[] {
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            createThisFieldAccess(getLocation(), this.keyVariableNames[0]),
                                            currentIndex.read()
                                    ),
                                    createAmbiguousNameRef(getLocation(), formalParameters[0].name)
                            }
                    )
            );

        } else {
            nextLoopDisjunction = neq(
                    getLocation(),
                    createArrayElementAccessExpr(
                            getLocation(),
                            createThisFieldAccess(getLocation(), this.keyVariableNames[0]),
                            currentIndex.read()
                    ),
                    createAmbiguousNameRef(getLocation(), formalParameters[0].name)
            );

        }

        for (int i = 1; i < this.keyVariableNames.length; i++) {
            Java.Rvalue conditionCheck;
            if (this.keyTypes[i] == S_FL_BIN || this.keyTypes[i] == S_VARCHAR) {
                conditionCheck = not(
                        getLocation(),
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "Arrays"),
                                "equals",
                                new Java.Rvalue[] {
                                        createArrayElementAccessExpr(
                                                getLocation(),
                                                createThisFieldAccess(getLocation(), this.keyVariableNames[i]),
                                                currentIndex.read()
                                        ),
                                        createAmbiguousNameRef(getLocation(), formalParameters[i].name)
                                }
                        )
                );

            } else {
                conditionCheck = neq(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), this.keyVariableNames[i]),
                                currentIndex.read()
                        ),
                        createAmbiguousNameRef(getLocation(), formalParameters[i].name)
                );

            }

            nextLoopDisjunction = or(
                    getLocation(),
                    nextLoopDisjunction,
                    conditionCheck
            );
        }

        Java.Block nextPointerLoopBody = new Java.Block(getLocation());
        findMethodBody.add(
                createWhileLoop(
                        getLocation(),
                        nextLoopDisjunction,
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

        // private int find([keys ...], long preHash)
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

        // int currentSize = this.keys_ord_0.length;
        ScalarVariableAccessPath currentSize = new ScalarVariableAccessPath("currentSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), currentSize.getType()),
                        currentSize.getVariableName(),
                        new Java.FieldAccessExpression(
                                getLocation(),
                                createThisFieldAccess(getLocation(), this.keyVariableNames[0]),
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

        // Grow, copy and fill the key arrays
        for (int i = 0; i < this.keyVariableNames.length; i++) {
            String keyVarName = this.keyVariableNames[i];
            String newKeyVarName = keyVarName + "_new";
            QueryVariableType keyPrimType = this.keyTypes[i];

            Java.Type arrayType;
            Java.NewArray theArray;
            if (keyPrimType == S_FL_BIN || keyPrimType == S_VARCHAR) {
                arrayType = createNestedPrimitiveArrayType(getLocation(), Java.Primitive.BYTE);
                theArray = createNew2DPrimitiveArray(getLocation(), Java.Primitive.BYTE, newSize.read());
            }
            else {
                arrayType = createPrimitiveArrayType(getLocation(), toJavaPrimitive(keyPrimType));
                theArray = createNewPrimitiveArray(getLocation(), toJavaPrimitive(keyPrimType), newSize.read());
            }

            growArraysMethodBody.add(
                    createLocalVariable(
                            getLocation(),
                            arrayType,
                            newKeyVarName,
                            theArray
                    )
            );

            growArraysMethodBody.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    createThisFieldAccess(getLocation(), keyVarName),
                                    createIntegerLiteral(getLocation(), 0),
                                    createAmbiguousNameRef(getLocation(), newKeyVarName),
                                    createIntegerLiteral(getLocation(), 0),
                                    currentSize.read()
                            }
                    )
            );

            Java.Rvalue initialisationLiteral = switch (keyPrimType) {
                case P_DOUBLE -> createFloatingPointLiteral(getLocation(), -1d);
                case P_INT, P_INT_DATE -> createIntegerLiteral(getLocation(), -1);
                case S_FL_BIN, S_VARCHAR -> new Java.NullLiteral(getLocation());

                default -> throw new UnsupportedOperationException(
                        "KeyValueMapGenerator.generateGrowArraysMethod does not support this key type: " + keyPrimType);
            };

            growArraysMethodBody.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "Arrays"),
                            "fill",
                            new Java.Rvalue[]{
                                    createAmbiguousNameRef(getLocation(), newKeyVarName),
                                    currentSize.read(),
                                    newSize.read(),
                                    initialisationLiteral
                            }
                    )
            );

            growArraysMethodBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(getLocation(), keyVarName),
                            createAmbiguousNameRef(getLocation(), newKeyVarName)
                    )
            );
        }

        // Grow, copy and fill the next array
        growArraysMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        toJavaType(getLocation(), primitiveArrayTypeForPrimitive(P_INT)),
                        "newNext",
                        createNewPrimitiveArray(
                                getLocation(),
                                toJavaPrimitive(P_INT),
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
                            createPrimitiveArrayType(
                                    getLocation(),
                                    toJavaPrimitive(this.valueTypes[i])
                            ),
                            newName,
                            createNewPrimitiveArray(
                                    getLocation(),
                                    toJavaPrimitive(this.valueTypes[i]),
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
                new Java.FunctionDeclarator.FormalParameter[this.keyTypes.length + 3];
        int currentFormalParamIndex = 0;

        for (int i = 0; i < this.keyTypes.length; i++) {
            formalParameters[currentFormalParamIndex++] = createFormalParameter(
                    getLocation(),
                    toJavaType(getLocation(), this.keyTypes[i]),
                    "key_ord_" + i
            );
        }

        int prehashFormalParamIndex = currentFormalParamIndex++;
        formalParameters[prehashFormalParamIndex] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_LONG),
                "preHash"
        );

        int indexFormalParamIndex = currentFormalParamIndex++;
        formalParameters[indexFormalParamIndex] = createFormalParameter(
                getLocation(),
                toJavaType(getLocation(), P_INT),
                "index"
        );

        int rehashOnCollisionFormalParamIndex = currentFormalParamIndex++;
        formalParameters[rehashOnCollisionFormalParamIndex] = createFormalParameter(
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
                                createAmbiguousNameRef(getLocation(), formalParameters[prehashFormalParamIndex].name)
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
                        createAmbiguousNameRef(getLocation(), formalParameters[indexFormalParamIndex].name)
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
                        createAmbiguousNameRef(getLocation(), formalParameters[rehashOnCollisionFormalParamIndex].name),
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

        // while ($ disjunction per key ord i $ this.keys_ord_i[currentIndex] != key_ord_i $$ && this.next[currentIndex] != -1) {
        //     currentIndex = this.next[currentIndex];
        // }
        Java.Rvalue probeWhileLoopDisjunction;
        if (this.keyTypes[0] == S_FL_BIN || this.keyTypes[0] == S_VARCHAR) {
            probeWhileLoopDisjunction = not(
                    getLocation(),
                    createMethodInvocation(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "Arrays"),
                            "equals",
                            new Java.Rvalue[] {
                                    createArrayElementAccessExpr(
                                            getLocation(),
                                            createThisFieldAccess(getLocation(), this.keyVariableNames[0]),
                                            currentIndex.read()
                                    ),
                                    createAmbiguousNameRef(getLocation(), formalParameters[0].name)
                            }
                    )
            );

        } else {
            probeWhileLoopDisjunction = neq(
                    getLocation(),
                    createArrayElementAccessExpr(
                            getLocation(),
                            createThisFieldAccess(getLocation(), this.keyVariableNames[0]),
                            currentIndex.read()
                    ),
                    createAmbiguousNameRef(getLocation(), formalParameters[0].name)
            );

        }

        for (int i = 1; i < this.keyVariableNames.length; i++) {
            Java.Rvalue condition;
            if (this.keyTypes[i] == S_FL_BIN || this.keyTypes[i] == S_VARCHAR) {
                condition = not(
                        getLocation(),
                        createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "Arrays"),
                                "equals",
                                new Java.Rvalue[] {
                                        createArrayElementAccessExpr(
                                                getLocation(),
                                                createThisFieldAccess(getLocation(), this.keyVariableNames[i]),
                                                currentIndex.read()
                                        ),
                                        createAmbiguousNameRef(getLocation(), formalParameters[i].name)
                                }
                        )
                );

            } else {
                condition = neq(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), this.keyVariableNames[i]),
                                currentIndex.read()
                        ),
                        createAmbiguousNameRef(getLocation(), formalParameters[i].name)
                );

            }

            probeWhileLoopDisjunction = or(
                    getLocation(),
                    probeWhileLoopDisjunction,
                    condition
            );
        }

        putHEMethodBody.add(
                createWhileLoop(
                        getLocation(),
                        and(
                                getLocation(),
                                probeWhileLoopDisjunction,
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
                        createAmbiguousNameRef(getLocation(), formalParameters[indexFormalParamIndex].name)
                )
        );

        // private void putHashEntry([keys ...], long preHash, int index, boolean rehashOnCollision)
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
        //     $ for each key ord j $
        //       [keyType] key_ord_j = this.keys_ord_j[i];
        //     long preHash = [hash_function_container].hash(key_ord_0);
        //     $ for each remaining key ord j $
        //       preHash ^= [hash_function_container].hash(key_ord_j);
        //     this.putHashEntry([keys ...], preHash, i, false);
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

        ScalarVariableAccessPath[] keyVarAPs = new ScalarVariableAccessPath[this.keyVariableNames.length];
        for (int i = 0; i < this.keyVariableNames.length; i++) {
            String keyName = "key_ord_" + i;
            QueryVariableType keyType = this.keyTypes[i];

            keyVarAPs[i] = new ScalarVariableAccessPath(keyName, keyType);
            hashAssociationLoopBody.addStatement(
                    createLocalVariable(
                            getLocation(),
                            toJavaType(getLocation(), keyType),
                            keyName,
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    createThisFieldAccess(getLocation(), this.keyVariableNames[i]),
                                    indexVar.read()
                            )
                    )
            );
        }

        for (int i = 0; i < this.keyVariableNames.length; i++) {
            Java.MethodInvocation hashMethodInvocation = createMethodInvocation(
                    getLocation(),
                    switch (this.keyTypes[i]) {
                        case P_DOUBLE -> createAmbiguousNameRef(getLocation(), "Double_Hash_Function");
                        case P_INT, P_INT_DATE -> createAmbiguousNameRef(getLocation(), "Int_Hash_Function");
                        case S_FL_BIN, S_VARCHAR -> createAmbiguousNameRef(getLocation(), "Char_Arr_Hash_Function");

                        default -> throw new UnsupportedOperationException(
                                "This key-type is currently not supported by the KeyValueMapGenerator");
                    },
                    "preHash",
                    new Java.Rvalue[] {
                            keyVarAPs[i].read()
                    }
            );

            if (i == 0) {
                // On the first key ordinal, we need to initialise the preHash variable
                hashAssociationLoopBody.addStatement(
                        createLocalVariable(
                                getLocation(),
                                createPrimitiveType(getLocation(), Java.Primitive.LONG),
                                "preHash",
                                hashMethodInvocation
                        )
                );
            } else {
                // On the remaining key ordinals, we need to "extend" the preHash variable
                hashAssociationLoopBody.addStatement(
                        createVariableXorAssignmentStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "preHash"),
                                hashMethodInvocation
                        )
                );
            }
        }

        Java.Rvalue[] putHashEntryArguments = new Java.Rvalue[keyVarAPs.length + 3];
        int currentPutHashEntryArgumentIndex = 0;
        for (int i = 0; i < keyVarAPs.length; i++)
            putHashEntryArguments[currentPutHashEntryArgumentIndex++] = keyVarAPs[i].read();
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] =
                createAmbiguousNameRef(getLocation(), "preHash");
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] = indexVar.read();
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] =
                new Java.BooleanLiteral(getLocation(), "false");

        hashAssociationLoopBody.addStatement(
                createMethodInvocationStm(
                        getLocation(),
                        new Java.ThisReference(getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        putHashEntryArguments
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
     * Method to generate the reset method, which "clears" the generated map. This is achieved
     * by simply resetting the keys, keysRecordCount and next arrays, as well as the hash-table and
     * the numberOfRecords value. The value arrays are also zeroed.
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

        // Arrays.fill(this.keys_ord_i, -1);
        for (int i = 0; i < this.keyVariableNames.length; i++) {
            QueryVariableType keyType = this.keyTypes[i];
            Java.Rvalue initialisationLiteral = switch (keyType) {
                case P_DOUBLE -> createFloatingPointLiteral(getLocation(), -1d);
                case P_INT, P_INT_DATE -> createIntegerLiteral(getLocation(), -1);
                case S_FL_BIN, S_VARCHAR -> new Java.NullLiteral(getLocation());

                default -> throw new UnsupportedOperationException(
                        "KeyValueMapGenerator.generateResetMethod does not support this key type: " + keyType);
            };

            resetMethodBody.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "Arrays"),
                            "fill",
                            new Java.Rvalue[]{
                                    createThisFieldAccess(getLocation(), this.keyVariableNames[i]),
                                    initialisationLiteral
                            }
                    )
            );
        }

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

        // Zero the value arrays
        for (int i = 0; i < this.valueVariableNames.length; i++) {
            QueryVariableType valueType = this.valueTypes[i];
            Java.Rvalue initialisationLiteral = switch (valueType) {
                case P_DOUBLE, P_FLOAT -> createFloatingPointLiteral(getLocation(), 0d);
                case P_INT, P_LONG -> createIntegerLiteral(getLocation(), 0);

                default -> throw new UnsupportedOperationException(
                        "KeyValueMapGenerator.generateResetMethod does not support this value type: " + valueType);
            };

            resetMethodBody.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), "Arrays"),
                            "fill",
                            new Java.Rvalue[] {
                                    createThisFieldAccess(getLocation(), this.valueVariableNames[i]),
                                    initialisationLiteral
                            }
                    )
            );
        }

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
        //     throw new IllegalArgumentException("The map expects the first key ordinal to be non-negative");
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
                                        createStringLiteral(getLocation(), "\"The map expects the first key ordinal to be non-negative\"")
                                }
                        )
                )
        );
    }

    /**
     * Method to generate statements that check that a given {@link Java.Rvalue}
     * is non-null.
     * @param rValueToCheck The {@link Java.Rvalue} to check for non-null-ity.
     * @return The generated check, which throws an exception on a null key.
     */
    private Java.Statement generateNonNullCheck(Java.Rvalue rValueToCheck) {
        // if ([rvalueToCheck] == null)
        //     throw new IllegalArgumentException("The map expects the first key ordinal to be non-null");
        return createIf(
                getLocation(),
                eq(
                        getLocation(),
                        rValueToCheck,
                        new Java.NullLiteral(getLocation())
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
                                        createStringLiteral(getLocation(), "\"The map expects the first key ordinal to be non-null\"")
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
