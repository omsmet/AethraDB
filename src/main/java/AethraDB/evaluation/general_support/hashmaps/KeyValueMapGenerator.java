package AethraDB.evaluation.general_support.hashmaps;

import AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoControlGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_A_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_BOOLEAN;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_INT;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.P_LONG;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_FL_BIN;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableType.S_VARCHAR;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.isPrimitive;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveMemberTypeForArray;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaPrimitive;
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.toJavaType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createArrayElementAccessExpr;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createIntegerLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createNewPrimitiveArray;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createThisFieldAccess;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createConstructor;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameter;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameters;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethod;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createReturnStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.lt;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createSimpleVariableDeclaration;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAdditionAssignmentStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableXorAssignmentStm;
import static AethraDB.evaluation.general_support.hashmaps.CommonMapGenerator.createMapAssignmentRValue;

/**
 * This class provides methods to generate a hash-map implementation for mapping some primitive type
 * keys to a single record-like type containing multiple values, where each field of the record is
 * also a primitive type. To hash keys, a generated map relies on the appropriate hash function
 * definitions for the given key type:
 *  - For double keys, the map uses the {@link Double_Hash_Function}.
 *  - For byte[] keys, the map uses the {@link Char_Arr_Hash_Function}.
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
    public Java.LocalClassDeclaration mapDeclaration;

    /**
     * The {@link AccessPath} to the variable storing the number of records in the map.
     */
    private static final ScalarVariableAccessPath numberOfRecordsAP =
            new ScalarVariableAccessPath("numberOfRecords", P_INT);

    /**
     * The {@link Java.MemberClassDeclaration} for the records stored in {@code this}.
     */
    public Java.MemberClassDeclaration recordDeclaration;

    /**
     * The name of the array storing the records.
     */
    public static final String recordArrayName = "records";

    /**
     * The names of the record fields storing keys in each record.
     */
    public final String[] keyFieldNames;

    /**
     * The names of the record fields storing the values in each record.
     */
    public final String[] valueFieldNames;

    /**
     * The name of the field of each record which stores the collision chain.
     */
    private static final String nextFieldName = "next";

    /**
     * The {@link AccessPath} to the array storing the hash-table.
     */
    private static final ArrayAccessPath hashTableAP =
            new ArrayAccessPath("hashTable", P_A_INT);

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
     * The default value for how many keys should be expected in the map.
     */
    private static final int initialKeysPerMap = 128;

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

        this.keyFieldNames = new String[keyTypes.length];
        for (int i = 0; i < keyFieldNames.length; i++)
            this.keyFieldNames[i] = "key_ord_" + i;

        this.valueFieldNames = new String[valueTypes.length];
        for (int i = 0; i < valueFieldNames.length; i++)
            this.valueFieldNames[i] = "value_ord_" + i;
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
        this.mapDeclaration = JaninoClassGen.createLocalClassDeclaration(
                JaninoGeneralGen.getLocation(),
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PRIVATE.toString(), JaninoGeneralGen.getLocation()),
                        new Java.AccessModifier("final", JaninoGeneralGen.getLocation())
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
                JaninoClassGen.createPublicFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getType()),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                numberOfRecordsAP.getVariableName()
                        )
                )
        );

        // Generate the class definition which define the records in this map
        generateRecordClassDefinition();
        this.mapDeclaration.addMemberTypeDeclaration(this.recordDeclaration);

        // Instantiate the array storing the records
        this.mapDeclaration.addFieldDeclaration(
                JaninoClassGen.createPublicFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        new Java.ArrayType(createReferenceType(getLocation(), this.recordDeclaration.name)),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                recordArrayName
                        )
                )
        );

        // Add the field storing the hash table
        this.mapDeclaration.addFieldDeclaration(
                JaninoClassGen.createPrivateFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), hashTableAP.getType()),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                hashTableAP.getVariableName()
                        )
                )
        );

    }

    /**
     * Method to generate the (nested) class which defines the records in the map.
     */
    private void generateRecordClassDefinition() {
        // Create the class definition
        this.recordDeclaration = new Java.MemberClassDeclaration(
                getLocation(),
                null,
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PUBLIC.toString(), JaninoGeneralGen.getLocation()),
                        new Java.AccessModifier("static", JaninoGeneralGen.getLocation()),
                        new Java.AccessModifier("final", JaninoGeneralGen.getLocation())
                },
                "RecordType",
                null,
                null,
                new Java.Type[0]
        );

        // Create a field for each section of the key
        for (int i = 0; i < this.keyFieldNames.length; i++) {
            Java.Type fieldType;
            if (this.keyTypes[i] == S_FL_BIN || this.keyTypes[i] == S_VARCHAR)
                fieldType = JaninoGeneralGen.createPrimitiveArrayType(JaninoGeneralGen.getLocation(), Java.Primitive.BYTE);
            else
                fieldType = toJavaType(getLocation(), this.keyTypes[i]);

            this.recordDeclaration.addFieldDeclaration(
                    JaninoClassGen.createPublicFieldDeclaration(
                            JaninoGeneralGen.getLocation(),
                            fieldType,
                            createSimpleVariableDeclaration(
                                    JaninoGeneralGen.getLocation(),
                                    this.keyFieldNames[i]
                            )
                    )
            );
        }

        // Create a field for each section of the values
        for (int i = 0; i < this.valueFieldNames.length; i++) {
            this.recordDeclaration.addFieldDeclaration(
                    JaninoClassGen.createPublicFieldDeclaration(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(getLocation(), this.valueTypes[i]),
                            createSimpleVariableDeclaration(
                                    JaninoGeneralGen.getLocation(),
                                    this.valueFieldNames[i]
                            )
                    )
            );
        }

        // Add the field storing the collision chain
        this.recordDeclaration.addFieldDeclaration(
                JaninoClassGen.createPublicFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), P_INT),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                nextFieldName
                        )
                )
        );

        // Create the constructor of the record which initialises all fields according to the parameter
        // value and initialises the next pointer at -1.
        var formalParameters = new Java.FunctionDeclarator.FormalParameter[this.keyFieldNames.length + this.valueFieldNames.length];
        for (int i = 0; i < this.keyFieldNames.length; i++) {
            formalParameters[i] =
                    createFormalParameter(getLocation(), toJavaType(getLocation(), this.keyTypes[i]), this.keyFieldNames[i]);
        }
        for (int i = 0; i < this.valueFieldNames.length; i++) {
            formalParameters[this.keyFieldNames.length + i] =
                    createFormalParameter(getLocation(), toJavaType(getLocation(), this.valueTypes[i]), this.valueFieldNames[i]);
        }

        List<Java.Statement> constructorBody = new ArrayList<>();
        for (int i = 0; i < formalParameters.length; i++) {
            constructorBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(
                                    getLocation(),
                                    formalParameters[i].name
                            ),
                            createAmbiguousNameRef(getLocation(), formalParameters[i].name)
                    )
            );
        }

        constructorBody.add(
                createVariableAssignmentStm(
                        getLocation(),
                        createThisFieldAccess(
                                getLocation(),
                                nextFieldName
                        ),
                        createIntegerLiteral(getLocation(), -1)
                )
        );

        createConstructor(
                JaninoGeneralGen.getLocation(),
                this.recordDeclaration,
                Access.PUBLIC,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
                null,
                constructorBody
        );


        // Create the increment method of the record
        formalParameters = new Java.FunctionDeclarator.FormalParameter[this.valueFieldNames.length];
        List<Java.Statement> incrementMethodBody = new ArrayList<>();
        for (int i = 0; i < this.valueFieldNames.length; i++) {
            var valueFieldName = this.valueFieldNames[i];
            formalParameters[i] =
                    createFormalParameter(getLocation(), toJavaType(getLocation(), this.valueTypes[i]), valueFieldName);
            incrementMethodBody.add(
                    createVariableAdditionAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(
                                    getLocation(),
                                    valueFieldName
                            ),
                            createAmbiguousNameRef(getLocation(), valueFieldName)
                    )
            );
        }

        createMethod(
                getLocation(),
                this.recordDeclaration,
                Access.PUBLIC,
                new Java.PrimitiveType(getLocation(), Java.Primitive.VOID),
                "increment",
                createFormalParameters(getLocation(), formalParameters),
                incrementMethodBody
        );
    }

    /**
     * Method to generate the constructors for the generated map type.
     */
    private void generateConstructors() {

        // Start by generating the no-argument constructor which calls the real constructor with
        // a default map-size of initialKeysPerMap.
        createConstructor(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createFormalParameters(JaninoGeneralGen.getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
                new Java.AlternateConstructorInvocation(
                        JaninoGeneralGen.getLocation(),
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), initialKeysPerMap)
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
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.not(
                                JaninoGeneralGen.getLocation(),
                                JaninoOperatorGen.and(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoOperatorGen.gt(
                                                JaninoGeneralGen.getLocation(),
                                                capacityParameterAP.read(),
                                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1)
                                        ),
                                        JaninoOperatorGen.eq(
                                                JaninoGeneralGen.getLocation(),
                                                JaninoOperatorGen.binAnd(
                                                        JaninoGeneralGen.getLocation(),
                                                        capacityParameterAP.read(),
                                                        JaninoOperatorGen.sub(
                                                                JaninoGeneralGen.getLocation(),
                                                                capacityParameterAP.read(),
                                                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1)
                                                        )
                                                ),
                                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                                        )
                                )
                        ),
                        new Java.ThrowStatement(
                                JaninoGeneralGen.getLocation(),
                                JaninoClassGen.createClassInstance(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createReferenceType(
                                                JaninoGeneralGen.getLocation(),
                                                "java.lang.IllegalArgumentException"
                                        ),
                                        new Java.Rvalue[] {
                                                JaninoGeneralGen.createStringLiteral(JaninoGeneralGen.getLocation(), "\"The map capacity is required to be a power of two\"")
                                        }
                                )
                        )
                )
        );

        // Initialise the numberOfRecords field
        constructorBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                numberOfRecordsAP.getVariableName()
                        ),
                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                )
        );

        // Initialise the records arrays
        constructorBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                recordArrayName
                        ),
                        new Java.NewArray(
                                JaninoGeneralGen.getLocation(),
                                createReferenceType(getLocation(), this.recordDeclaration.name),
                                new Java.Rvalue[] { capacityParameterAP.read() },
                                0
                        )
                )
        );

        // Initialise the hash table
        constructorBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                hashTableAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(hashTableAP.getType())),
                                capacityParameterAP.read()
                        )
                )
        );

        constructorBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                new Java.FieldAccessExpression(
                                        JaninoGeneralGen.getLocation(),
                                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                        hashTableAP.getVariableName()
                                ),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
                )
        );

        // Add the actual constructor
        createConstructor(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                createFormalParameters(
                        JaninoGeneralGen.getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[] {
                                createFormalParameter(
                                        JaninoGeneralGen.getLocation(),
                                        toJavaType(JaninoGeneralGen.getLocation(), capacityParameterAP.getType()),
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
                    JaninoGeneralGen.getLocation(),
                    toJavaType(JaninoGeneralGen.getLocation(), this.keyTypes[i]),
                    "key_ord_" + i
            );
        }

        formalParameters[currentFormalParamIndex++] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_LONG),
                "preHash"
        );

        int valueOrdsFormalParametersBaseIndex = currentFormalParamIndex;
        for (int i = 0; i < this.valueTypes.length; i++) {
            formalParameters[currentFormalParamIndex++] = createFormalParameter(
                    JaninoGeneralGen.getLocation(),
                    toJavaType(JaninoGeneralGen.getLocation(), this.valueTypes[i]),
                    "value_ord_" + i
            );
        }

        // Create the method body
        List<Java.Statement> incrementForKeyMethodBody = new ArrayList<>();

        // Create the first key ordinal to be non-negative
        if (this.keyTypes[0] == S_FL_BIN || this.keyTypes[0] == S_VARCHAR) {
            incrementForKeyMethodBody.add(
                    generateNonNullCheck(
                            JaninoGeneralGen.createAmbiguousNameRef(
                                    JaninoGeneralGen.getLocation(),
                                    formalParameters[0].name
                            )
                    )
            );
        } else {
            incrementForKeyMethodBody.add(
                    generateNonNegativeCheck(
                            JaninoGeneralGen.createAmbiguousNameRef(
                                    JaninoGeneralGen.getLocation(),
                                    formalParameters[0].name
                            )
                    )
            );
        }

        // Declare the index variable and check whether the key is already contained in the map
        // int index = find(keys ..., preHash);
        ScalarVariableAccessPath indexAP = new ScalarVariableAccessPath("index", P_INT);
        Java.Rvalue[] findMethodArguments = new Java.Rvalue[this.keyFieldNames.length + 1];
        for (int i = 0; i < findMethodArguments.length; i++)
            findMethodArguments[i] = JaninoGeneralGen.createAmbiguousNameRef(
                    JaninoGeneralGen.getLocation(),
                    formalParameters[i].name
            );

        incrementForKeyMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), indexAP.getType()),
                        indexAP.getVariableName(),
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                FIND_METHOD_NAME,
                                findMethodArguments
                        )
                )
        );

        // If so, set the index variable to the existing entry, otherwise, allocate a new index
        // if (index == -1) {
        //     index = this.numberOfRecords++;
        //     if (this.recordsArray.length == index)
        //         growArrays();
        //     this.recordsArray[index] = new RecordType( ... );
        //     boolean rehashOnCollision = this.numberOfRecords > (3 * this.hashTable.length) / 4;
        //     putHashEntry(keys ..., preHash, newIndex, rehashOnCollision);
        //     return;
        // }
        Java.Block allocateIndexBody = new Java.Block(JaninoGeneralGen.getLocation());
        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        indexAP.write(),
                        JaninoOperatorGen.postIncrement(
                                JaninoGeneralGen.getLocation(),
                                new Java.FieldAccessExpression(
                                        JaninoGeneralGen.getLocation(),
                                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                        numberOfRecordsAP.getVariableName()
                                )
                        )
                )
        );
        allocateIndexBody.addStatement(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(
                                JaninoGeneralGen.getLocation(),
                                new Java.FieldAccessExpression(
                                        JaninoGeneralGen.getLocation(),
                                        new Java.FieldAccessExpression(
                                                JaninoGeneralGen.getLocation(),
                                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                                recordArrayName
                                        ),
                                        "length"
                                ),
                                indexAP.read()
                        ),
                        createMethodInvocationStm(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                GROW_ARRAYS_METHOD_NAME
                        )
                )
        );

        Java.Rvalue[] recordOrdinalValues = new Java.Rvalue[this.keyFieldNames.length + this.valueFieldNames.length];
        for (int i = 0; i < this.keyFieldNames.length; i++) {
            recordOrdinalValues[i] = createMapAssignmentRValue( this.keyTypes[i], formalParameters[i].name, allocateIndexBody);
        }
        for (int i = 0; i < this.valueFieldNames.length; i++) {
            recordOrdinalValues[this.keyFieldNames.length + i] = createAmbiguousNameRef(getLocation(), formalParameters[this.keyFieldNames.length + 1 + i].name);
        }
        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        getLocation(),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(),
                                        recordArrayName
                                ),
                                indexAP.read()
                        ),
                        new Java.NewClassInstance(
                                getLocation(),
                                null,
                                createReferenceType(getLocation(), this.recordDeclaration.name),
                                recordOrdinalValues
                        )
                )
        );

        allocateIndexBody.addStatement(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.BOOLEAN),
                        "rehashOnCollision",
                        JaninoOperatorGen.gt(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getVariableName()),
                                JaninoOperatorGen.div(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoOperatorGen.mul(
                                                JaninoGeneralGen.getLocation(),
                                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 3),
                                                new Java.FieldAccessExpression(
                                                        JaninoGeneralGen.getLocation(),
                                                        JaninoGeneralGen.createThisFieldAccess(
                                                                JaninoGeneralGen.getLocation(),
                                                                hashTableAP.getVariableName()
                                                        ),
                                                        "length"
                                                )
                                        ),
                                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 4)
                                )
                        )
                )
        );

        Java.Rvalue[] putHashEntryArguments = new Java.Rvalue[this.keyFieldNames.length + 3];
        int currentPutHashEntryArgumentIndex = 0;
        for (int i = 0; i < this.keyFieldNames.length + 1; i++) // Keys and pre-hash value
            putHashEntryArguments[currentPutHashEntryArgumentIndex++] = JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[i].name);
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] = indexAP.read();
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] = JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "rehashOnCollision");

        allocateIndexBody.addStatement(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        putHashEntryArguments
                )
        );

        allocateIndexBody.addStatement(new Java.ReturnStatement(getLocation(), null));

        incrementForKeyMethodBody.add(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(
                                JaninoGeneralGen.getLocation(),
                                indexAP.read(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        ),
                        allocateIndexBody
                )
        );

        // Increment this key's values by the provided amounts
        // this.recordsArray[index].increment( ... );
        Java.Rvalue[] increments = new Java.Rvalue[this.valueFieldNames.length];
        for (int i = 0; i < valueFieldNames.length; i++) {
            increments[i] = createAmbiguousNameRef(getLocation(), formalParameters[this.keyFieldNames.length + 1 + i].name);
        }
        incrementForKeyMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), recordArrayName),
                                indexAP.read()
                        ),
                        "increment",
                        increments
                )
        );

        // public void incrementForKey([keys ...], long preHash, [values ...])
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                INCREMENT_FOR_KEY_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
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
                    JaninoGeneralGen.getLocation(),
                    toJavaType(JaninoGeneralGen.getLocation(), this.keyTypes[i]),
                    "key_ord_" + i
            );
        }

        int preHashFormalParamIndex = currentFormalParameterIndex++;
        formalParameters[preHashFormalParamIndex] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_LONG),
                "preHash"
        );

        // Create the method body
        List<Java.Statement> findMethodBody = new ArrayList<>();

        // int htIndex = "hash"(preHash);
        ScalarVariableAccessPath htIndex = new ScalarVariableAccessPath("htIndex", P_INT);
        findMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), htIndex.getType()),
                        htIndex.getVariableName(),
                        generatePreHashToHashStatement(
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[preHashFormalParamIndex].name))
                )
        );

        // int initialIndex = this.hashTable[htIndex];
        ScalarVariableAccessPath initialIndex = new ScalarVariableAccessPath("initialIndex", P_INT);
        findMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), initialIndex.getType()),
                        initialIndex.getVariableName(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(
                                        JaninoGeneralGen.getLocation(),
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
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(
                                JaninoGeneralGen.getLocation(),
                                initialIndex.read(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        ),
                        createReturnStm(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        )
                )
        );

        // Otherwise follow next-pointers while necessary
        // int currentIndex = initialIndex;
        // RecordType currentRecord = this.recordsArray[currentIndex];
        // while ($ disjunction of key ords $ currentRecord.key_ord_i != key_ord_i) {
        //     int potentialNextIndex = currentRecord.next;
        //     if (potentialNextIndex == -1)
        //         return -1;
        //     currentIndex = potentialNextIndex;
        //     currentRecord = this.recordsArray[currentIndex];
        // }
        ScalarVariableAccessPath currentIndex = new ScalarVariableAccessPath("currentIndex", P_INT);
        findMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), currentIndex.getType()),
                        currentIndex.getVariableName(),
                        initialIndex.read()
                )
        );
        String currentRecord = "currentRecord";
        findMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(getLocation(), this.recordDeclaration.name),
                        currentRecord,
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(),
                                        recordArrayName
                                ),
                                currentIndex.read()
                        )
                )
        );

        // Generate the disjunction for the while-loop guard
        Java.Rvalue nextLoopDisjunction;
        if (this.keyTypes[0] == S_FL_BIN || this.keyTypes[0] == S_VARCHAR) {
            nextLoopDisjunction = JaninoOperatorGen.not(
                    JaninoGeneralGen.getLocation(),
                    createMethodInvocation(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                            "equals",
                            new Java.Rvalue[] {
                                    new Java.FieldAccessExpression(
                                            JaninoGeneralGen.getLocation(),
                                            createAmbiguousNameRef(getLocation(), currentRecord),
                                            this.keyFieldNames[0]
                                    ),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name)
                            }
                    )
            );

        } else {
            nextLoopDisjunction = JaninoOperatorGen.neq(
                    JaninoGeneralGen.getLocation(),
                    new Java.FieldAccessExpression(
                            JaninoGeneralGen.getLocation(),
                            createAmbiguousNameRef(getLocation(), currentRecord),
                            this.keyFieldNames[0]
                    ),
                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name)
            );

        }

        for (int i = 1; i < this.keyFieldNames.length; i++) {
            Java.Rvalue conditionCheck;
            if (this.keyTypes[i] == S_FL_BIN || this.keyTypes[i] == S_VARCHAR) {
                conditionCheck = JaninoOperatorGen.not(
                        JaninoGeneralGen.getLocation(),
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                                "equals",
                                new Java.Rvalue[] {
                                        new Java.FieldAccessExpression(
                                                JaninoGeneralGen.getLocation(),
                                                createAmbiguousNameRef(getLocation(), currentRecord),
                                                this.keyFieldNames[i]
                                        ),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[i].name)
                                }
                        )
                );

            } else {
                conditionCheck = JaninoOperatorGen.neq(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                createAmbiguousNameRef(getLocation(), currentRecord),
                                this.keyFieldNames[i]
                        ),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[i].name)
                );

            }

            nextLoopDisjunction = JaninoOperatorGen.or(
                    JaninoGeneralGen.getLocation(),
                    nextLoopDisjunction,
                    conditionCheck
            );
        }

        Java.Block nextPointerLoopBody = new Java.Block(JaninoGeneralGen.getLocation());
        findMethodBody.add(
                JaninoControlGen.createWhileLoop(
                        JaninoGeneralGen.getLocation(),
                        nextLoopDisjunction,
                        nextPointerLoopBody
                )
        );

        ScalarVariableAccessPath pni = new ScalarVariableAccessPath("potentialNextIndex", P_INT);
        nextPointerLoopBody.addStatement(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), pni.getType()),
                        pni.getVariableName(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                createAmbiguousNameRef(getLocation(), currentRecord),
                                nextFieldName
                        )
                )
        );

        nextPointerLoopBody.addStatement(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(JaninoGeneralGen.getLocation(), pni.read(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)),
                        createReturnStm(JaninoGeneralGen.getLocation(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1))
                )
        );

        nextPointerLoopBody.addStatement(createVariableAssignmentStm(JaninoGeneralGen.getLocation(), currentIndex.write(), pni.read()));
        nextPointerLoopBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        createAmbiguousNameRef(getLocation(), currentRecord),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(
                                        getLocation(),
                                        recordArrayName
                                ),
                                currentIndex.read()
                        )
                )
        );

        // We found the index: return currentIndex;
        findMethodBody.add(createReturnStm(JaninoGeneralGen.getLocation(), currentIndex.read()));

        // private int find([keys ...], long preHash)
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.INT),
                FIND_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
                findMethodBody
        );

    }

    /**
     * Method to generate the "growArrays" method to grow the records array when it becomes too small.
     */
    private void generateGrowArraysMethod() {
        List<Java.Statement> growArraysMethodBody = new ArrayList<>();

        // int currentSize = this.recordsArray.length;
        ScalarVariableAccessPath currentSize = new ScalarVariableAccessPath("currentSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), currentSize.getType()),
                        currentSize.getVariableName(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), recordArrayName),
                                "length"
                        )
                )
        );

        // int newSize = currentSize << 1;
        ScalarVariableAccessPath newSize = new ScalarVariableAccessPath("newSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), newSize.getType()),
                        newSize.getVariableName(),
                        JaninoOperatorGen.lShift(JaninoGeneralGen.getLocation(), currentSize.read(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1))
                )
        );

        // if (newSize > Integer.MAX_VALUE - 1)
        //     throw new UnsupportedOperationException("Map has grown too large");
        growArraysMethodBody.add(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.gt(
                                JaninoGeneralGen.getLocation(),
                                newSize.read(),
                                JaninoOperatorGen.sub(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Integer.MAX_VALUE"),
                                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1)
                                )
                        ),
                        new Java.ThrowStatement(
                                JaninoGeneralGen.getLocation(),
                                JaninoClassGen.createClassInstance(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createReferenceType(JaninoGeneralGen.getLocation(), "java.lang.UnsupportedOperationException"),
                                        new Java.Rvalue[] {
                                                JaninoGeneralGen.createStringLiteral(
                                                        JaninoGeneralGen.getLocation(),
                                                        "\"Map has grown too large\""
                                                )
                                        }
                                )
                        )
                )
        );

        // Grow, copy and fill the new array
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        new Java.ArrayType(createReferenceType(getLocation(), this.recordDeclaration.name)),
                        "new" + recordArrayName,
                        new Java.NewArray(
                                getLocation(),
                                createReferenceType(getLocation(), this.recordDeclaration.name),
                                new Java.Rvalue[] { newSize.read() },
                                0
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), recordArrayName),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "new" + recordArrayName),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), recordArrayName),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "new" + recordArrayName)
                )
        );

        // private void growArrays()
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                GROW_ARRAYS_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
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
                    JaninoGeneralGen.getLocation(),
                    toJavaType(JaninoGeneralGen.getLocation(), this.keyTypes[i]),
                    "key_ord_" + i
            );
        }

        int prehashFormalParamIndex = currentFormalParamIndex++;
        formalParameters[prehashFormalParamIndex] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_LONG),
                "preHash"
        );

        int indexFormalParamIndex = currentFormalParamIndex++;
        formalParameters[indexFormalParamIndex] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_INT),
                "index"
        );

        int rehashOnCollisionFormalParamIndex = currentFormalParamIndex++;
        formalParameters[rehashOnCollisionFormalParamIndex] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_BOOLEAN),
                "rehashOnCollision"
        );

        // Generate the method body
        List<Java.Statement> putHEMethodBody = new ArrayList<>();

        // int htIndex = "hash"(preHash);
        ScalarVariableAccessPath htIndex = new ScalarVariableAccessPath("htIndex", P_INT);
        putHEMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), htIndex.getType()),
                        htIndex.getVariableName(),
                        this.generatePreHashToHashStatement(
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[prehashFormalParamIndex].name)
                        )
                )
        );

        // int initialIndex = this.hashTable[htIndex];
        ScalarVariableAccessPath initialIndex = new ScalarVariableAccessPath("initialIndex", P_INT);
        putHEMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), initialIndex.getType()),
                        initialIndex.getVariableName(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                htIndex.read()
                        )
                )
        );

        // If there is a free entry, simply store the new index there
        // if (initialIndex == -1) {
        //     this.hashTable[htIndex] = index;
        //     return;
        // }
        Java.Block addIndexToFreeEntryBody = new Java.Block(JaninoGeneralGen.getLocation());
        addIndexToFreeEntryBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                htIndex.read()
                        ),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[indexFormalParamIndex].name)
                )
        );

        addIndexToFreeEntryBody.addStatement(createReturnStm(JaninoGeneralGen.getLocation(), null));

        putHEMethodBody.add(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(JaninoGeneralGen.getLocation(), initialIndex.read(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)),
                        addIndexToFreeEntryBody
                )
        );

        // Otherwise we have a collision, so rehash if we were instructed to do so
        Java.Block rehashOnCollisionBody = new Java.Block(JaninoGeneralGen.getLocation());
        rehashOnCollisionBody.addStatement(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                        REHASH_METHOD_NAME
                )
        );

        rehashOnCollisionBody.addStatement(createReturnStm(JaninoGeneralGen.getLocation(), null));

        putHEMethodBody.add(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[rehashOnCollisionFormalParamIndex].name),
                        rehashOnCollisionBody
                )
        );

        // Otherwise store the index in the first free "next" entry in the probe sequence
        // int currentIndex = initialIndex;
        ScalarVariableAccessPath currentIndex = new ScalarVariableAccessPath("currentIndex", P_INT);
        putHEMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), currentIndex.getType()),
                        currentIndex.getVariableName(),
                        initialIndex.read()
                )
        );

        // RecordType currentRecord = this.recordArray[currentIndex];
        // while ($ disjunction per key ord i $ currentRecord.keys_ord_i != key_ord_i $$ && currentRecord.next != -1) {
        //     currentIndex = currentRecord.next;
        //     currentRecord = this.recordArray[currentIndex];
        // }
        String currentRecord = "currentRecord";
        putHEMethodBody.add(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(getLocation(), this.recordDeclaration.name),
                        currentRecord,
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), recordArrayName),
                                currentIndex.read()
                        )
                )
        );

        Java.Rvalue probeWhileLoopDisjunction;
        if (this.keyTypes[0] == S_FL_BIN || this.keyTypes[0] == S_VARCHAR) {
            probeWhileLoopDisjunction = JaninoOperatorGen.not(
                    JaninoGeneralGen.getLocation(),
                    createMethodInvocation(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                            "equals",
                            new Java.Rvalue[] {
                                    new Java.FieldAccessExpression(
                                            JaninoGeneralGen.getLocation(),
                                            createAmbiguousNameRef(getLocation(), currentRecord),
                                            this.keyFieldNames[0]
                                    ),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name)
                            }
                    )
            );

        } else {
            probeWhileLoopDisjunction = JaninoOperatorGen.neq(
                    JaninoGeneralGen.getLocation(),
                    new Java.FieldAccessExpression(
                            JaninoGeneralGen.getLocation(),
                            createAmbiguousNameRef(getLocation(), currentRecord),
                            this.keyFieldNames[0]
                    ),
                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name)
            );

        }

        for (int i = 1; i < this.keyFieldNames.length; i++) {
            Java.Rvalue condition;
            if (this.keyTypes[i] == S_FL_BIN || this.keyTypes[i] == S_VARCHAR) {
                condition = JaninoOperatorGen.not(
                        JaninoGeneralGen.getLocation(),
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                                "equals",
                                new Java.Rvalue[] {
                                        new Java.FieldAccessExpression(
                                                JaninoGeneralGen.getLocation(),
                                                createAmbiguousNameRef(getLocation(), currentRecord),
                                                this.keyFieldNames[i]
                                        ),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[i].name)
                                }
                        )
                );

            } else {
                condition = JaninoOperatorGen.neq(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                createAmbiguousNameRef(getLocation(), currentRecord),
                                this.keyFieldNames[i]
                        ),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[i].name)
                );

            }

            probeWhileLoopDisjunction = JaninoOperatorGen.or(
                    JaninoGeneralGen.getLocation(),
                    probeWhileLoopDisjunction,
                    condition
            );
        }

        Java.Block findAvailableNextLoopBody = new Java.Block(getLocation());
        putHEMethodBody.add(
                JaninoControlGen.createWhileLoop(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.and(
                                JaninoGeneralGen.getLocation(),
                                probeWhileLoopDisjunction,
                                JaninoOperatorGen.neq(
                                        JaninoGeneralGen.getLocation(),
                                        new Java.FieldAccessExpression(
                                                JaninoGeneralGen.getLocation(),
                                                createAmbiguousNameRef(getLocation(), currentRecord),
                                                nextFieldName
                                        ),
                                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                                )
                        ),
                        findAvailableNextLoopBody
                )
        );

        findAvailableNextLoopBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        currentIndex.write(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                createAmbiguousNameRef(getLocation(), currentRecord),
                                nextFieldName
                        )
                )
        );

        findAvailableNextLoopBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        createAmbiguousNameRef(getLocation(), currentRecord),
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), recordArrayName),
                                currentIndex.read()
                        )
                )
        );

        // At this point currentRecord has property that currentRecord.next == -1, so we update it to
        // point to index to ensure the collision list is correct
        // currentIndex.next = index;
        putHEMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                createAmbiguousNameRef(getLocation(), currentRecord),
                                nextFieldName
                        ),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[indexFormalParamIndex].name)
                )
        );

        // private void putHashEntry([keys ...], long preHash, int index, boolean rehashOnCollision)
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                PUT_HASH_ENTRY_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
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
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), size.getType()),
                        size.getVariableName(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                "length"
                        )
                )
        );

        // while (size <= this.numberOfRecords) size = (size << 1);
        rehashMethodBody.add(
                JaninoControlGen.createWhileLoop(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.le(
                                JaninoGeneralGen.getLocation(),
                                size.read(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getVariableName())
                        ),
                        createVariableAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                size.write(),
                                JaninoOperatorGen.lShift(JaninoGeneralGen.getLocation(), size.read(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1))
                        )
                )
        );

        // Add some additional size to prevent collisions
        // size = size << 1;
        rehashMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        size.write(),
                        JaninoOperatorGen.lShift(JaninoGeneralGen.getLocation(), size.read(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1))
                )
        );

        // Initialise the new hash-table
        // this.hashTable = new int[size];
        // Arrays.fill(this.hashTable, -1);
        rehashMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                        JaninoGeneralGen.createNewPrimitiveArray(JaninoGeneralGen.getLocation(), Java.Primitive.INT, size.read())
                )
        );

        rehashMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
                )
        );

        // Reset the next pointers
        // for (int i = 0; i < numberOfRecords; i++) {
        //     this.records[i].next = -1;
        // }
        ScalarVariableAccessPath nextPointerIndex = new ScalarVariableAccessPath("recordIndex", P_INT);
        rehashMethodBody.add(
                JaninoControlGen.createForLoop(
                        getLocation(),
                        createLocalVariable(
                                getLocation(),
                                toJavaType(getLocation(), nextPointerIndex.getType()),
                                nextPointerIndex.getVariableName(),
                                createIntegerLiteral(getLocation(), 0)
                        ),
                        lt(getLocation(), nextPointerIndex.read(), numberOfRecordsAP.read()),
                        postIncrement(getLocation(), nextPointerIndex.write()),
                        createVariableAssignmentStm(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        getLocation(),
                                        createArrayElementAccessExpr(
                                                getLocation(),
                                                createThisFieldAccess(getLocation(), recordArrayName),
                                                nextPointerIndex.read()
                                        ),
                                        nextFieldName
                                ),
                                createIntegerLiteral(getLocation(), -1)
                        )
                )
        );

        // Finally insert all key-value associations in the new hash-table
        // for (int i = 0; i < this.numberOfRecords; i++) {
        //     RecordType currentRecord = this.recordArray[i];
        //     $ for each key ord j $
        //       [keyType] key_ord_j = currentRecord.keys_ord_j;
        //     long preHash = [hash_function_container].hash(key_ord_0);
        //     $ for each remaining key ord j $
        //       preHash ^= [hash_function_container].hash(key_ord_j);
        //     this.putHashEntry([keys ...], preHash, i, false);
        // }
        Java.Block hashAssociationLoopBody = new Java.Block(JaninoGeneralGen.getLocation());

        ScalarVariableAccessPath indexVar = new ScalarVariableAccessPath("i", P_INT);
        rehashMethodBody.add(
                JaninoControlGen.createForLoop(
                        JaninoGeneralGen.getLocation(),
                        createLocalVariable(
                                JaninoGeneralGen.getLocation(),
                                toJavaType(JaninoGeneralGen.getLocation(), indexVar.getType()),
                                indexVar.getVariableName(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                        ),
                        lt(
                                JaninoGeneralGen.getLocation(),
                                indexVar.read(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getVariableName())
                        ),
                        JaninoOperatorGen.postIncrement(JaninoGeneralGen.getLocation(), indexVar.write()),
                        hashAssociationLoopBody
                )
        );

        String currentRecord = "currentRecord";
        hashAssociationLoopBody.addStatement(
                createLocalVariable(
                        getLocation(),
                        createReferenceType(getLocation(), this.recordDeclaration.name),
                        currentRecord,
                        createArrayElementAccessExpr(
                                getLocation(),
                                createThisFieldAccess(getLocation(), recordArrayName),
                                indexVar.read()
                        )
                )
        );

        ScalarVariableAccessPath[] keyVarAPs = new ScalarVariableAccessPath[this.keyFieldNames.length];
        for (int i = 0; i < this.keyFieldNames.length; i++) {
            String keyName = "key_ord_" + i;
            QueryVariableType keyType = this.keyTypes[i];

            keyVarAPs[i] = new ScalarVariableAccessPath(keyName, keyType);
            hashAssociationLoopBody.addStatement(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), keyType),
                            keyName,
                            new Java.FieldAccessExpression(
                                    JaninoGeneralGen.getLocation(),
                                    createAmbiguousNameRef(getLocation(), currentRecord),
                                    this.keyFieldNames[i]
                            )
                    )
            );
        }

        for (int i = 0; i < this.keyFieldNames.length; i++) {
            Java.MethodInvocation hashMethodInvocation = createMethodInvocation(
                    JaninoGeneralGen.getLocation(),
                    switch (this.keyTypes[i]) {
                        case P_DOUBLE -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Double_Hash_Function");
                        case P_INT, P_INT_DATE -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Int_Hash_Function");
                        case S_FL_BIN, S_VARCHAR -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Char_Arr_Hash_Function");

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
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.LONG),
                                "preHash",
                                hashMethodInvocation
                        )
                );
            } else {
                // On the remaining key ordinals, we need to "extend" the preHash variable
                hashAssociationLoopBody.addStatement(
                        createVariableXorAssignmentStm(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "preHash"),
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
                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "preHash");
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] = indexVar.read();
        putHashEntryArguments[currentPutHashEntryArgumentIndex++] =
                new Java.BooleanLiteral(JaninoGeneralGen.getLocation(), "false");

        hashAssociationLoopBody.addStatement(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        putHashEntryArguments
                )
        );

        // private void rehash()
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                REHASH_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
                rehashMethodBody
        );
    }

    /**
     * Method to generate the reset method, which "clears" the generated map.
     */
    private void generateResetMethod() {
        List<Java.Statement> resetMethodBody = new ArrayList<>();

        // this.numberOfRecords = 0;
        resetMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createThisFieldAccess(
                                JaninoGeneralGen.getLocation(),
                                numberOfRecordsAP.getVariableName()
                        ),
                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                )
        );

        // Arrays.fill(this.hashTable, -1);
        resetMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
                )
        );

        // Arrays.fill(this.recordArray, null);
        resetMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), recordArrayName),
                                new Java.NullLiteral(getLocation())
                        }
                )
        );

        // public void reset()
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                RESET_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), new Java.FunctionDeclarator.FormalParameter[0]),
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
        return JaninoControlGen.createIf(
                JaninoGeneralGen.getLocation(),
                lt(
                        JaninoGeneralGen.getLocation(),
                        rValueToCheck,
                        JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                ),
                new Java.ThrowStatement(
                        JaninoGeneralGen.getLocation(),
                        JaninoClassGen.createClassInstance(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createReferenceType(
                                        JaninoGeneralGen.getLocation(),
                                        "java.lang.IllegalArgumentException"
                                ),
                                new Java.Rvalue[] {
                                        JaninoGeneralGen.createStringLiteral(JaninoGeneralGen.getLocation(), "\"The map expects the first key ordinal to be non-negative\"")
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
        return JaninoControlGen.createIf(
                JaninoGeneralGen.getLocation(),
                JaninoOperatorGen.eq(
                        JaninoGeneralGen.getLocation(),
                        rValueToCheck,
                        new Java.NullLiteral(JaninoGeneralGen.getLocation())
                ),
                new Java.ThrowStatement(
                        JaninoGeneralGen.getLocation(),
                        JaninoClassGen.createClassInstance(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createReferenceType(
                                        JaninoGeneralGen.getLocation(),
                                        "java.lang.IllegalArgumentException"
                                ),
                                new Java.Rvalue[] {
                                        JaninoGeneralGen.createStringLiteral(JaninoGeneralGen.getLocation(), "\"The map expects the first key ordinal to be non-null\"")
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
        return JaninoGeneralGen.createCast(
                JaninoGeneralGen.getLocation(),
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.INT),
                JaninoOperatorGen.binAnd(
                        JaninoGeneralGen.getLocation(),
                        preHashRValue,
                        JaninoOperatorGen.sub(
                                JaninoGeneralGen.getLocation(),
                                new Java.FieldAccessExpression(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                        "length"
                                ),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 1)
                        )
                )
        );

    }

}
