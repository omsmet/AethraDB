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
import static AethraDB.evaluation.codegen.infrastructure.context.QueryVariableTypeMethods.primitiveArrayTypeForPrimitive;
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
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.mul;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoOperatorGen.postIncrement;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createSimpleVariableDeclaration;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createVariableAssignmentStm;
import static AethraDB.evaluation.general_support.hashmaps.CommonMapGenerator.createMapAssignmentRValue;

/**
 * This class provides methods to generate a hash-map implementation for mapping some primitive type
 * keys to multiple objects of some record-like type, where each field of the record is also a
 * primitive type. To hash keys, a generated map relies on the appropriate hash function definitions
 * for the given key type:
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
    public Java.LocalClassDeclaration mapDeclaration;

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
    public static final ArrayAccessPath keysRecordCountAP =
            new ArrayAccessPath("keysRecordCount", P_A_INT);

    /**
     * The {@link AccessPath} to the array storing the collision chains.
     */
    private static final ArrayAccessPath nextArrayAP =
            new ArrayAccessPath("next", P_A_INT);

    /**
     * The {@link AccessPath} to the array storing the hash-table.
     */
    private static final ArrayAccessPath hashTableAP =
            new ArrayAccessPath("hashTable", P_A_INT);

    /**
     * The {@link Java.MemberClassDeclaration} for the value part of the records stored in {@code this}.
     */
    public Java.MemberClassDeclaration valueRecordDeclaration;

    /**
     * The name of the array storing the value records.
     */
    public static final String valueRecordArrayName = "records";

    /**
     * The names of the record fields storing the values in the map.
     */
    public final String[] valueFieldNames;

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
     * The default value for how many keys should be expected in the map.
     */
    private static final int initialKeysPerMap = 262_144;

    /**
     * The grow-factor used for upgrading the hash-table size when it "overflows".
     */
    private static final int hashTableGrowFactor = 8;

    /**
     * The grow-factor used for upgrading the record arrays when they "overflow".
     */
    private static final int dataGrowFactor = 8;

    /**
     * The default value for how many records should be created per key.
     */
    private static final int initialRecordsPerKeyCount = 1;

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
            if (!isPrimitive(valueType) && valueType != S_FL_BIN && valueType != S_VARCHAR)
                throw new IllegalArgumentException("KeyMultiRecordMapGenerator expects primitive value types, not " + valueType);
        }

        this.keyType = keyType;
        this.valueTypes = valueTypes;

        this.generationFinished = false;

        this.keysAP = new ArrayAccessPath("keys", primitiveArrayTypeForPrimitive(this.keyType));
        this.valueFieldNames = new String[valueTypes.length];
        for (int i = 0; i < valueFieldNames.length; i++)
            this.valueFieldNames[i] = "value_ord_" + i;
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
        this.mapDeclaration = JaninoClassGen.createLocalClassDeclaration(
                JaninoGeneralGen.getLocation(),
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PRIVATE.toString(), JaninoGeneralGen.getLocation()),
                        new Java.AccessModifier("final", JaninoGeneralGen.getLocation())
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
                JaninoClassGen.createPrivateFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getType()),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                numberOfRecordsAP.getVariableName()
                        )
                )
        );

        // Add the variable storing the keys in the map
        this.mapDeclaration.addFieldDeclaration(
                JaninoClassGen.createPrivateFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), this.keysAP.getType()),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                this.keysAP.getVariableName()
                        )
                )
        );

        // Add the variable storing the record count per key in the map
        this.mapDeclaration.addFieldDeclaration(
                JaninoClassGen.createPublicFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), keysRecordCountAP.getType()),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                keysRecordCountAP.getVariableName()
                        )
                )
        );

        // Generate the class definition which defines the value-record types in this map.
        if (this.valueFieldNames.length > 0) {
            generateValueRecordClassDefinition();
            this.mapDeclaration.addMemberTypeDeclaration(this.valueRecordDeclaration);
        }

        if (this.valueFieldNames.length > 0) {
            // Instantiate the nested array storing the value records
            this.mapDeclaration.addFieldDeclaration(
                    JaninoClassGen.createPublicFieldDeclaration(
                            JaninoGeneralGen.getLocation(),
                            new Java.ArrayType(new Java.ArrayType(createReferenceType(getLocation(), this.valueRecordDeclaration.name))),
                            createSimpleVariableDeclaration(
                                    JaninoGeneralGen.getLocation(),
                                    valueRecordArrayName
                            )
                    )
            );
        }

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

        // Add the field storing the collision chains
        this.mapDeclaration.addFieldDeclaration(
                JaninoClassGen.createPrivateFieldDeclaration(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), nextArrayAP.getType()),
                        createSimpleVariableDeclaration(
                                JaninoGeneralGen.getLocation(),
                                nextArrayAP.getVariableName()
                        )
                )
        );

    }

    /**
     * Method to generate the value record type for this map.
     */
    private void generateValueRecordClassDefinition() {
        // Create the class definition
        this.valueRecordDeclaration = new Java.MemberClassDeclaration(
                getLocation(),
                null,
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PUBLIC.toString(), JaninoGeneralGen.getLocation()),
                        new Java.AccessModifier("static", JaninoGeneralGen.getLocation()),
                        new Java.AccessModifier("final", JaninoGeneralGen.getLocation())
                },
                "ValueRecordType",
                null,
                null,
                new Java.Type[0]
        );

        // Create a field for each section of the values
        for (int i = 0; i < this.valueFieldNames.length; i++) {
            this.valueRecordDeclaration.addFieldDeclaration(
                    JaninoClassGen.createPublicFinalFieldDeclaration(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(getLocation(), this.valueTypes[i]),
                            createSimpleVariableDeclaration(
                                    JaninoGeneralGen.getLocation(),
                                    this.valueFieldNames[i]
                            )
                    )
            );
        }

        // Create the constructor of the record which initialises all fields according to the parameter value
        var formalParameters = new Java.FunctionDeclarator.FormalParameter[this.valueFieldNames.length];
        for (int i = 0; i < this.valueFieldNames.length; i++) {
            formalParameters[i] =
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

        createConstructor(
                JaninoGeneralGen.getLocation(),
                this.valueRecordDeclaration,
                Access.PUBLIC,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
                null,
                constructorBody
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

        // Initialise the keys array
        constructorBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                keysAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(keyType),
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
                                        keysAP.getVariableName()
                                ),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
                )
        );

        // Initialise the record count per key
        constructorBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                keysRecordCountAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(keysRecordCountAP.getType())),
                                capacityParameterAP.read()
                        )
                )
        );

        // Initialise the value records array
        if (this.valueFieldNames.length > 0) {
            constructorBody.add(
                    createVariableAssignmentStm(
                            JaninoGeneralGen.getLocation(),
                            new Java.FieldAccessExpression(
                                    JaninoGeneralGen.getLocation(),
                                    new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                    valueRecordArrayName
                            ),
                            new Java.NewArray(
                                    JaninoGeneralGen.getLocation(),
                                    createReferenceType(getLocation(), this.valueRecordDeclaration.name),
                                    new Java.Rvalue[]{
                                            capacityParameterAP.read(),
                                            createIntegerLiteral(getLocation(), initialRecordsPerKeyCount)
                                    },
                                    0
                            )
                    )
            );
        }

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

        // Initialise the collision chain array
        constructorBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                nextArrayAP.getVariableName()
                        ),
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(nextArrayAP.getType())),
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
                                        nextArrayAP.getVariableName()
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
     * Method to generate the "associate" method for the generated map type, which associates a
     * record to a key.
     */
    private void generateAssociateMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[this.valueTypes.length + 2];

        formalParameters[0] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_LONG),
                "preHash"
        );

        for (int i = 0; i < this.valueTypes.length; i++) {
            formalParameters[i + 2] = createFormalParameter(
                    JaninoGeneralGen.getLocation(),
                    toJavaType(JaninoGeneralGen.getLocation(), this.valueTypes[i]),
                    "record_ord_" + i
            );
        }

        // Create the method body
        List<Java.Statement> associateMethodBody = new ArrayList<>();

        // Create the non-negative key check
        associateMethodBody.add(
                generateNonNegativeCheck(
                        JaninoGeneralGen.createAmbiguousNameRef(
                                JaninoGeneralGen.getLocation(),
                                formalParameters[0].name
                        )
                )
        );

        // Declare the index variable and check whether the key is already contained in the map
        // int index = find(key, preHash);
        ScalarVariableAccessPath indexAP = new ScalarVariableAccessPath("index", P_INT);
        associateMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), indexAP.getType()),
                        indexAP.getVariableName(),
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                FIND_METHOD_NAME,
                                new Java.Rvalue[] {
                                        JaninoGeneralGen.createAmbiguousNameRef(
                                                JaninoGeneralGen.getLocation(),
                                                formalParameters[0].name
                                        ),
                                        JaninoGeneralGen.createAmbiguousNameRef(
                                                JaninoGeneralGen.getLocation(),
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
        //     this.keys[index] = key; <-- Note this assignment is slightly different for byte[]
        // }
        ScalarVariableAccessPath newEntryAP = new ScalarVariableAccessPath("newEntry", P_BOOLEAN);
        associateMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), newEntryAP.getType()),
                        newEntryAP.getVariableName(),
                        new Java.BooleanLiteral(JaninoGeneralGen.getLocation(), "false")
                )
        );

        Java.Block allocateIndexBody = new Java.Block(JaninoGeneralGen.getLocation());
        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        newEntryAP.write(),
                        new Java.BooleanLiteral(JaninoGeneralGen.getLocation(), "true")
                )
        );

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
                                                this.keysAP.getVariableName()
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
        allocateIndexBody.addStatement(
                createVariableAssignmentStm(
                        getLocation(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(
                                        JaninoGeneralGen.getLocation(),
                                        this.keysAP.getVariableName()
                                ),
                                indexAP.read()
                        ),
                        createMapAssignmentRValue(this.keyType, formalParameters[0].name, allocateIndexBody)
                )
        );

        associateMethodBody.add(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(
                                JaninoGeneralGen.getLocation(),
                                indexAP.read(),
                                createIntegerLiteral(getLocation(), -1)
                        ),
                        allocateIndexBody
                )
        );

        // Register additional values if necessary
        if (valueFieldNames.length > 0) {
            // Check if there is enough room left in this key's value arrays to store the new record
            // int insertionIndex = this.keysRecordCount[index];
            // if (!(insertionIndex < this.records[index].length)) [recordStoreExtendArrays]
            ScalarVariableAccessPath insertionIndexAP =
                    new ScalarVariableAccessPath("insertionIndex", P_INT);
            associateMethodBody.add(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), insertionIndexAP.getType()),
                            insertionIndexAP.getVariableName(),
                            JaninoGeneralGen.createArrayElementAccessExpr(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), keysRecordCountAP.getVariableName()),
                                    indexAP.read()
                            )
                    )
            );

            Java.Block recordStoreExtendArrays = new Java.Block(JaninoGeneralGen.getLocation());
            associateMethodBody.add(
                    JaninoControlGen.createIf(
                            JaninoGeneralGen.getLocation(),
                            JaninoOperatorGen.not(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoOperatorGen.lt(
                                            JaninoGeneralGen.getLocation(),
                                            insertionIndexAP.read(),
                                            new Java.FieldAccessExpression(
                                                    JaninoGeneralGen.getLocation(),
                                                    JaninoGeneralGen.createArrayElementAccessExpr(
                                                            JaninoGeneralGen.getLocation(),
                                                            JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                                                            indexAP.read()
                                                    ),
                                                    "length"
                                            )
                                    )
                            ),
                            recordStoreExtendArrays
                    )
            );

            // Extend the value array for the current index by doubling its size
            // int currentValueArraysSize = this.valueRecords[index].length;
            ScalarVariableAccessPath currentValueArraysSize =
                    new ScalarVariableAccessPath("currentValueArraySize", P_INT);
            recordStoreExtendArrays.addStatement(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), currentValueArraysSize.getType()),
                            currentValueArraysSize.getVariableName(),
                            new Java.FieldAccessExpression(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createArrayElementAccessExpr(
                                            JaninoGeneralGen.getLocation(),
                                            JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                                            indexAP.read()
                                    ),
                                    "length"
                            )
                    )
            );

            // int newValueArraysSize = [dataGrowFactor] * currentValueArraysSize;
            ScalarVariableAccessPath newValueArraysSize =
                    new ScalarVariableAccessPath("newValueArraySize", P_INT);
            recordStoreExtendArrays.addStatement(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            toJavaType(JaninoGeneralGen.getLocation(), newValueArraysSize.getType()),
                            newValueArraysSize.getVariableName(),
                            JaninoOperatorGen.mul(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), dataGrowFactor),
                                    currentValueArraysSize.read()
                            )
                    )
            );

            String tempVarName = "temp_" + valueRecordArrayName;
            recordStoreExtendArrays.addStatement(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            new Java.ArrayType(createReferenceType(getLocation(), this.valueRecordDeclaration.name)),
                            tempVarName,
                            new Java.NewArray(
                                    JaninoGeneralGen.getLocation(),
                                    createReferenceType(getLocation(), this.valueRecordDeclaration.name),
                                    new Java.Rvalue[] { newValueArraysSize.read() },
                                    0
                            )
                    )
            );

            recordStoreExtendArrays.addStatement(
                    createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    JaninoGeneralGen.createArrayElementAccessExpr(
                                            JaninoGeneralGen.getLocation(),
                                            JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                                            indexAP.read()
                                    ),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), tempVarName),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    currentValueArraysSize.read()
                            }
                    )
            );

            recordStoreExtendArrays.addStatement(
                    createVariableAssignmentStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createArrayElementAccessExpr(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                                    indexAP.read()
                            ),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), tempVarName)
                    )
            );

            // Insert the value record and increment the correct record count
            // this.records[index][insertionIndex] = new ValueRecord( ... );
            Java.Rvalue[] valueOrdinals = new Java.Rvalue[this.valueFieldNames.length];
            for (int i = 0; i < this.valueFieldNames.length; i++) {
                valueOrdinals[i] = createMapAssignmentRValue(this.valueTypes[i], formalParameters[i + 2].name, associateMethodBody);
            }

            associateMethodBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createArrayElementAccessExpr(
                                    getLocation(),
                                    JaninoGeneralGen.createArrayElementAccessExpr(
                                            JaninoGeneralGen.getLocation(),
                                            JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                                            indexAP.read()
                                    ),
                                    insertionIndexAP.read()
                            ),
                            new Java.NewClassInstance(
                                    getLocation(),
                                    null,
                                    createReferenceType(getLocation(), this.valueRecordDeclaration.name),
                                    valueOrdinals
                            )
                    )
            );

        }

        // this.keysRecordCount[index]++;
        associateMethodBody.add(
                JaninoOperatorGen.postIncrementStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), keysRecordCountAP.getVariableName()),
                                indexAP.read()
                        )
                )
        );

        // Invoke the putHashEntry method if the records was a new record
        // if (newEntry) {
        //     boolean rehashOnCollision = this.numberOfRecords > (3 * this.hashTable.length) / 4;
        //     putHashEntry(preHash, newIndex, rehashOnCollision);
        // }
        Java.Block hashMaintentanceBlock = new Java.Block(JaninoGeneralGen.getLocation());

        hashMaintentanceBlock.addStatement(
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

        hashMaintentanceBlock.addStatement(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[1].name),
                                indexAP.read(),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "rehashOnCollision")
                        }
                )
        );

        associateMethodBody.add(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        newEntryAP.read(),
                        hashMaintentanceBlock
                )
        );


        // public void associate([keyType] key, long preHash, [values ...])
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                ASSOCIATE_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
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
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
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
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[1].name))
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
        // while (this.keys[currentIndex] != key) {
        //     currentIndex = this.next[currentIndex];
        //     if (currentIndex == -1)
        //         return -1;
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

        Java.Block nextPointerLoopBody = new Java.Block(JaninoGeneralGen.getLocation());
        findMethodBody.add(
                JaninoControlGen.createWhileLoop(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.neq(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), this.keysAP.getVariableName()),
                                        currentIndex.read()
                                ),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name)
                        ),
                        nextPointerLoopBody
                )
        );

        nextPointerLoopBody.addStatement(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        currentIndex.write(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                currentIndex.read()
                        )
                )
        );

        nextPointerLoopBody.addStatement(
                JaninoControlGen.createIf(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.eq(
                                JaninoGeneralGen.getLocation(),
                                currentIndex.read(),
                                createIntegerLiteral(getLocation(), -1)
                            ),
                        createReturnStm(JaninoGeneralGen.getLocation(), createIntegerLiteral(getLocation(), -1))
                )
        );

        // We found the key record: return currentIndex;
        findMethodBody.add(createReturnStm(JaninoGeneralGen.getLocation(), currentIndex.read()));

        // private int find([keyType key], long preHash)
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PRIVATE,
                toJavaType(getLocation(), P_INT),
                FIND_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
                findMethodBody
        );

    }

    /**
     * Method to generate the "growArrays" method to grow the key and value record arrays they become too small.
     */
    private void generateGrowArraysMethod() {
        List<Java.Statement> growArraysMethodBody = new ArrayList<>();

        // int currentSize = this.keys.length;
        ScalarVariableAccessPath currentSize = new ScalarVariableAccessPath("currentSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), currentSize.getType()),
                        currentSize.getVariableName(),
                        new Java.FieldAccessExpression(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), keysAP.getVariableName()),
                                "length"
                        )
                )
        );

        // int newSize = currentSize * [dataGrowFactor];
        ScalarVariableAccessPath newSize = new ScalarVariableAccessPath("newSize", P_INT);
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), newSize.getType()),
                        newSize.getVariableName(),
                        JaninoOperatorGen.mul(JaninoGeneralGen.getLocation(), currentSize.read(), JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), dataGrowFactor))
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

        // Grow, copy and fill the keys array
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), primitiveArrayTypeForPrimitive(this.keyType)),
                        "newKeys",
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(this.keyType),
                                newSize.read()
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), this.keysAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newKeys"),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newKeys"),
                                currentSize.read(),
                                newSize.read(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), this.keysAP.getVariableName()),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newKeys")
                )
        );

        // Grow and copy the keysRecordCount array
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), keysRecordCountAP.getType()),
                        "newKeysRecordCount",
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(primitiveMemberTypeForArray(keysRecordCountAP.getType())),
                                newSize.read()
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), keysRecordCountAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newKeysRecordCount"),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), keysRecordCountAP.getVariableName()),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newKeysRecordCount")
                )
        );

        // Grow, copy and fill the next array
        growArraysMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), primitiveArrayTypeForPrimitive(this.keyType)),
                        "newNext",
                        createNewPrimitiveArray(
                                JaninoGeneralGen.getLocation(),
                                toJavaPrimitive(this.keyType),
                                newSize.read()
                        )
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                        "arraycopy",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newNext"),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                currentSize.read()
                        }
                )
        );

        growArraysMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newNext"),
                                currentSize.read(),
                                newSize.read(),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
                )
        );

        growArraysMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newNext")
                )
        );

        // Grow and copy all the value records array
        if (this.valueFieldNames.length > 0) {
            growArraysMethodBody.add(
                    createLocalVariable(
                            JaninoGeneralGen.getLocation(),
                            new Java.ArrayType(new Java.ArrayType(createReferenceType(getLocation(), this.valueRecordDeclaration.name))),
                            "newValues",
                            new Java.NewArray(
                                    JaninoGeneralGen.getLocation(),
                                    createReferenceType(getLocation(), this.valueRecordDeclaration.name),
                                    new Java.Rvalue[]{
                                            newSize.read(),
                                            createIntegerLiteral(getLocation(), initialRecordsPerKeyCount)
                                    },
                                    0
                            )
                    )
            );

            growArraysMethodBody.add(
                    createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "System"),
                            "arraycopy",
                            new Java.Rvalue[]{
                                    JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newValues"),
                                    JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0),
                                    currentSize.read()
                            }
                    )
            );

            growArraysMethodBody.add(
                    createVariableAssignmentStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), valueRecordArrayName),
                            JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "newValues")
                    )
            );
        }

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
                new Java.FunctionDeclarator.FormalParameter[3];

        int prehashParamIndex = 0;
        formalParameters[prehashParamIndex] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_LONG),
                "preHash"
        );

        int indexParamIndex = 1;
        formalParameters[indexParamIndex] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_INT),
                "index"
        );

        int rehashOnCollisionParamIndex = 2;
        formalParameters[rehashOnCollisionParamIndex] = createFormalParameter(
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
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[prehashParamIndex].name)
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
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[indexParamIndex].name)
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
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[rehashOnCollisionParamIndex].name),
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

        // while (this.next[currentIndex] != -1) {
        //     currentIndex = this.next[currentIndex];
        // }
        putHEMethodBody.add(
                JaninoControlGen.createWhileLoop(
                        JaninoGeneralGen.getLocation(),
                        JaninoOperatorGen.neq(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                        currentIndex.read()
                                ),
                                createIntegerLiteral(getLocation(), -1)
                        ),
                        createVariableAssignmentStm(
                                getLocation(),
                                currentIndex.write(),
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                        currentIndex.read()
                                )
                        )
                )
        );

        // At this point currentKeyRecord has property that currentKeyRecord.next == null, so we update it to
        // point to index to ensure the collision list is correct
        // currentKeyRecord.next = keyRecord;
        putHEMethodBody.add(
                createVariableAssignmentStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                currentIndex.read()
                        ),
                        createAmbiguousNameRef(getLocation(), formalParameters[indexParamIndex].name)
                )
        );

        // private void putHashEntry(long preHash, int index, boolean rehashOnCollision)
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

        // Compute the new hash-table size as
        // int size = this.hashTable.length * [hashTableGrowFactor];
        ScalarVariableAccessPath size = new ScalarVariableAccessPath("size", P_INT);
        rehashMethodBody.add(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), size.getType()),
                        size.getVariableName(),
                        mul(
                                getLocation(),
                                new Java.FieldAccessExpression(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), hashTableAP.getVariableName()),
                                        "length"
                                ),
                                createIntegerLiteral(getLocation(), hashTableGrowFactor)
                        )
                )
        );

        // Initialise the new hash-table and next array
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
        //     this.next[i] = -1;
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
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        JaninoGeneralGen.getLocation(),
                                        JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                        nextPointerIndex.read()
                                ),
                                createIntegerLiteral(getLocation(), -1)
                        )
                )
        );

        // Finally insert all key-value associations in the new hash-table
        // for (int i = 0; i < this.numberOfRecords; i++) {
        //     [keyType] key = this.keys[i];
        //     long preHash = [hash_function_container].hash(key);
        //     this.putHashEntry(preHash, i, false);
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
                        JaninoOperatorGen.lt(
                                JaninoGeneralGen.getLocation(),
                                indexVar.read(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), numberOfRecordsAP.getVariableName())
                        ),
                        JaninoOperatorGen.postIncrement(JaninoGeneralGen.getLocation(), indexVar.write()),
                        hashAssociationLoopBody
                )
        );

        String key = "key";
        hashAssociationLoopBody.addStatement(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        toJavaType(JaninoGeneralGen.getLocation(), keyType),
                        key,
                        JaninoGeneralGen.createArrayElementAccessExpr(
                                JaninoGeneralGen.getLocation(),
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), this.keysAP.getVariableName()),
                                indexVar.read()
                        )
                )
        );

        hashAssociationLoopBody.addStatement(
                createLocalVariable(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.LONG),
                        "preHash",
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                switch (keyType) {
                                    case P_INT -> JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Int_Hash_Function");
                                    default -> throw new UnsupportedOperationException(
                                            "This key-type is currently not supported by the KeyMultiRecordMapGenerator");
                                },
                                "preHash",
                                new Java.Rvalue[] {
                                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), key)
                                }
                        )
                )
        );

        hashAssociationLoopBody.addStatement(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        new Java.ThisReference(JaninoGeneralGen.getLocation()),
                        PUT_HASH_ENTRY_METHOD_NAME,
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "preHash"),
                                indexVar.read(),
                                new Java.BooleanLiteral(JaninoGeneralGen.getLocation(), "false")
                        }
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
     * Method to generate the public "getIndex" method, which returns the index in the map that
     * contains the values for a given key, or -1 if the map does not contain the key.
     */
    private void generateGetIndexMethod() {
        // Generate the method signature
        Java.FunctionDeclarator.FormalParameter[] formalParameters =
                new Java.FunctionDeclarator.FormalParameter[2];

        formalParameters[0] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), keyType),
                "key"
        );

        formalParameters[1] = createFormalParameter(
                JaninoGeneralGen.getLocation(),
                toJavaType(JaninoGeneralGen.getLocation(), P_LONG),
                "preHash"
        );

        // Generate the method body
        List<Java.Statement> getIndexMethodBody = new ArrayList<>();

        // Check that the provided key is non-negative
        getIndexMethodBody.add(
                generateNonNegativeCheck(
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name)
                )
        );

        // Return the result of the find method
        getIndexMethodBody.add(
                createReturnStm(
                        JaninoGeneralGen.getLocation(),
                        createMethodInvocation(
                                JaninoGeneralGen.getLocation(),
                                new Java.ThisReference(JaninoGeneralGen.getLocation()),
                                FIND_METHOD_NAME,
                                new Java.Rvalue[] {
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[0].name),
                                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), formalParameters[1].name)
                                }
                        )
                )
        );

        // public int getIndex([keyType key], long preHash)
        createMethod(
                JaninoGeneralGen.getLocation(),
                this.mapDeclaration,
                Access.PUBLIC,
                toJavaType(getLocation(), P_INT),
                GET_INDEX_METHOD_NAME,
                createFormalParameters(JaninoGeneralGen.getLocation(), formalParameters),
                getIndexMethodBody
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

        // Arrays.fill(this.keys, -1);
        resetMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), this.keysAP.getVariableName()),
                                createIntegerLiteral(getLocation(), -1)
                        }
                )
        );

        // Arrays.fill(this.keysRecordCount, 0);
        resetMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[]{
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), keysRecordCountAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), 0)
                        }
                )
        );

        // Arrays.fill(this.next, -1);
        resetMethodBody.add(
                createMethodInvocationStm(
                        JaninoGeneralGen.getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "Arrays"),
                        "fill",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createThisFieldAccess(JaninoGeneralGen.getLocation(), nextArrayAP.getVariableName()),
                                JaninoGeneralGen.createIntegerLiteral(JaninoGeneralGen.getLocation(), -1)
                        }
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

        // this.records = new RecordType[this.keys.length][initialRecordsPerKeyCount];
        if (this.valueFieldNames.length > 0) {
            resetMethodBody.add(
                    createVariableAssignmentStm(
                            getLocation(),
                            createThisFieldAccess(getLocation(), valueRecordArrayName),
                            new Java.NewArray(
                                    getLocation(),
                                    createReferenceType(getLocation(), this.valueRecordDeclaration.name),
                                    new Java.Rvalue[]{
                                            new Java.FieldAccessExpression(
                                                    getLocation(),
                                                    createThisFieldAccess(getLocation(), this.keysAP.getVariableName()),
                                                    "length"
                                            ),
                                            createIntegerLiteral(getLocation(), initialRecordsPerKeyCount)
                                    },
                                    0
                            )
                    )
            );
        }

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
        //     throw new IllegalArgumentException("The map expects non-negative keys");
        return JaninoControlGen.createIf(
                JaninoGeneralGen.getLocation(),
                JaninoOperatorGen.lt(
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
                                        JaninoGeneralGen.createStringLiteral(JaninoGeneralGen.getLocation(), "\"The map expects non-negative keys\"")
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
