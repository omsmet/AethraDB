package evaluation.codegen;

import evaluation.codegen.infrastructure.CodeGenContext;
import evaluation.codegen.infrastructure.janino.*;
import evaluation.codegen.operators.CodeGenOperator;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.*;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.*;

/**
 * Class taking care of general purpose operations in query code generation.
 * We extend the {@link org.codehaus.janino.SimpleCompiler} class to make compilation easier.
 */
public class QueryCodeGenerator extends SimpleCompiler {

    /**
     * The root operator of the query for which code should be generated.
     */
    private final CodeGenOperator rootOperator;

    /**
     * The {@link CodeGenContext} that was/will be used for generating the query.
     */
    private final CodeGenContext cCtx;

    /**
     * Variable for keeping track of whether the query has already been generated.
     */
    private boolean generated;

    /**
     * Variable storing the generated query when {@code this.generated == true}.
     */
    private GeneratedQuery generatedQuery;

    /**
     * Variable storing the name of the class that was/will be generated for the query.
     */
    private final String generatedQueryClassName;

    /**
     * Variable storing the classes that will be imported by the generated query class.
     */
    private final String[] defaultImports;

    /**
     * Creates a {@link QueryCodeGenerator} instance for a specific query.
     * @param rootOperator The root operator of the query for which we need to perform code generation.
     */
    public QueryCodeGenerator(CodeGenOperator rootOperator) {
        // Perform SimpleCompiler constructor
        super();

        // Initialise member variables
        this.rootOperator = rootOperator;
        this.cCtx = new CodeGenContext(42);
        this.generated = false;
        this.generatedQuery = null;
        this.generatedQueryClassName = "GeneratedQuery_" + rootOperator.hashCode();
        this.defaultImports = new String[] {
                jdk.incubator.vector.VectorSpecies.class.getName(),
                jdk.incubator.vector.IntVector.class.getName()
        };

        // Initialise the compiler, so it will allow us to generate a class which extends
        // the GeneratedQuery class and import necessary dependencies
        ClassLoader globalClassLoader = ClassLoader.getSystemClassLoader();
        this.setParentClassLoader(globalClassLoader);
    }

    /**
     * Method performing the actual code generation for the query represented by {@code this.rootOperator}.
     * @return the {@link GeneratedQuery} corresponding to the query represented by {@code this.rootOperator}.
     * @throws Exception when an exception occurs during the code generation stage.
     */
    public GeneratedQuery generateQuery() throws Exception {
        // Return the cached query if we have already generated the query
        if (generated)
            return this.generatedQuery;

        // Generate the query
        Java.AbstractCompilationUnit.ImportDeclaration[] importDeclarations = this.makeImportDeclarations();
        Java.CompilationUnit generatedQueryUnit = new Java.CompilationUnit(
                this.generatedQueryClassName + ".java",
                importDeclarations
        );

        // Generate the class representing the query
        // public GeneratedQueryClass extends GeneratedQuery
        Java.PackageMemberClassDeclaration queryClass = JaninoClassGen.addPackageMemberClassDeclaration(
                getLocation(),
                Access.PUBLIC,
                generatedQueryUnit,
                generatedQueryClassName,
                this.classToType(getLocation(), GeneratedQuery.class)
        );

        // Add a constructor matching the GeneratedQuery super-constructor
        // public GeneratedQueryClass(CodeGenContext cCtx) {
        //     super(cCtx);
        // }
        String cCtxParamName = "cCtx";
        JaninoMethodGen.createConstructor(
                getLocation(),
                queryClass,
                Access.PUBLIC,
                JaninoMethodGen.createFormalParameters(
                        getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[] {
                                JaninoMethodGen.createFormalParameter(
                                        getLocation(),
                                        this.classToType(getLocation(), CodeGenContext.class),
                                        cCtxParamName
                                )
                        }
                ),
                new Java.SuperConstructorInvocation(
                        getLocation(),
                        null,
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createAmbiguousNameRef(getLocation(), cCtxParamName)
                        }
                ),
                new ArrayList<>()
        );

        // Create the body for the execute method
        // Currently a method that verifies that cCtx can be accessed at runtime
        // TODO: replace by operator tree generation
        List<Java.Statement> executeMethodBody = new ArrayList<>();

        // int toPrint;
        executeMethodBody.add(JaninoVariableGen.createPrimitiveLocalVar(
                getLocation(),
                Java.Primitive.INT,
                "toPrint"
        ));

        // toPrint = cCtx.getTest();
        executeMethodBody.add(JaninoVariableGen.createVariableAssignmentExpr(
                getLocation(),
                JaninoGeneralGen.createAmbiguousNameRef(getLocation(), "toPrint"),
                JaninoMethodGen.createMethodInvocation(
                        getLocation(),
                        JaninoGeneralGen.createAmbiguousNameRef(getLocation(), "cCtx"),
                        "getTest",
                        new Java.Rvalue[0]
                )
        ));

        // System.out.println(toPrint);
        executeMethodBody.add(
                JaninoMethodGen.createMethodInvocationStm(
                        getLocation(),
                        new Java.AmbiguousName(getLocation(), new String[] {"System", "out"}),
                        "println",
                        new Java.Rvalue[]{
                                JaninoGeneralGen.createAmbiguousNameRef(getLocation(), "toPrint")
                        }
                )
        );

        // We also add the vectorised processing example.
        // int[] testArray = new int[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        executeMethodBody.add(
            JaninoVariableGen.createLocalVariable(
                    getLocation(),
                    JaninoGeneralGen.createPrimitiveArrayType(getLocation(), Java.Primitive.INT),
                    "testArray",
                    JaninoGeneralGen.createPrimitiveArrayInitialiser(
                            getLocation(),
                            Java.Primitive.INT,
                            new String[] { "1", "2", "3", "4", "5", "6", "7", "8" }
                    )
            )
        );

        // VectorSpecies<Integer> integerVectorSpecies = IntVector.SPECIES_PREFERRED;
        executeMethodBody.add(
                JaninoVariableGen.createLocalVariable(
                        getLocation(),
                        JaninoGeneralGen.createReferenceType(
                                getLocation(), "VectorSpecies", "Integer"),
                        "integerVectorSpecies",
                        new Java.AmbiguousName(
                                getLocation(),
                                new String[] {
                                        "IntVector",
                                        "SPECIES_PREFERRED"
                                })
                )
        );

        // IntVector testArrayVector = IntVector.fromArray(integerVectorSpecies, testArray, 0);
        executeMethodBody.add(
                JaninoVariableGen.createLocalVariable(
                        getLocation(),
                        JaninoGeneralGen.createReferenceType(getLocation(), "IntVector"),
                        "testArrayVector",
                        JaninoMethodGen.createMethodInvocation(
                                getLocation(),
                                JaninoGeneralGen.createAmbiguousNameRef(getLocation(),"IntVector"),
                                "fromArray",
                                new Java.Rvalue[] {
                                        createAmbiguousNameRef(getLocation(), "integerVectorSpecies"),
                                        createAmbiguousNameRef(getLocation(), "testArray"),
                                        createIntegerLiteral(getLocation(), "0")
                                }
                        )
                )
        );

        // IntVector resultVector = testArrayVector.add(42);
        executeMethodBody.add(
                JaninoVariableGen.createLocalVariable(
                        getLocation(),
                        createReferenceType(getLocation(), "IntVector"),
                        "resultVector",
                        JaninoMethodGen.createMethodInvocation(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "testArrayVector"),
                                "add",
                                new Java.Rvalue[] {
                                        createIntegerLiteral(getLocation(), "42")
                                }
                        )
                )
        );

        // resultVector.intoArray(testArray, 0);
        executeMethodBody.add(
                JaninoMethodGen.createMethodInvocationStm(
                        getLocation(),
                        createAmbiguousNameRef(getLocation(), "resultVector"),
                        "intoArray",
                        new Java.Rvalue[] {
                                createAmbiguousNameRef(getLocation(), "testArray"),
                                createIntegerLiteral(getLocation(), "0")
                        }
                )
        );

        // for (int i = 0; i < testArray.length; i++) System.out.println(testArray[i]);
        Java.Block forLoopBody = JaninoMethodGen.createBlock(getLocation());
        executeMethodBody.add(
                JaninoControlGen.createForLoop(
                        getLocation(),
                        JaninoVariableGen.createPrimitiveLocalVar(                                                        // int i = 0
                                getLocation(),
                                Java.Primitive.INT,
                                "i",
                                "0"
                        ),
                        JaninoOperatorGen.lt(                                                                             // i < testArray.length
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), "i"),
                                createAmbiguousNameRef(getLocation(), "testArray.length")),
                        JaninoOperatorGen.postIncrement(getLocation(), createAmbiguousNameRef(getLocation(), "i")), // i++
                        forLoopBody
                )
        );

        forLoopBody.addStatement(
                JaninoMethodGen.createMethodInvocationStm(
                        getLocation(),
                        new Java.AmbiguousName(getLocation(), new String[] {"System", "out"}),
                        "println",
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createArrayElementAccessExpr(
                                        getLocation(),
                                        createAmbiguousNameRef(getLocation(), "testArray"),
                                        createAmbiguousNameRef(getLocation(), "i")
                                )
                        }
                )
        );

        // Generate the execute method
        // @Override public void execute()
        JaninoMethodGen.createMethod(
                getLocation(),
                queryClass,
                Access.PUBLIC,
                JaninoGeneralGen.createPrimitiveType(getLocation(), Java.Primitive.VOID),
                "execute",
                executeMethodBody
        );

        // Compile the class
        this.cook(generatedQueryUnit);

        // Create an instance of the compiled class and store it
        try {
            Class<?> compiledClass = this.getClassLoader().loadClass(this.generatedQueryClassName);
            Constructor<?> compiledQueryConstructor = compiledClass.getDeclaredConstructor(CodeGenContext.class);
            this.generatedQuery = (GeneratedQuery) compiledQueryConstructor.newInstance(this.cCtx);
            this.generated = true;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not instantiate the generated query class: " + e.getMessage());
        }

        // Return the generated query
        return this.generatedQuery;
    };

    /**
     * Method for creating an array containing all required imports from their String names.
     * @return The {@link Java.AbstractCompilationUnit.ImportDeclaration}[] corresponding to {@code this.defaultImports}.
     * @throws CompileException if one of the import declarations cannot be parsed.
     * @throws IOException if one of the import declarations cannot be parsed.
     */
    private Java.AbstractCompilationUnit.ImportDeclaration[]
    makeImportDeclarations() throws CompileException, IOException {

        List<Java.AbstractCompilationUnit.ImportDeclaration>
                l = new ArrayList<>();

        // Honor the default imports.
        for (String defaultImport : this.defaultImports) {

            final Parser p = new Parser(new Scanner(null, new StringReader(defaultImport)));
            p.setSourceVersion(-1);
            p.setWarningHandler(null);

            l.add(p.parseImportDeclarationBody());
            p.read(TokenType.END_OF_INPUT);
        }

        return l.toArray(
                new Java.AbstractCompilationUnit.ImportDeclaration[l.size()]
        );
    }


}
