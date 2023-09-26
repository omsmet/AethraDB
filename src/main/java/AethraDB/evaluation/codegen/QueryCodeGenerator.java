package AethraDB.evaluation.codegen;

import AethraDB.AethraDB;
import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import AethraDB.evaluation.general_support.QueryCodePrinter;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.SimpleCompiler;
import org.codehaus.janino.TokenType;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Class taking care of general purpose operations in query code generation.
 * We extend the {@link org.codehaus.janino.SimpleCompiler} class to make compilation easier.
 */
public class QueryCodeGenerator extends SimpleCompiler {

    /**
     * The root operator of the query for which code should be generated.
     */
    private final CodeGenOperator<?> rootOperator;

    /**
     * Whether to use the vectorised execution produce method on the top-level operator.
     */
    private final boolean rootOperatorVectorised;

    /**
     * The {@link CodeGenContext} that was/will be used for generating the query.
     */
    private final CodeGenContext cCtx;

    /**
     * The {@link OptimisationContext} that was/will be used for generating the query.
     */
    private final OptimisationContext oCtx;

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
     * @param cCtx The {@link CodeGenContext} to use for the query code generation.
     * @param oCtx The {@link OptimisationContext} to use for the query code generation.
     * @param rootOperator The root operator of the query for which we need to perform code generation.
     * @param useVectorised Whether to use the vectorised produce method on the root operator.
     */
    public QueryCodeGenerator(
            CodeGenContext cCtx, OptimisationContext oCtx, CodeGenOperator<?> rootOperator, boolean useVectorised) {
        // Perform SimpleCompiler constructor
        super();

        // Initialise member variables
        this.rootOperator = rootOperator;
        this.rootOperatorVectorised = useVectorised;
        this.cCtx = cCtx;
        this.oCtx = oCtx;
        this.generated = false;
        this.generatedQuery = null;
        this.generatedQueryClassName = "GeneratedQuery_" + rootOperator.hashCode();
        this.defaultImports = new String[] {
                "AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader",

                "AethraDB.evaluation.general_support.ArrowOptimisations",
                "AethraDB.evaluation.general_support.hashmaps.Double_Hash_Function",
                "AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function",
                "AethraDB.evaluation.general_support.hashmaps.Char_Arr_Hash_Function",

                "AethraDB.evaluation.non_vector_support.LikeOperatorPrimitives",

                "AethraDB.evaluation.vector_support.VectorisedAggregationOperators",
                "AethraDB.evaluation.vector_support.VectorisedArithmeticOperators",
                "AethraDB.evaluation.vector_support.VectorisedFilterOperators",
                "AethraDB.evaluation.vector_support.VectorisedHashOperators",
                "AethraDB.evaluation.vector_support.VectorisedOperators",
                "AethraDB.evaluation.vector_support.VectorisedPrintOperators",

                "java.lang.foreign.MemorySegment",
                "java.util.ArrayList",
                "java.util.Arrays"
        };

        // Initialise the compiler, so it will allow us to generate a class which extends
        // the GeneratedQuery class and import necessary dependencies
        ClassLoader globalClassLoader = ClassLoader.getSystemClassLoader();
        this.setParentClassLoader(globalClassLoader);
    }

    /**
     * Method performing the actual code generation for the query represented by {@code this.rootOperator}.
     * @param printCode Whether to print the generated code to the standard output.
     * @return the {@link GeneratedQuery} corresponding to the query represented by {@code this.rootOperator}.
     * @throws Exception when an exception occurs during the code generation stage.
     */
    public GeneratedQuery generateQuery(boolean printCode) throws Exception {
        // Return the cached query if we have already generated the query
        if (generated)
            return this.generatedQuery;
        System.out.println("Generating query code ...");

        // Generate the query
        Java.AbstractCompilationUnit.ImportDeclaration[] importDeclarations = this.makeImportDeclarations();
        Java.CompilationUnit generatedQueryUnit = new Java.CompilationUnit(
                this.generatedQueryClassName + ".java",
                importDeclarations
        );

        // Generate the class representing the query
        // public GeneratedQueryClass extends GeneratedQuery
        Java.PackageMemberClassDeclaration queryClass = JaninoClassGen.addPackageMemberClassDeclaration(
                JaninoGeneralGen.getLocation(),
                Access.PUBLIC,
                generatedQueryUnit,
                generatedQueryClassName,
                this.classToType(JaninoGeneralGen.getLocation(), GeneratedQuery.class)
        );

        // Add a constructor matching the GeneratedQuery super-constructor
        // public GeneratedQueryClass(CodeGenContext cCtx, OptimisationContext oCtx) {
        //     super(cCtx, oCtx);
        // }
        String cCtxParamName = "cCtx";
        String oCtxParamName = "oCtx";
        JaninoMethodGen.createConstructor(
                JaninoGeneralGen.getLocation(),
                queryClass,
                Access.PUBLIC,
                JaninoMethodGen.createFormalParameters(
                        JaninoGeneralGen.getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[] {
                                JaninoMethodGen.createFormalParameter(
                                        JaninoGeneralGen.getLocation(),
                                        this.classToType(JaninoGeneralGen.getLocation(), CodeGenContext.class),
                                        cCtxParamName
                                ),
                                JaninoMethodGen.createFormalParameter(
                                        JaninoGeneralGen.getLocation(),
                                        this.classToType(JaninoGeneralGen.getLocation(), OptimisationContext.class),
                                        oCtxParamName
                                )
                        }
                ),
                new Java.SuperConstructorInvocation(
                        JaninoGeneralGen.getLocation(),
                        null,
                        new Java.Rvalue[] {
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), cCtxParamName),
                                JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), oCtxParamName)
                        }
                ),
                new ArrayList<>()
        );

        // Create the body for the execute method
        List<Java.Statement> executeMethodBody;
        if (this.rootOperatorVectorised && this.rootOperator.canProduceVectorised())
            executeMethodBody = this.rootOperator.produceVec(this.cCtx, this.oCtx);
        else if (!this.rootOperatorVectorised && this.rootOperator.canProduceNonVectorised())
            executeMethodBody = this.rootOperator.produceNonVec(this.cCtx, this.oCtx);
        else
            throw new UnsupportedOperationException("Attempting to invoke a CodeGenOperator produce method that is not supported");

        // Introduce the query-global variable declarations
        var queryGlobalVariableDeclarations = this.cCtx.getQueryGlobalVariables();
        List<Java.Statement> completedExecuteMethodBody = new ArrayList<>(queryGlobalVariableDeclarations.left);
        completedExecuteMethodBody.addAll(executeMethodBody);
        for (String varToDallocate : queryGlobalVariableDeclarations.right) {
            completedExecuteMethodBody.add(
                    JaninoMethodGen.createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoMethodGen.createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "release",
                            new Java.Rvalue[] {
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), varToDallocate)
                            }
                    )
            );
        }

        // Generate the execute method
        // @Override public void execute() throws IOException
        JaninoMethodGen.createMethod(
                JaninoGeneralGen.getLocation(),
                queryClass,
                Access.PUBLIC,
                JaninoGeneralGen.createPrimitiveType(JaninoGeneralGen.getLocation(), Java.Primitive.VOID),
                "execute",
                JaninoMethodGen.createFormalParameters(
                        JaninoGeneralGen.getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[0]
                ),
                new Java.Type[] { this.classToType(JaninoGeneralGen.getLocation(), IOException.class) },
                completedExecuteMethodBody
        );

        AethraDB.codeGenerationEnd = System.nanoTime();
        System.out.println("Finished query code generation!");
        System.out.println();

        // Print the generated method body if requested
        if (printCode) {
            System.out.println("[Generated query code]");
            QueryCodePrinter.printCode(completedExecuteMethodBody);
            System.out.println();
        }

        // Compile the class
        AethraDB.codeCompilationStart = System.nanoTime();
        this.cook(generatedQueryUnit);

        // Create an instance of the compiled class and store it
        try {
            Class<?> compiledClass = this.getClassLoader().loadClass(this.generatedQueryClassName);
            Constructor<?> compiledQueryConstructor = compiledClass.getDeclaredConstructor(
                    CodeGenContext.class,
                    OptimisationContext.class);
            this.generatedQuery = (GeneratedQuery) compiledQueryConstructor.newInstance(this.cCtx, this.oCtx);
            this.generated = true;
            AethraDB.codeCompilationEnd = System.nanoTime();
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

        return l.toArray(new Java.AbstractCompilationUnit.ImportDeclaration[0]);
    }

}