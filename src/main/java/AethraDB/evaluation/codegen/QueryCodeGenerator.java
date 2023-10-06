package AethraDB.evaluation.codegen;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.SimpleCompiler;
import org.codehaus.janino.TokenType;
import org.codehaus.janino.util.ClassFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
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
    private final CodeGenOperator rootOperator;

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
     * Variable storing the generated query class when {@code this.generated == true}.
     */
    private Java.PackageMemberClassDeclaration generatedQueryClass;

    /**
     * Variable storing the {@link Java.CompilationUnit} containing the generated class
     * when {@code this.generated == true}.
     */
    private Java.CompilationUnit generatedCompilationUnit;

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
            CodeGenContext cCtx, OptimisationContext oCtx, CodeGenOperator rootOperator, boolean useVectorised) {
        // Perform SimpleCompiler constructor
        super();

        // Initialise member variables
        this.rootOperator = rootOperator;
        this.rootOperatorVectorised = useVectorised;
        this.cCtx = cCtx;
        this.oCtx = oCtx;
        this.generated = false;
        this.generatedQueryClass = null;
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
     * @return the {@link GeneratedQuery} corresponding to the query represented by {@code this.rootOperator}.
     */
    public Java.PackageMemberClassDeclaration generateQueryClass() throws Exception {
        // Return the cached query if we have already generated the query
        if (generated)
            return this.generatedQueryClass;

        // Generate the query
        Java.AbstractCompilationUnit.ImportDeclaration[] importDeclarations = this.makeImportDeclarations();
        this.generatedCompilationUnit = new Java.CompilationUnit(
                this.generatedQueryClassName + ".java",
                importDeclarations
        );

        // Generate the class representing the query
        // public GeneratedQueryClass extends GeneratedQuery
        this.generatedQueryClass = JaninoClassGen.addPackageMemberClassDeclaration(
                JaninoGeneralGen.getLocation(),
                Access.PUBLIC,
                this.generatedCompilationUnit,
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
                this.generatedQueryClass,
                Access.PUBLIC,
                JaninoMethodGen.createFormalParameters(
                        JaninoGeneralGen.getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[]{
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
                        new Java.Rvalue[]{
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
        List<Java.Statement> completedExecuteMethodBody = new ArrayList<>(queryGlobalVariableDeclarations.getLeft());
        completedExecuteMethodBody.addAll(executeMethodBody);
        for (String varToDallocate : queryGlobalVariableDeclarations.getRight()) {
            completedExecuteMethodBody.add(
                    JaninoMethodGen.createMethodInvocationStm(
                            JaninoGeneralGen.getLocation(),
                            JaninoMethodGen.createMethodInvocation(
                                    JaninoGeneralGen.getLocation(),
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), "cCtx"),
                                    "getAllocationManager"
                            ),
                            "release",
                            new Java.Rvalue[]{
                                    JaninoGeneralGen.createAmbiguousNameRef(JaninoGeneralGen.getLocation(), varToDallocate)
                            }
                    )
            );
        }

        // Generate the execute method
        // @Override public void execute() throws IOException
        JaninoMethodGen.createMethod(
                JaninoGeneralGen.getLocation(),
                this.generatedQueryClass,
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

        // Return the generated class
        this.generated = true;
        return this.generatedQueryClass;
    }


    /**
     * Method which compiles the generated class and stores it on disk in a temporary folder.
     * @return The path of the temporary folder containing the compiled class and its dependencies.
     */
    public String compileQueryClass() throws CompileException {
        if (!this.generated)
            throw new IllegalStateException("Cannot compile a generated class if it has not been generated yet");

        // Compile the class
        this.cook(this.generatedCompilationUnit);

        // Create a temporary directory to store the compiled class files in
        File baseTempDir = new File(System.getProperty("java.io.tmpdir"));
        String queryTempDirName = "AethraTempQuery_" + System.nanoTime();
        File queryTempDir = new File(baseTempDir, queryTempDirName);
        boolean couldCreateTempDir = queryTempDir.mkdir();
        if (!couldCreateTempDir)
            throw new RuntimeException("Could not create a temporary directory to write the compiled query class files to");

        // Write the class files to the directory
        ClassFile[] compiledClasses = this.getClassFiles();
        for (ClassFile compiledClass : compiledClasses) {
            File ccFile = new File(queryTempDir, compiledClass.getThisClassName() + ".class");
            try (
                    FileOutputStream ccFileOS = new FileOutputStream(ccFile);
                    BufferedOutputStream ccFileBOS = new BufferedOutputStream(ccFileOS);
            ) {
                compiledClass.store(ccFileBOS);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Finally, return the directory containing the compile class files
        return queryTempDir.getAbsolutePath();
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

    /**
     * Method to obtain the {@link CodeGenContext} used by {@code this}.
     * @return The {@link CodeGenContext} used by {@code this}.
     */
    public CodeGenContext getCCtx() {
        return this.cCtx;
    }

}
