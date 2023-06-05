package evaluation.codegen;

import evaluation.codegen.infrastructure.CodeGenContext;
import evaluation.codegen.infrastructure.janino.JaninoClassGen;
import evaluation.codegen.infrastructure.janino.JaninoGeneralGen;
import evaluation.codegen.infrastructure.janino.JaninoMethodGen;
import evaluation.codegen.infrastructure.janino.JaninoVariableGen;
import evaluation.codegen.operators.CodeGenOperator;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.vector.IntVector;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.*;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

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
                VectorSpecies.class.getName(),
                IntVector.class.getName()
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
                        new Java.FunctionDeclarator.FormalParameter[] {
                                JaninoMethodGen.createFormalParameter(
                                        this.classToType(getLocation(), CodeGenContext.class),
                                        cCtxParamName
                                )
                        }
                ),
                new Java.SuperConstructorInvocation(
                        getLocation(),
                        null,
                        new Java.Rvalue[] {
                                JaninoVariableGen.createVariableRef(getLocation(), cCtxParamName)
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
                JaninoVariableGen.createVariableRef(getLocation(), "toPrint"),
                JaninoMethodGen.createMethodInvocation(
                        getLocation(),
                        JaninoVariableGen.createVariableRef(getLocation(), "cCtx"),
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
                                JaninoVariableGen.createVariableRef(getLocation(), "toPrint")
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
