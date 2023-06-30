package evaluation.codegen;

import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.operators.CodeGenOperator;
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

import static evaluation.codegen.infrastructure.janino.JaninoClassGen.addPackageMemberClassDeclaration;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createPrimitiveType;
import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createConstructor;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameter;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createFormalParameters;
import static evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethod;

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
     * @param rootOperator The root operator of the query for which we need to perform code generation.
     * @param useVectorised Whether to use the vectorised produce method on the root operator.
     */
    public QueryCodeGenerator(CodeGenOperator<?> rootOperator, boolean useVectorised) {
        // Perform SimpleCompiler constructor
        super();

        // Initialise member variables
        this.rootOperator = rootOperator;
        this.rootOperatorVectorised = useVectorised;
        this.cCtx = new CodeGenContext();
        this.oCtx = new OptimisationContext();
        this.generated = false;
        this.generatedQuery = null;
        this.generatedQueryClassName = "GeneratedQuery_" + rootOperator.hashCode();
        this.defaultImports = new String[] {
                "evaluation.general_support.hashmaps.Int_Hash_Function",
                "evaluation.general_support.hashmaps.Simple_Int_Long_Map",
                "evaluation.general_support.hashmaps.Simple_Int_Long_Map.Simple_Int_Long_Map_Iterator",
                "evaluation.general_support.hashmaps.Int_Multi_Object_Map",
                "evaluation.general_support.hashmaps.Int_Multi_Object_Map.Int_Multi_Object_Map_Iterator",

                "evaluation.codegen.infrastructure.data.ArrowTableReader",

                "evaluation.vector_support.VectorisedAggregationOperators",
                "evaluation.vector_support.VectorisedFilterOperators",
                "evaluation.vector_support.VectorisedOperators",
                "evaluation.vector_support.VectorisedPrintOperators",

                "java.lang.foreign.MemorySegment",
                "java.util.List"
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

        // Generate the query
        Java.AbstractCompilationUnit.ImportDeclaration[] importDeclarations = this.makeImportDeclarations();
        Java.CompilationUnit generatedQueryUnit = new Java.CompilationUnit(
                this.generatedQueryClassName + ".java",
                importDeclarations
        );

        // Generate the class representing the query
        // public GeneratedQueryClass extends GeneratedQuery
        Java.PackageMemberClassDeclaration queryClass = addPackageMemberClassDeclaration(
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
        String oCtxParamName = "oCtx";
        createConstructor(
                getLocation(),
                queryClass,
                Access.PUBLIC,
                createFormalParameters(
                        getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[] {
                                createFormalParameter(
                                        getLocation(),
                                        this.classToType(getLocation(), CodeGenContext.class),
                                        cCtxParamName
                                ),
                                createFormalParameter(
                                        getLocation(),
                                        this.classToType(getLocation(), OptimisationContext.class),
                                        oCtxParamName
                                )
                        }
                ),
                new Java.SuperConstructorInvocation(
                        getLocation(),
                        null,
                        new Java.Rvalue[] {
                                createAmbiguousNameRef(getLocation(), cCtxParamName),
                                createAmbiguousNameRef(getLocation(), oCtxParamName)
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

        // Generate the execute method
        // @Override public void execute() throws IOException
        createMethod(
                getLocation(),
                queryClass,
                Access.PUBLIC,
                createPrimitiveType(getLocation(), Java.Primitive.VOID),
                "execute",
                createFormalParameters(
                        getLocation(),
                        new Java.FunctionDeclarator.FormalParameter[0]
                ),
                new Java.Type[] { this.classToType(getLocation(), IOException.class) },
                executeMethodBody
        );

        // Print the generated method body if requested
        if (printCode) {
            System.out.println("[Generated query code]");
            printCode(executeMethodBody);
            System.out.println();
        }

        // Compile the class
        this.cook(generatedQueryUnit);

        // Create an instance of the compiled class and store it
        try {
            Class<?> compiledClass = this.getClassLoader().loadClass(this.generatedQueryClassName);
            Constructor<?> compiledQueryConstructor = compiledClass.getDeclaredConstructor(
                    CodeGenContext.class,
                    OptimisationContext.class);
            this.generatedQuery = (GeneratedQuery) compiledQueryConstructor.newInstance(this.cCtx, this.oCtx);
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

        return l.toArray(new Java.AbstractCompilationUnit.ImportDeclaration[0]);
    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     */
    private static void printCode(List<Java.Statement> code) {
        printCode(code, 0);
    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     * @param indentationLevel The number of spaces to add before each line of the code.
     */
    private static void printCode(List<? extends Java.BlockStatement> code, int indentationLevel) {
        for (Java.BlockStatement statement : code)
            printCode(statement, indentationLevel);
    }

    /**
     * Method to print generated code to the standard output.
     * @param code The code to print.
     * @param indentationLevel The number of spaces to add before each line of the code.
     */
    private static void printCode(Java.BlockStatement code, int indentationLevel) {
        if (code instanceof Java.Block block) {
            printCode(block.statements, indentationLevel);

        } else if (code instanceof Java.WhileStatement whileStatement) {
            String whileGuardLine = "while (" + whileStatement.condition.toString() + ") {";
            System.out.print(whileGuardLine.indent(indentationLevel));
            printCode(whileStatement.body, indentationLevel + 4);
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.ForStatement forStatement) {
            String forGuardLine =
                    "for (" + forStatement.init.toString() + " "
                            + forStatement.condition.toString() + "; "
                            + forStatement.update[0].toString() + ") {";
            System.out.print(forGuardLine.indent(indentationLevel));
            printCode(forStatement.body, indentationLevel + 4);
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.ForEachStatement forEachStatement) {
            String forGuardLine =
                    "foreach (" + forEachStatement.currentElement.toString() + " : "
                                + forEachStatement.expression.toString() + ") {";
            System.out.print(forGuardLine.indent(indentationLevel));
            printCode(forEachStatement.body, indentationLevel + 4);
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.IfStatement ifStatement) {
            String ifGuardLine = "if (" + ifStatement.condition.toString() + ") {";
            System.out.print(ifGuardLine.indent(indentationLevel));
            printCode(ifStatement.thenStatement, indentationLevel + 4);
            if (ifStatement.elseStatement != null) {
                System.out.print("} else }".indent(indentationLevel));
                printCode(ifStatement.elseStatement, indentationLevel + 4);
            }
            System.out.print("}".indent(indentationLevel));

        } else if (code instanceof Java.LocalClassDeclarationStatement lcdStatement) {
            Java.LocalClassDeclaration lcd = lcdStatement.lcd;
            String classDeclarationLine = "";
            for (Java.Modifier modifier : lcd.getModifiers())
                classDeclarationLine += modifier.toString() + " ";
            classDeclarationLine += "class " + lcd.getName() + " {";
            System.out.print(classDeclarationLine.indent(indentationLevel));

            printCode(lcd.fieldDeclarationsAndInitializers, indentationLevel + 4);
            System.out.println();

            for (Java.ConstructorDeclarator constructorDeclarator : lcd.constructors) {
                String constructorDeclarationLine = constructorDeclarator.getModifiers()[0].toString();
                constructorDeclarationLine += " " + constructorDeclarator.getDeclaringType().toString() + "(";

                Java.FunctionDeclarator.FormalParameter[] parameters = constructorDeclarator.formalParameters.parameters;
                for (int i = 0; i < parameters.length; i++) {
                    constructorDeclarationLine += parameters[i].toString();

                    if (i != parameters.length - 1)
                        constructorDeclarationLine += ", ";
                }

                constructorDeclarationLine += ") {";
                System.out.print(constructorDeclarationLine.indent(indentationLevel + 4));

                printCode(constructorDeclarator.statements, indentationLevel + 8);
                System.out.print("}".indent(indentationLevel + 4));
            }

            System.out.println("}".indent(indentationLevel));

        } else if (code instanceof Java.FieldDeclaration fieldDeclaration) {
            System.out.print((fieldDeclaration.toString() + ";").indent(indentationLevel));

        } else {
            System.out.print(code.toString().indent(indentationLevel));
        }
    }

}
