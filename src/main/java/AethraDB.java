import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;
import org.apache.commons.cli.*;
import org.checkerframework.checker.units.qual.C;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;
import org.codehaus.janino.SimpleCompiler;
import org.locationtech.proj4j.proj.Eckert1Projection;
import util.janino.GeneratorMethods;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class of the AethraDB database engine, used for invoking the engine for testing purposes.
 */
public class AethraDB {

    private static Option dbDirectoryPath;
    private static Option queryFilePath;

    /**
     * Main entry point of the application.
     * @param args A list of arguments influencing the behaviour of the engine.
     */
    public static void main(String[] args) throws IOException, SqlParseException, ValidationException, RelConversionException, CompileException {
        // Define argument parser
        Options cliOptions = createOptionConfiguration();
        CommandLineParser cliParser = new DefaultParser();
        HelpFormatter cliHelpFormatter = new HelpFormatter();
        CommandLine cmdArguments = null;

        // Parse the arguments
        try {
            cmdArguments = cliParser.parse(cliOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelpFormatter.printHelp("Usage:", cliOptions);
            System.exit(0);
        }

        // Create the database instance
//        ArrowDatabase database = new ArrowDatabase(cmdArguments.getOptionValue(dbDirectoryPath));
//
//        // Print the schema for debug purposes
//        System.out.println("[Database schema]");
//        database.printSchema();
//        System.out.println();
//
//        // Read the query
//        File queryFile = new File(cmdArguments.getOptionValue(queryFilePath));
//        if (!queryFile.exists() || !queryFile.isFile())
//            throw new IllegalStateException("The query file does not exist");
//        String textualSqlQuery = Files.readString(queryFile.toPath());
//
//        // Parse query into AST
//        SqlNode parsedSqlQuery = database.parseQuery(textualSqlQuery);
//        System.out.println("[Parsed query]");
//        System.out.println(parsedSqlQuery.toString());
//        System.out.println();
//
//        // Validate the parsed query
//        RelNode validatedSqlQuery = database.validateQuery(parsedSqlQuery);
//        System.out.println("[Validated query]");
//        System.out.println(validatedSqlQuery.toString());
//        System.out.println();
//
//        // Plan the query
//        RelNode logicalQueryPlan = database.planQuery(validatedSqlQuery);
//        System.out.println("[Optimised query]");
//        System.out.println(RelOptUtil.toString(logicalQueryPlan));

        Java.CompilationUnit testUnit = new Java.CompilationUnit("JaninoTest.java");

        // Create a test class for encapsulation
        final Java.PackageMemberClassDeclaration testClass = GeneratorMethods.createClass(
                testUnit, Access.PUBLIC, "TestClassJanino", null);

        // Create a testMethod() which computes int x = 2; double y = x * 3 + 5.0; return y;
        List<Java.Statement> testMethodBody = new ArrayList<>();

        Java.Block computationBlock = GeneratorMethods.createBlock();
        computationBlock.addStatement(GeneratorMethods.createLocalVarDecl(Java.Primitive.INT, "x", "2"));
        computationBlock.addStatement(GeneratorMethods.createLocalVarDecl(Java.Primitive.DOUBLE, "y"));
        computationBlock.addStatement( // y = x * 3 + 5.0
                GeneratorMethods.createVariableAssignmentExpr(
                        GeneratorMethods.createVariableRef("y"),
                        GeneratorMethods.createAdditionOp(
                                GeneratorMethods.createMultiplicationOp(
                                        GeneratorMethods.createVariableRef("x"),
                                        GeneratorMethods.createIntegerLiteral("3")
                                ),
                                GeneratorMethods.createFloatingPointLiteral("5.0")
                        )
                )
        );

        computationBlock.addStatement(GeneratorMethods.createReturnStm(
                GeneratorMethods.createVariableRef("y")
        ));
        testMethodBody.add(computationBlock);

        GeneratorMethods.createMethod(
                testClass,
                Access.PUBLIC,
                GeneratorMethods.createPrimitiveType(Java.Primitive.DOUBLE),
                "testMethod",
                testMethodBody);

        // Try to evaluate the program thus far
        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.cook(testUnit);

            ClassLoader loader = compiler.getClassLoader();

            Class<?> compiledClass = loader.loadClass("TestClassJanino");
            Object compiledClassInstance = compiledClass.getDeclaredConstructor().newInstance();
            Method methodToInvoke = compiledClass.getMethod("testMethod");
            double res = (Double) methodToInvoke.invoke(compiledClassInstance);
            System.out.println(res);
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    /**
     * A "Clever" method to get a location from a stack trace.
     */
    private static Location
    getLocation() {
        Exception         e   = new Exception();
        StackTraceElement ste = e.getStackTrace()[1]; //we only care about our caller
        return new Location(
                ste.getFileName(),
                ste.getLineNumber(),
                0
        );
    }

    /**
     * Method which creates the configuration required for the CLI argument parser.
     * @return A description of accepted/required CLI arguments.
     */
    public static Options createOptionConfiguration() {
        Options options = new Options();

        // Define option for supplying Apache Arrow IPC database directory
        dbDirectoryPath = Option
                .builder("d")
                .longOpt("dbFolder")
                .hasArg(true)
                .required(true)
                .desc("The directory containing the Apache Arrow IPC files that make up the database")
                .build();
        options.addOption(dbDirectoryPath);

        // Define option for supplying the query file
        queryFilePath = Option
                .builder("q")
                .longOpt("query")
                .hasArg(true)
                .required(true)
                .desc("The file containing the SQL query to be executed")
                .build();
        options.addOption(queryFilePath);

        return options;
    }

}
