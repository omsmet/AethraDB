import evaluation.codegen.GeneratedQuery;
import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.translation.NonVectorisedQueryTranslator;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.cli.*;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import util.arrow.ArrowDatabase;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;

/**
 * Main class of the AethraDB database engine, used for invoking the engine for testing purposes.
 */
public class AethraDB {

    /**
     * Command line option for obtaining the directory in which the Arrow database is located.
     */
    private static Option dbDirectoryPath;

    /**
     * Command line option for obtaining the file that contains the SQL query to be executed.
     */
    private static Option queryFilePath;

    /**
     * Main entry point of the application.
     * @param args A list of arguments influencing the behaviour of the engine.
     */
    public static void main(String[] args) throws IOException, SqlParseException, ValidationException, RelConversionException {
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
        ArrowDatabase database = new ArrowDatabase(cmdArguments.getOptionValue(dbDirectoryPath));

        // Print the schema for debug purposes
        System.out.println("[Database schema]");
        database.printSchema();
        System.out.println();

        // Read the query
        File queryFile = new File(cmdArguments.getOptionValue(queryFilePath));
        if (!queryFile.exists() || !queryFile.isFile())
            throw new IllegalStateException("The query file does not exist");
        String textualSqlQuery = Files.readString(queryFile.toPath());

        // Parse query into AST
        SqlNode parsedSqlQuery = database.parseQuery(textualSqlQuery);
        System.out.println("[Parsed query]");
        System.out.println(parsedSqlQuery.toString());
        System.out.println();

        // Validate the parsed query
        RelNode validatedSqlQuery = database.validateQuery(parsedSqlQuery);
        System.out.println("[Validated query]");
        System.out.println(validatedSqlQuery.toString());
        System.out.println();

        // Plan the query
        RelNode logicalQueryPlan = database.planQuery(validatedSqlQuery);
        System.out.println("[Optimised query]");
        System.out.println(RelOptUtil.toString(logicalQueryPlan));

        // Generate code for the query
        NonVectorisedQueryTranslator queryTranslator = new NonVectorisedQueryTranslator();
        QueryCodeGenerator queryCodeGenerator = queryTranslator.translate(logicalQueryPlan);
        GeneratedQuery generatedQuery;
        try {
            generatedQuery = queryCodeGenerator.generateQuery();
        } catch (Exception e) {
            throw new RuntimeException("Could not generate code for query", e);
        }

        // Execute the generated query
        generatedQuery.execute();

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
