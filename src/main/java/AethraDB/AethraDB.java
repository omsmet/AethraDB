package AethraDB;

import AethraDB.evaluation.codegen.GeneratedQuery;
import AethraDB.evaluation.codegen.QueryCodeGenerator;
import AethraDB.evaluation.codegen.QueryTranslator;
import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import AethraDB.evaluation.codegen.operators.QueryResultCountOperator;
import AethraDB.evaluation.codegen.operators.QueryResultPrinterOperator;
import AethraDB.util.arrow.ArrowDatabase;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.PrintWriter;
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
     * Command line option for enabling verbose output.
     */
    private static Option verboseOutput;

    /**
     * Command line option for selecting the query processing paradigm.
     */
    private static Option processingParadigm;

    /**
     * Command line option to summarise the query result as the count of the number of result lines.
     */
    private static Option summariseAsCount;

    /**
     * Command line option to output profiling information to std err.
     */
    private static Option outputProfileInformation;

    /**
     * Variables to keep track of running-time information for main method benchmarking.
     */
    public static long queryPlanningStart = -1;
    public static long queryPlanningEnd = -1;
    public static long codeGenerationStart = -1;
    public static long codeGenerationEnd = -1;
    public static long codeCompilationStart = -1;
    public static long codeCompilationEnd = -1;
    public static long queryExecutionStart = -1;
    public static long queryExecutionEnd = -1;

    /**
     * Main entry point of the application.
     * @param args A list of arguments influencing the behaviour of the engine.
     */
    public static void main(String[] args) throws Exception {
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

        // Get the database & query paths
        String databaseDirectoryPath = cmdArguments.getOptionValue(dbDirectoryPath);
        File queryFile = new File(cmdArguments.getOptionValue(queryFilePath));
        if (!queryFile.exists() || !queryFile.isFile())
            throw new IllegalStateException("The query file does not exist");

        // Check if verbose output is enabled
        boolean verboseOutputEnabled = cmdArguments.hasOption(verboseOutput);

        // Extract the query processing paradigm to use
        String paradigmArgVal = cmdArguments.getOptionValue(processingParadigm);
        boolean useVectorisedProcessing;
        if (paradigmArgVal.equals("non-vectorised"))
            useVectorisedProcessing = false;
        else if (paradigmArgVal.equals("vectorised"))
            useVectorisedProcessing = true;
        else {
            System.out.println("Unexpected paradigm option value");
            cliHelpFormatter.printHelp("Usage:", cliOptions);
            return;
        }

        // Initialise the arrow root allocator
        RootAllocator arrowRootAllocator = new RootAllocator();

        // Take time when query planning starts
        queryPlanningStart = System.nanoTime();

        // Create the database instance
        ArrowDatabase database = new ArrowDatabase(databaseDirectoryPath, arrowRootAllocator);

        // Read the query
        String textualSqlQuery = Files.readString(queryFile.toPath());

        // Parse query into AST
        SqlNode parsedSqlQuery = database.parseQuery(textualSqlQuery);

        // Validate the parsed query
        RelNode validatedSqlQuery = database.validateQuery(parsedSqlQuery);

        // Plan the query
        RelNode logicalQueryPlan = database.planQuery(validatedSqlQuery);

        // Take time when the query planning has finished
        queryPlanningEnd = System.nanoTime();

        // Print debug information if required
        if (verboseOutputEnabled) {
            System.out.println("[Database schema]");
            database.printSchema();
            System.out.println();

            System.out.println("[Parsed query]");
            System.out.println(parsedSqlQuery.toString());
            System.out.println();

            System.out.println("[Validated query]");
            System.out.println(validatedSqlQuery.toString());
            System.out.println();

            System.out.println("[Optimised query]");
            var relWriter = new RelWriterImpl(new PrintWriter(System.out, true), SqlExplainLevel.NON_COST_ATTRIBUTES, false);
            logicalQueryPlan.explain(relWriter);
            System.out.println();
        }

        // Take time when the code generation starts (and query planning has finished)
        codeGenerationStart = System.nanoTime();

        // Create the contexts required for code generation
        CodeGenContext cCtx = new CodeGenContext(database, arrowRootAllocator);
        OptimisationContext oCtx = new OptimisationContext();

        // Generate code for the query which prints the result to the standard output
        // while summarising the result if necessary
        boolean shouldSummarise = cmdArguments.hasOption(summariseAsCount);
        QueryTranslator queryTranslator = new QueryTranslator();
        CodeGenOperator<?> queryRootOperator = queryTranslator.translate(logicalQueryPlan, false);
        if (shouldSummarise)
            queryRootOperator = new QueryResultCountOperator(logicalQueryPlan, queryRootOperator);
        CodeGenOperator<?> printOperator = new QueryResultPrinterOperator(queryRootOperator.getLogicalSubplan(), queryRootOperator);
        QueryCodeGenerator queryCodeGenerator = new QueryCodeGenerator(cCtx, oCtx, printOperator, useVectorisedProcessing);

        GeneratedQuery generatedQuery;
        try {
            generatedQuery = queryCodeGenerator.generateQuery(verboseOutputEnabled);
        } catch (Exception e) {
            throw new RuntimeException("Could not generate code for query", e);
        }

        // Execute the generated query
        System.out.println("[Query result]");
        queryExecutionStart = System.nanoTime();
        generatedQuery.execute();
        queryExecutionEnd = System.nanoTime();
        generatedQuery.getCCtx().close();
        // We do not perform maintenance on the allocation manager in the cCtx of the query as we only execute a single query

        // Output profiling information if required
        if (cmdArguments.hasOption(outputProfileInformation)) {
            double planningTimeMs = ((double) (queryPlanningEnd - queryPlanningStart)) / 1000000d;
            double codegenTimeMs = ((double) (codeGenerationEnd - codeGenerationStart)) / 1000000d;
            double compilationTimeMs = ((double) (codeCompilationEnd - codeCompilationStart)) / 1000000d;
            double queryExecutionTimeMs = ((double) (queryExecutionEnd - queryExecutionStart)) / 1000000d;
            System.err.println(
                    "{\"planning\": " + planningTimeMs
                            + ", \"codegen\": " + codegenTimeMs
                            + ", " + "\"compilation\": " + compilationTimeMs
                            + ", \"execution\": " + queryExecutionTimeMs
                            + "}");
        }
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

        // Define option for enabling verbose output
        verboseOutput = Option
                .builder("v")
                .longOpt("verbose")
                .hasArg(false)
                .required(false)
                .desc("Output verbose query processing information to the standard output")
                .build();
        options.addOption(verboseOutput);

        // Define option for selecting the query processing paradigm
        processingParadigm = Option
                .builder("p")
                .longOpt("paradigm")
                .hasArg(true)
                .required(true)
                .desc("Supply the query processing paradigm to execute: [non-vectorised, vectorised]")
                .build();
        options.addOption(processingParadigm);

        // Define option to summarise the query result as a count only
        summariseAsCount = Option
                .builder("s")
                .longOpt("summarise")
                .hasArg(false)
                .required(false)
                .desc("Summarise the result of a query as the number of rows in the result")
                .build();
        options.addOption(summariseAsCount);

        // Define option to output runtime profiling information
        outputProfileInformation = Option
                .builder("t")
                .longOpt("timing")
                .hasArg(false)
                .required(false)
                .desc("Time the execution of several parts of the query engine")
                .build();
        options.addOption(outputProfileInformation);

        return options;
    }

}
