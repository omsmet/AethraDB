package AethraDB;

import AethraDB.evaluation.codegen.GeneratedQuery;
import AethraDB.util.AethraGenerator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.UnsafeAllocationManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

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
    public static long totalTimeStart = -1;
    public static long totalTimeEnd = -1;

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

        // We define total time to start here (as it is most similar to duckdb)
        totalTimeStart = System.nanoTime();

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

        // Check whether the result should be summarised
        boolean shouldSummarise = cmdArguments.hasOption(summariseAsCount);

        // Initialise the arrow root allocator
        var arrowConfig = RootAllocator.configBuilder()
                .allocationManagerFactory(UnsafeAllocationManager.FACTORY)
                .build();
        RootAllocator arrowRootAllocator = new RootAllocator(arrowConfig);

        // Plan, generate and instantiate the query
        GeneratedQuery generatedQuery = AethraGenerator.planGenerateCompileQuery(
                arrowRootAllocator,
                databaseDirectoryPath,
                queryFile.getPath(),
                useVectorisedProcessing,
                shouldSummarise);

        // Execute the generated query
        queryExecutionStart = System.nanoTime();
        generatedQuery.execute();
        queryExecutionEnd = System.nanoTime();
        generatedQuery.getCCtx().close();
        // We do not perform maintenance on the allocation manager in the cCtx of the query as we only execute a single query

        // We define total time to end here (as it is most similar to DuckDB)
        totalTimeEnd = System.nanoTime();

        // Output profiling information if required
        if (cmdArguments.hasOption(outputProfileInformation)) {
            double planningTimeMs = ((double) (queryPlanningEnd - queryPlanningStart)) / 1_000_000d;
            double codegenTimeMs = ((double) (codeGenerationEnd - codeGenerationStart)) / 1_000_000d;
            double compilationTimeMs = ((double) (codeCompilationEnd - codeCompilationStart)) / 1_000_000d;
            double queryExecutionTimeMs = ((double) (queryExecutionEnd - queryExecutionStart)) / 1_000_000d;
            double totalTimeMs = ((double) (totalTimeEnd - totalTimeStart)) / 1_000_000d;
            System.err.println(
                    "{\"planning\": " + planningTimeMs
                            + ", \"codegen\": " + codegenTimeMs
                            + ", " + "\"compilation\": " + compilationTimeMs
                            + ", \"execution\": " + queryExecutionTimeMs
                            + ", \"total\": " + totalTimeMs
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
