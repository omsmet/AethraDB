import calcite.rules.ArrowTableScanProjectionRule;
import calcite.rules.ArrowTableScanRule;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.commons.cli.*;
import util.ArrowSchemaBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;

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

        // Obtain the database schema for the given database directory
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema databaseSchema = ArrowSchemaBuilder.fromDirectory(
                cmdArguments.getOptionValue(dbDirectoryPath),
                typeFactory);
        System.out.println("[Database schema]");
        printSchema(databaseSchema, typeFactory);
        System.out.println();

        // Read the query
        File queryFile = new File(cmdArguments.getOptionValue(queryFilePath));
        if (!queryFile.exists() || !queryFile.isFile())
            throw new IllegalStateException("The query file does not exist");
        String textualSqlQuery = Files.readString(queryFile.toPath());

        // Create the planner
        SqlParser.Config sqlParserConfig = SqlParser.config().withCaseSensitive(false);

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig)
                .defaultSchema(databaseSchema.plus())
                .build();

        Planner queryPlanner = Frameworks.getPlanner(frameworkConfig);

        // Parse query into AST
        SqlNode parsedSqlQuery = queryPlanner.parse(textualSqlQuery);
        System.out.println("[Parsed query]");
        System.out.println(parsedSqlQuery.toString());
        System.out.println();

        // Validate the parsed query
        SqlNode validatedSqlQuery = queryPlanner.validate(parsedSqlQuery);
        RelRoot queryRoot = queryPlanner.rel(validatedSqlQuery);
        RelNode queryNode = queryRoot.project();

        System.out.println("[Validated query]");
        System.out.println(validatedSqlQuery.toString());
        System.out.println();

        // Create the optimiser
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

//        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE);          // Pushes an aggregate past a Join
//        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED); // extension of above to push down aggregate functions
        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);        // Reduces aggregate functions in an aggregate to simpler forms

        hepProgramBuilder.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS);           // Reduces constants inside a logical calc
        hepProgramBuilder.addRuleInstance(CoreRules.CALC_REMOVE);                       // Removes trivial logical calc nodes

        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);        // Pushes a filter past an aggregate
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_MERGE);                      // Rule that combines two logical filters
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);         // Reduces constant expressions inside filters

//        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_ASSOCIATE);                    // Rule that changes a join based on the associativity rule
//        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_COMMUTE);                      // Rule that permutes the inputs to an inner join
        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);               // Rule that pushes predicates in a join into the inputs into the join
        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);   // Rule that infers predicates from a join and creates filters if those predicates can be pushed to its inputs
        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS);           // Rule that reduces constants inside a join

        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_AGGREGATE_MERGE);           // Projects away aggregate calls that are not used
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE);          // Push project past filter
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);            // Push project past join
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);        // Reduces constant expressions inside projections
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_REMOVE);                    // Removes projections that only return their input)

        // Rules to enable custom, arrow-specific optimisations/translations
        final ArrowTableScanProjectionRule PROJECT_SCAN = ArrowTableScanProjectionRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(PROJECT_SCAN);
        final ArrowTableScanRule ARROW_SCAN = ArrowTableScanRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN);

        // Interesting sort and union rules also exist

        HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build());

        // Plan the query
        hepPlanner.setRoot(queryNode);
        queryNode = hepPlanner.findBestExp();
        System.out.println("[Optimised query]");
        System.out.println(RelOptUtil.toString(queryNode));
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

    /**
     * Prints a given {@link CalciteSchema} to the standard output in a human-readable format.
     * @param schema The schema to print.
     * @param typeFactory The {@link RelDataTypeFactory} used by the {code schema}.
     */
    private static void printSchema(CalciteSchema schema, RelDataTypeFactory typeFactory) {
        System.out.println("+++++");
        for (String tableName : schema.getTableNames()) {
            System.out.println(tableName);

            Table table = Objects.requireNonNull(schema.getTable(tableName, false)).getTable();
            RelDataType rowType = table.getRowType(typeFactory);
            for (RelDataTypeField field : rowType.getFieldList()) {
                System.out.println(" - " + field.getName() + " : " + field.getType().toString());
            }

            System.out.println("+++++");
        }
    }

}
