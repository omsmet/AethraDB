import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.commons.cli.*;
import org.checkerframework.checker.units.qual.C;
import util.ArrowSchemaBuilder;
import util.ArrowTable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
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
    public static void main(String[] args) throws IOException {
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

        // Parse query into AST
        SqlParser sqlParser = SqlParser.create(textualSqlQuery);
        SqlNode parsedSqlQuery = null;
        try {
            parsedSqlQuery = sqlParser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        System.out.println("[Parsed query]");
        System.out.println(parsedSqlQuery.toString());
        System.out.println();

        // Create a catalog reader
        CalciteConnectionConfig catalogReaderConfig = CalciteConnectionConfig.DEFAULT
                .set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                databaseSchema,
                Collections.emptyList(),
                typeFactory,
                catalogReaderConfig);

        // Create the SQL validator using the standard operator table and default configuration
        SqlValidator sqlValidator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the parsed query
        SqlNode validatedSqlQuery = sqlValidator.validate(parsedSqlQuery);
        System.out.println("[Validated query]");
        System.out.println(validatedSqlQuery.toString());
        System.out.println();

        // Create the optimisation cluster to maintain planning information
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster planningCluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        // Convert the query into a logical plan
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                (type, query, schema, path) -> null, // NOOP_EXPANDER
                sqlValidator,
                catalogReader,
                planningCluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        RelNode logicalPlan = sqlToRelConverter.convertQuery(validatedSqlQuery, false, true).rel;
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]",
                        logicalPlan,
                        SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));
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
