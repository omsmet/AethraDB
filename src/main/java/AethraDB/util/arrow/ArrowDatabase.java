package AethraDB.util.arrow;

import AethraDB.calcite.rules.ArrowTableScanFilterProjectRule;
import AethraDB.calcite.rules.ArrowTableScanProjectionRule;
import AethraDB.calcite.rules.ArrowTableScanRule;
import AethraDB.util.calcite.ParseAndValidateOnlyPlannerImpl;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import java.io.Reader;
import java.util.Objects;

/**
 * Class used for representing a database over a directory of Arrow files by exposing methods for
 * parsing a query into a logical query plan.
 */
public class ArrowDatabase {

    /**
     * The {@link RelDataTypeFactory} to use for handling this database.
     */
    private final RelDataTypeFactory typeFactory;

    /**
     * The {@link CalciteSchema} of this database.
     */
    private final CalciteSchema databaseSchema;

    /**
     * The {@link Planner} of this database.
     */
    private final Planner queryPlanner;

    /**
     * The {@link HepPlanner} of this database.
     */
    private final HepPlanner hepPlanner;

    /**
     * Boolean indicating whether the maximum varchar length of the database has already been determined.
     */
    private boolean maxVarCharLengthValid;

    /**
     * Variable indicating the largest length of any varchar column in the database represented by {@code this}.
     */
    private int maxVarCharLength;

    /**
     * Constructs a new {@link ArrowDatabase} instance.
     * @param databaseDirectoryPath The directory from which the database should be loaded.
     */
    public ArrowDatabase(String databaseDirectoryPath) {
        // Initialise the schema
        this.typeFactory = new JavaTypeFactoryImpl();
        this.databaseSchema = ArrowSchemaBuilder.fromDirectory(databaseDirectoryPath, this.typeFactory);

        // Initialise the planner
        SqlParser.Config sqlParserConfig = SqlParser.config().withCaseSensitive(false);
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig)
                .defaultSchema(databaseSchema.plus())
                .build();
        this.queryPlanner = new ParseAndValidateOnlyPlannerImpl(frameworkConfig);

        // Configure the HEP optimiser
        this.hepPlanner = configureOptimiser();

        // Initialise utility variables
        this.maxVarCharLengthValid = false;
        this.maxVarCharLength = -1;
    }

    /**
     * Method which returns the maximum varchar column length in the database, or -1 if no varchar
     * column exists in the schema.
     * @return The maximum varchar column length in the database, or -1 if no varchar column exists.
     */
    public int getMaximumVarCharColumnLength() {
        if (this.maxVarCharLengthValid)
            return this.maxVarCharLength;

        // Determine the actual max varchar length
        int resultLength = -1;

        // TODO: Arrow varchar vectors to not encode their maximum length, deal with this properly
        // The returned value thus is optimised for the TPC-H experiment, whose maximum varchar
        // column has length 199
        resultLength = 200;

        this.maxVarCharLengthValid = true;
        this.maxVarCharLength = resultLength;
        return resultLength;
    }

    /**
     * Method for parsing a textual SQL query into an AST format.
     * @param query The query to parse.
     * @return The parsed query.
     * @throws SqlParseException when the query cannot be successfully parsed.
     */
    public SqlNode parseQuery(Reader query) throws SqlParseException {
        return this.queryPlanner.parse(query);
    }

    /**
     * Method for parsing and validating a textual SQL query.
     * @param query The query to parse and validate
     * @return The parsed and validated query's root.
     * @throws SqlParseException when the query cannot be successfully parsed.
     * @throws ValidationException when the query cannot be successfully validated.
     * @throws RelConversionException if the query cannot be converted into the appropriate return format.
     */
    public RelNode validateQuery(Reader query) throws SqlParseException, ValidationException, RelConversionException {
        SqlNode parsedQuery = this.parseQuery(query);
        return this.validateQuery(parsedQuery);
    }

    /**
     * Method for validating an SQL query provided as an AST.
     * @param query The query to validate.
     * @return A validated SQL query's root.
     * @throws ValidationException when the query cannot be successfully validated.
     * @throws RelConversionException if the query cannot be converted into the appropriate return format.
     */
    public RelNode validateQuery(SqlNode query) throws ValidationException, RelConversionException {
        SqlNode validatedQuery = this.queryPlanner.validate(query);
        RelRoot queryRoot = this.queryPlanner.rel(validatedQuery);
        return queryRoot.project();
    }

    /**
     * Method for planning a raw SQL query to obtain an optimised logical query plan.
     * @param query The query to plan.
     * @return The logical plan for the query.
     * @throws SqlParseException when the query cannot be successfully parsed.
     * @throws ValidationException when the query cannot be successfully validated.
     * @throws RelConversionException if the query cannot be converted into the appropriate return format.
     */
    public RelNode planQuery(Reader query) throws SqlParseException, ValidationException, RelConversionException {
        RelNode queryNode = this.validateQuery(query);
        return this.planQuery(queryNode);
    }

    /**
     * Method for planning a parsed SQL query to obtain an optimised logical query plan.
     * @param query The query to plan.
     * @return The logical plan for the query.
     * @throws ValidationException when the provided query cannot be successfully validated.
     * @throws RelConversionException if the query cannot be converted into the appropriate return format.
     */
    public RelNode planQuery(SqlNode query) throws ValidationException, RelConversionException {
        RelNode queryNode = this.validateQuery(query);
        return this.planQuery(queryNode);
    }

    /**
     * Method for planning a validated SQL query to obtain an optimised logical query plan.
     * @param query The query to plan.
     * @return The logical plan for the query.
     */
    public RelNode planQuery(RelNode query) {
        this.hepPlanner.setRoot(query);
        return this.hepPlanner.findBestExp();
    }

    /**
     * Method to print the schema of this database.
     */
    public void printSchema() {
        System.out.println("+++++");
        for (String tableName : this.databaseSchema.getTableNames()) {
            System.out.println(tableName);

            Table table = Objects.requireNonNull(this.databaseSchema.getTable(tableName, false)).getTable();
            RelDataType rowType = table.getRowType(typeFactory);
            for (RelDataTypeField field : rowType.getFieldList()) {
                System.out.println(" - " + field.getName() + " : " + field.getType().toString());
            }

            System.out.println("+++++");
        }
    }

    /**
     * Method for configuring and instantiating a HEP planner with appropriate optimisation rules.
     * @return An {@link HepPlanner} instance that can be used for database optimisation.
     */
    private HepPlanner configureOptimiser() {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

        // CoreRules have been translated to their actual value to prevent instantiation of the complete class

        // Required for AVG aggregation
        // hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);        // Reduces aggregate functions in an aggregate to simpler forms
        hepProgramBuilder.addRuleInstance(AggregateReduceFunctionsRule.Config.DEFAULT.toRule());

        // Required to rewrite WHERE conditions into an INNER JOIN
        // hepProgramBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);                  // Pushes filter expressions into a join condition and into the inputs of the join
        hepProgramBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule());

        // Required to ensure online necessary columns are processed by the JOIN operator
        // hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE);            // Push project past join
        hepProgramBuilder.addRuleInstance(ProjectJoinTransposeRule.Config.DEFAULT.toRule());

        // Required to optimise projections
        // hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_REMOVE);                    // Removes projections that only return their input
        hepProgramBuilder.addRuleInstance(ProjectRemoveRule.Config.DEFAULT.toRule());

        // Rules to enable custom, arrow-specific optimisations/translations
        final ArrowTableScanProjectionRule PROJECT_SCAN = ArrowTableScanProjectionRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(PROJECT_SCAN);
        final ArrowTableScanRule ARROW_SCAN = ArrowTableScanRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN);
        final ArrowTableScanFilterProjectRule ARROW_SCAN_FILTER_PROJECT = ArrowTableScanFilterProjectRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN_FILTER_PROJECT);

        return new HepPlanner(hepProgramBuilder.build());
    }

}
