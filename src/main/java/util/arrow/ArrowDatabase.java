package util.arrow;

import calcite.rules.ArrowTableScanFilterProjectRule;
import calcite.rules.ArrowTableScanProjectionRule;
import calcite.rules.ArrowTableScanRule;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
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
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

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
        this.queryPlanner = Frameworks.getPlanner(frameworkConfig);

        // Configure the HEP optimiser
        this.hepPlanner = configureOptimiser();
    }

    /**
     * Method for parsing a textual SQL query into an AST format.
     * @param query The query to parse.
     * @return The parsed query.
     * @throws SqlParseException when the query cannot be successfully parsed.
     */
    public SqlNode parseQuery(String query) throws SqlParseException {
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
    public RelNode validateQuery(String query) throws SqlParseException, ValidationException, RelConversionException {
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
    public RelNode planQuery(String query) throws SqlParseException, ValidationException, RelConversionException {
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
        RelNode plannedQuery = this.hepPlanner.findBestExp();

        // Finally, configure the VolcanoPlanner of the query to give approximate costs for the operators
        RelOptCluster queryCluster = plannedQuery.getCluster();
        VolcanoPlanner queryVolcanoPlanner = (VolcanoPlanner) queryCluster.getPlanner();
        queryVolcanoPlanner.setNoneConventionHasInfiniteCost(false);

        return plannedQuery;
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

//        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE);          // Pushes an aggregate past a Join
//        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED); // extension of above to push down aggregate functions
        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);        // Reduces aggregate functions in an aggregate to simpler forms

        hepProgramBuilder.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS);           // Reduces constants inside a logical calc
        hepProgramBuilder.addRuleInstance(CoreRules.CALC_REMOVE);                       // Removes trivial logical calc nodes

        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);        // Pushes a filter past an aggregate
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);                  // Pushes filter expressions into a join condition and into the inputs of the join
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
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_REMOVE);                    // Removes projections that only return their input

        // Rules to enable custom, arrow-specific optimisations/translations
        final ArrowTableScanProjectionRule PROJECT_SCAN = ArrowTableScanProjectionRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(PROJECT_SCAN);
        final ArrowTableScanRule ARROW_SCAN = ArrowTableScanRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN);
        final ArrowTableScanFilterProjectRule ARROW_SCAN_FILTER_PROJECT = ArrowTableScanFilterProjectRule.Config.DEFAULT.toRule();
        hepProgramBuilder.addRuleInstance(ARROW_SCAN_FILTER_PROJECT);

        // Interesting sort and union rules also exist

        return new HepPlanner(hepProgramBuilder.build());
    }

}
