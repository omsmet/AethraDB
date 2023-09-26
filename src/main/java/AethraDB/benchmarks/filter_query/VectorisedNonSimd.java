package AethraDB.benchmarks.filter_query;

import AethraDB.benchmarks.util.ResultConsumptionOperator;
import AethraDB.benchmarks.util.ResultConsumptionTarget;
import AethraDB.evaluation.codegen.GeneratedQuery;
import AethraDB.evaluation.codegen.QueryCodeGenerator;
import AethraDB.evaluation.codegen.QueryTranslator;
import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import AethraDB.util.arrow.ArrowDatabase;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.RelNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using its vectorised
 * query code generation without SIMD-ed operators.
 */
@State(Scope.Benchmark)
public class VectorisedNonSimd extends ResultConsumptionTarget {

    /**
     * We want to test the query processing performance for different table instances, where respectively
     * the first, second or third column is the highly selective column. We do so using the different database instances.
     */
    @Param({
            // SF-1
             "/nvtmp/AethraTestData/filter_query_int/arrow_col1_002_col2_098_col3_098/",
             "/nvtmp/AethraTestData/filter_query_int/arrow_col1_098_col2_002_col3_098/",
             "/nvtmp/AethraTestData/filter_query_int/arrow_col1_098_col2_098_col3_002/",

            // SF-10
            "/nvtmp/AethraTestData/filter_query_int_sf10/arrow_col1_002_col2_098_col3_098/",
            "/nvtmp/AethraTestData/filter_query_int_sf10/arrow_col1_098_col2_002_col3_098/",
            "/nvtmp/AethraTestData/filter_query_int_sf10/arrow_col1_098_col2_098_col3_002/",

            // SF-20
            "/nvtmp/AethraTestData/filter_query_int_sf20/arrow_col1_002_col2_098_col3_098/",
            "/nvtmp/AethraTestData/filter_query_int_sf20/arrow_col1_098_col2_002_col3_098/",
            "/nvtmp/AethraTestData/filter_query_int_sf20/arrow_col1_098_col2_098_col3_002/",
    })
    private String tableFilePath;

    /**
     * The query to execute using AethraDB.
     */
    private static final String query =
            """
            SELECT COUNT(*)
            FROM filter_query_table T
            WHERE T.col1 < 3000
            AND T.col2 < 3000
            AND T.col3 < 3000
            """;

    /**
     * State: the database on which to execute the query.
     */
    private ArrowDatabase database;

    /**
     * State: the generated query to execute/benchmark.
     */
    private GeneratedQuery generatedQuery;

    /**
     * State the code generation context used for generating the query code.
     */
    private CodeGenContext generatedQueryCCtx;

    /**
     * State: the result of the query (-1 if the query has not been executed yet).
     */
    private long result;

    /**
     * State: the expected result of the query.
     */
    private long expectedResult;

    /**
     * Method to consume the query result.
     * The result is a single long that should simply be written to the result field of {@code this}.
     * @param value The value to be consumed.
     */
    @Override
    public void consumeResultItem(long value) {
        this.result = value;
    }

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the arrow root allocator
        RootAllocator arrowRootAllocator = new RootAllocator();

        // Setup the database
        this.database = new ArrowDatabase(this.tableFilePath, arrowRootAllocator);

        // Plan the query
        RelNode plannedQuery = this.database.planQuery(query);

        // Create the contexts required for code generation
        CodeGenContext cCtx = new CodeGenContext(database, arrowRootAllocator);
        OptimisationContext oCtx = new OptimisationContext();

        // Generate code for the query
        QueryTranslator queryTranslator = new QueryTranslator();
        CodeGenOperator<?> queryRootOperator = queryTranslator.translate(plannedQuery, false);
        CodeGenOperator<RelNode> queryResultConsumptionOperator = new ResultConsumptionOperator(plannedQuery, queryRootOperator);
        QueryCodeGenerator queryCodeGenerator = new QueryCodeGenerator(cCtx, oCtx, queryResultConsumptionOperator, true);
        this.generatedQuery = queryCodeGenerator.generateQuery(true);
        this.generatedQueryCCtx = this.generatedQuery.getCCtx();
        this.generatedQueryCCtx.setResultConsumptionTarget(this);

        // Initialise the result
        this.result = -1;

        // Initialise the expected result
        if (this.tableFilePath.contains("sf20/"))
            this.expectedResult = 12084336;
        else if (this.tableFilePath.contains("sf10/"))
            this.expectedResult = 6042864;
        else // if (this.tableFilePath.contains("sf1/"))
            this.expectedResult = 603769;
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationSetup() throws Exception {
        // Refresh the arrow reader of the query for the next iteration
        for (ArrowTableReader atr : this.generatedQueryCCtx.getArrowReaders())
            atr.reset();
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void teardown() {
        if (result != this.expectedResult)
            throw new RuntimeException("The computed result is incorrect");
        result = -1; // reset the result after verifying it

        // Have the allocation manager perform the required maintenance
        generatedQueryCCtx.getAllocationManager().performMaintenance();
    }

    /**
     * This method performs the actual execution of the query for benchmarking.
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(jvmArgsAppend = {
            "--add-modules=jdk.incubator.vector",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "-Darrow.enable_unsafe_memory_access=true",
            "-Darrow.enable_null_check_for_get=false",
            "--enable-preview",
            "-Xmx32g",
            "-Xms16g"
    })
    public void executeFilterQuery() throws IOException {
        this.generatedQuery.execute();
    }

}
