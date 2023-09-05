package benchmarks.join_query;

import benchmarks.util.ResultConsumptionOperator;
import benchmarks.util.ResultConsumptionTarget;
import evaluation.codegen.GeneratedQuery;
import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.QueryTranslator;
import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.operators.CodeGenOperator;
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
import util.arrow.ArrowDatabase;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using its non-vectorised
 * query code generation without SIMD-ed operators.
 */
@State(Scope.Benchmark)
public class NonVectorisedNonSimd extends ResultConsumptionTarget {

    /**
     * We want to test the query processing performance for different table instances, where the
     * selectivity of the join condition between different columns varies.
     */
    @Param({
            // SF-1
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.6",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.8",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.6",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.8",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.6",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.8",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.6",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.8"

            // SF-10
            "/nvtmp/AethraTestData/join_query_int_sf10/A_B_0.6_C_0.8",

            // SF-20
//            "/nvtmp/AethraTestData/join_query_int_sf20/A_B_0.6_C_0.8",
    })
    private String tableFilePath;

    /**
     * The query to execute using AethraDB.
     */
    private static final String query =
            """
            SELECT COUNT(*)
            FROM table_A A
            INNER JOIN table_B B ON A.col1 = B.col1
            INNER JOIN table_C C on A.col2 = C.col1
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
        // Setup the database
        this.database = new ArrowDatabase(this.tableFilePath);

        // Plan the query
        RelNode plannedQuery = this.database.planQuery(query);

        // Generate code for the query
        QueryTranslator queryTranslator = new QueryTranslator();
        CodeGenOperator<?> queryRootOperator = queryTranslator.translate(plannedQuery, false);
        CodeGenOperator<RelNode> queryResultConsumptionOperator = new ResultConsumptionOperator(plannedQuery, queryRootOperator);
        QueryCodeGenerator queryCodeGenerator = new QueryCodeGenerator(queryResultConsumptionOperator, false);
        this.generatedQuery = queryCodeGenerator.generateQuery(true);
        this.generatedQueryCCtx = this.generatedQuery.getCCtx();
        this.generatedQueryCCtx.setResultConsumptionTarget(this);

        // Initialise the result
        this.result = -1;

        // Initialise the expected result
        Scanner fileScanner = new Scanner(new File(this.tableFilePath, "expected_count.csv"));
        this.expectedResult = fileScanner.nextInt();
        fileScanner.close();
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
        if (result != expectedResult)
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
    public void executeQuery() throws IOException {
        this.generatedQuery.execute();
    }

}
