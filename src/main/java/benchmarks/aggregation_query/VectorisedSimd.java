package benchmarks.aggregation_query;

import benchmarks.util.ResultConsumptionOperator;
import benchmarks.util.ResultConsumptionTarget;
import evaluation.codegen.GeneratedQuery;
import evaluation.codegen.QueryCodeGenerator;
import evaluation.codegen.infrastructure.context.CodeGenContext;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.operators.CodeGenOperator;
import evaluation.codegen.translation.QueryTranslator;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using its vectorised
 * query code generation with SIMD-ed operators.
 */
@State(Scope.Benchmark)
public class VectorisedSimd extends ResultConsumptionTarget {

    /**
     * We want to test the query processing performance for different table instances, where different
     * instances have different number of aggregation groups and/or key skew.
     */
    @Param({
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_1",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_2",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_4",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_8",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_16",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_32",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_64",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_128",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_256",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_512",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_1024",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_2048",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_4096",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_8192",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_16384",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_32768",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_65536",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_131072",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_262144",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_524288",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.2",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.4",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.6",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.8",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.0",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.2",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.4",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.6",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.8",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_2.0",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_2.2",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_2.4"
    })
    private String tableFilePath;

    /**
     * The query to execute using AethraDB.
     */
    private static final String query =
            """
            SELECT col1, SUM(col2), SUM(col3), SUM(col4)
            FROM aggregation_query_table
            GROUP BY col1
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
     * State: the code generation context used for generating the query code.
     */
    private CodeGenContext generatedQueryCCtx;

    /**
     * State: the partial result of the query (null if the query has not been executed yet).
     */
    private int[] keyResult;

    /**
     * State: the partial result of the query (null if the query has not been executed yet).
     */
    private long[] col2SumResult;

    /**
     * State: the partial result of the query (null if the query has not been executed yet).
     */
    private long[] col3SumResult;

    /**
     * State: the partial result of the query (null if the query has not been executed yet).
     */
    private long[] col4SumResult;

    /**
     * State: counter which keeps track of the amount of times that {@code consumeResultItem} has
     * been called during this query so that the value is written to the correct result array.
     */
    private int consumeResultItemCounter;

    /**
     * State: the {@link ResultVerifier} that will be used to check the result of the query.
     */
    private ResultVerifier resultVerifier;

    /**
     * Method to consume the (partial) query result and store it in the appropriate result array.
     * @param value The value to be consumed.
     */
    @Override
    public void consumeResultItem(int[] value) {
        if (consumeResultItemCounter == 0)
            this.keyResult = value;
        consumeResultItemCounter++;
    }

    /**
     * Method to consume the (partial) query result and store it in the appropriate result array.
     * @param value The value to be consumed.
     */
    @Override
    public void consumeResultItem(long[] value) {
        if (consumeResultItemCounter == 1)
            this.col2SumResult = value;
        else if (consumeResultItemCounter == 2)
            this.col3SumResult = value;
        else if (consumeResultItemCounter == 3)
            this.col4SumResult = value;
        consumeResultItemCounter++;
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

        // Generate code operator tree for the query
        QueryTranslator queryTranslator = new QueryTranslator();
        CodeGenOperator<?> queryRootOperator = queryTranslator.translate(plannedQuery, true);

        // Extract the expected result size to construct the int[] packaging operator
        Pattern keysPattern = Pattern.compile("keys\\_\\d+");
        Matcher keysMatcher = keysPattern.matcher(this.tableFilePath);
        keysMatcher.find();
        int numberKeys = Integer.parseInt(keysMatcher.group(0).split("_")[1]);
        CodeGenOperator<?> packageOperator = new ResultPackageOperator(plannedQuery, queryRootOperator, numberKeys * 4);

        // Construct the packaging and result consumption operators and generate the code
        CodeGenOperator<RelNode> queryResultConsumptionOperator = new ResultConsumptionOperator(plannedQuery, packageOperator);
        QueryCodeGenerator queryCodeGenerator = new QueryCodeGenerator(queryResultConsumptionOperator, true);
        this.generatedQuery = queryCodeGenerator.generateQuery(true);
        this.generatedQueryCCtx = this.generatedQuery.getCCtx();
        this.generatedQueryCCtx.setResultConsumptionTarget(this);

        // Initialise the result
        this.keyResult = null;
        this.col2SumResult = null;
        this.col3SumResult = null;
        this.col4SumResult = null;
        this.consumeResultItemCounter = 0;

        // And the result verifier
        this.resultVerifier = new ResultVerifier(this.tableFilePath);
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
        // Verify the result
        if (!this.resultVerifier.resultCorrect(this.keyResult, this.col2SumResult, this.col3SumResult, this.col4SumResult))
            throw new RuntimeException("The computed result is incorrect");

        // Reset the result after verifying it
        this.keyResult = null;
        this.col2SumResult = null;
        this.col3SumResult = null;
        this.col4SumResult = null;
        this.consumeResultItemCounter = 0;

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
            "--enable-native-access=ALL-UNNAMED"
    })
    public void executeQuery() throws IOException {
        this.generatedQuery.execute();
    }

}
