package benchmarks.aggregation_query_hard_coded;

import benchmarks.aggregation_query.ResultVerifier;
import benchmarks.aggregation_query_hard_coded.VectorisedSupport.KeyValueMap_2096808863;
import evaluation.codegen.infrastructure.data.AllocationManager;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.BufferPoolAllocationManager;
import evaluation.codegen.infrastructure.data.CachingArrowTableReader;
import evaluation.vector_support.VectorisedAggregationOperators;
import evaluation.vector_support.VectorisedHashOperators;
import org.apache.arrow.memory.RootAllocator;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using the code generated
 * by its vectorised query code generation without SIMD-ed operators, but while not actually
 * invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class VectorisedNonSimd {

    /**
     * We want to test the query processing performance for different table instances, where different
     * instances have different number of aggregation groups and/or key skew.
     */
    @Param({
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_1",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_2",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_4",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_8",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_16",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_32",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_64",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_128",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_256",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_512",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_1024",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_2048",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_4096",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_8192",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_16384",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_32768",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_65536",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_131072",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_262144",
            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_31457280_keys_524288",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.2",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.4",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.6",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_0.8",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.0",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.2",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.4",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.6",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_1.8",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_2.0",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_2.2",
//            "/nvtmp/AethraTestData/aggregation_query_int/arrow_size_62914560_keys_262144_skew_2.4"
    })
    private String tableFilePath;

    /**
     * State: the {@link RootAllocator} used for reading Arrow files.
     */
    private RootAllocator rootAllocator;

    /**
     * State: the {@link AllocationManager} used for executing the query.
     */
    private AllocationManager allocationManager;

    /**
     * State: the {@link ArrowTableReader} used for reading from the table.
     */
    private ArrowTableReader aggregation_query_table;

    /**
     * State: the size at which the hash-tables should be initialised.
     */
    private int hashTable_size;

    /**
     * State: the hash-table which is used by the query for the aggregation.
     */
    private KeyValueMap_2096808863 aggregation_state_map;

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
     * State: the {@link ResultVerifier} that will be used to check the result of the query.
     */
    private ResultVerifier resultVerifier;

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        this.aggregation_query_table = new CachingArrowTableReader(new File(this.tableFilePath + "/aggregation_query_table.arrow"), this.rootAllocator);

        // Compute the hash-table sizes as the correct power of two size
        Pattern keysPattern = Pattern.compile("keys\\_\\d+");
        Matcher keysMatcher = keysPattern.matcher(this.tableFilePath);
        keysMatcher.find();
        int numberKeys = Integer.parseInt(keysMatcher.group(0).split("_")[1]);
        this.hashTable_size = Integer.highestOneBit(numberKeys) << 1;

        // Initialise the hash-table
        this.aggregation_state_map = new KeyValueMap_2096808863(this.hashTable_size);

        // Setup the allocation manager
        this.allocationManager = new BufferPoolAllocationManager(8);

        // Initialise the result
        this.keyResult = new int[numberKeys];
        Arrays.fill(this.keyResult, -1);
        this.col2SumResult = new long[numberKeys];
        Arrays.fill(this.col2SumResult, -1);
        this.col3SumResult = new long[numberKeys];
        Arrays.fill(this.col3SumResult, -1);
        this.col4SumResult = new long[numberKeys];
        Arrays.fill(this.col4SumResult, -1);

        // And the result verifier
        this.resultVerifier = new ResultVerifier(this.tableFilePath);
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationSetup() throws Exception {
        // Refresh the arrow reader of the query for the next iteration
        this.aggregation_query_table.reset();

        // Reset the aggregation map
        this.aggregation_state_map.reset();

        // Perform allocation manager maintenance
        this.allocationManager.performMaintenance();
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
        Arrays.fill(this.keyResult, -1);
        Arrays.fill(this.col2SumResult, -1);
        Arrays.fill(this.col3SumResult, -1);
        Arrays.fill(this.col4SumResult, -1);
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
            "-Xmx16g",
            "-Xms8g"
    })
    public void executeQuery() throws IOException {
        long[] groupKeyPreHashVector = this.allocationManager.getLongVector();           // DIFF: hard-coded
        // KeyValueMap_2096808863 aggregation_state_map = new KeyValueMap_2096808863();     DIFF: hard-coded
        // ArrowTableReader aggregation_query_table = cCtx.getArrowReader(0);               DIFF: hard-coded
        while (aggregation_query_table.loadNextBatch()) {
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_0 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(0));
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_1 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(1));
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_2 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(2));
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_3 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(3));
            VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, aggregation_query_table_vc_0, false);
            int recordCount = aggregation_query_table_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                aggregation_state_map.incrementForKey(aggregation_query_table_vc_0.get(aviv), groupKeyPreHashVector[aviv], aggregation_query_table_vc_1.get(aviv), aggregation_query_table_vc_2.get(aviv), aggregation_query_table_vc_3.get(aviv));
            }
        }
        int aggregationResultVectorLength;
        int[] groupKeyVector = this.allocationManager.getIntVector();       // DIFF: hard-coded
        long[] agg_G_SUM_0_vector = this.allocationManager.getLongVector(); // DIFF: hard-coded
        long[] agg_G_SUM_1_vector = this.allocationManager.getLongVector(); // DIFF: hard-coded
        long[] agg_G_SUM_2_vector = this.allocationManager.getLongVector(); // DIFF: hard-coded
        int current_key_offset = 0;
        int number_of_records = aggregation_state_map.numberOfRecords;
        while (current_key_offset < number_of_records) {
            aggregationResultVectorLength = VectorisedAggregationOperators.constructVector(groupKeyVector, aggregation_state_map.keys, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_0_vector, aggregation_state_map.values_ord_0, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_1_vector, aggregation_state_map.values_ord_1, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_2_vector, aggregation_state_map.values_ord_2, number_of_records, current_key_offset);
            System.arraycopy(groupKeyVector, 0, this.keyResult, current_key_offset, aggregationResultVectorLength);         // DIFF: added
            System.arraycopy(agg_G_SUM_0_vector, 0, this.col2SumResult, current_key_offset, aggregationResultVectorLength); // DIFF: added
            System.arraycopy(agg_G_SUM_1_vector, 0, this.col3SumResult, current_key_offset, aggregationResultVectorLength); // DIFF: added
            System.arraycopy(agg_G_SUM_2_vector, 0, this.col4SumResult, current_key_offset, aggregationResultVectorLength); // DIFF: added
            current_key_offset += aggregationResultVectorLength;
            // VectorisedPrintOperators.print(groupKeyVector, aggregationResultVectorLength);       // DIFF: removed
            // VectorisedPrintOperators.print(agg_G_SUM_0_vector, aggregationResultVectorLength);   // DIFF: removed
            // VectorisedPrintOperators.print(agg_G_SUM_1_vector, aggregationResultVectorLength);   // DIFF: removed
            // VectorisedPrintOperators.print(agg_G_SUM_2_vector, aggregationResultVectorLength);   // DIFF: removed
            // System.out.println();
        }
        this.allocationManager.release(groupKeyVector);         // DIFF: hard-coded
        this.allocationManager.release(agg_G_SUM_0_vector);     // DIFF: hard-coded
        this.allocationManager.release(agg_G_SUM_1_vector);     // DIFF: hard-coded
        this.allocationManager.release(agg_G_SUM_2_vector);     // DIFF: hard-coded
        this.allocationManager.release(groupKeyPreHashVector);  // DIFF: hard-coded
    }

}
