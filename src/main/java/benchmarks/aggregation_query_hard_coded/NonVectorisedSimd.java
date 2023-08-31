package benchmarks.aggregation_query_hard_coded;

import benchmarks.aggregation_query.ResultVerifier;
import benchmarks.aggregation_query_hard_coded.NonVectorisedSimdSupport.KeyValueMap_1306429814;
import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.general_support.hashmaps.Int_Hash_Function;
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
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using the code generated
 * by its non-vectorised query code generation with SIMD-ed operators, but while not actually
 * invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class NonVectorisedSimd {

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
    private KeyValueMap_1306429814 aggregation_state_map;

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
        this.aggregation_query_table = new ABQArrowTableReader(new File(this.tableFilePath + "/aggregation_query_table.arrow"), this.rootAllocator);

        // Compute the hash-table sizes as the correct power of two size
        Pattern keysPattern = Pattern.compile("keys\\_\\d+");
        Matcher keysMatcher = keysPattern.matcher(this.tableFilePath);
        keysMatcher.find();
        int numberKeys = Integer.parseInt(keysMatcher.group(0).split("_")[1]);
        this.hashTable_size = Integer.highestOneBit(numberKeys) << 1;

        // Initialise the hash-table
        this.aggregation_state_map = new KeyValueMap_1306429814(this.hashTable_size);

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
            "--enable-native-access=ALL-UNNAMED",
            "-Xmx16g",
            "-Xms8g"
    })
    public void executeQuery() throws IOException {
        // KeyValueMap_1306429814 aggregation_state_map = new KeyValueMap_1306429814();                             // DIFF: hard-coded
        int commonSIMDVectorLength = 4;
        jdk.incubator.vector.VectorSpecies<Integer> IntVectorSpecies = OptimisationContext.getVectorSpeciesInt();   // DIFF: call on class instead of object
        // ArrowTableReader aggregation_query_table = cCtx.getArrowReader(0);                                       // DIFF: hard-coded
        while (aggregation_query_table.loadNextBatch()) {
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_0 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(0));
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_1 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(1));
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_2 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(2));
            org.apache.arrow.vector.IntVector aggregation_query_table_vc_3 = ((org.apache.arrow.vector.IntVector) aggregation_query_table.getVector(3));
            int arrowVectorLength = aggregation_query_table_vc_0.getValueCount();
            // DIFF: call on class instead of object
            MemorySegment col_0_ms = OptimisationContext.createMemorySegmentForAddress(aggregation_query_table_vc_0.getDataBufferAddress(), arrowVectorLength * aggregation_query_table_vc_0.TYPE_WIDTH);
            // DIFF: call on class instead of object
            MemorySegment col_1_ms = OptimisationContext.createMemorySegmentForAddress(aggregation_query_table_vc_1.getDataBufferAddress(), arrowVectorLength * aggregation_query_table_vc_1.TYPE_WIDTH);
            // DIFF: call on class instead of object
            MemorySegment col_2_ms = OptimisationContext.createMemorySegmentForAddress(aggregation_query_table_vc_2.getDataBufferAddress(), arrowVectorLength * aggregation_query_table_vc_2.TYPE_WIDTH);
            // DIFF: call on class instead of object
            MemorySegment col_3_ms = OptimisationContext.createMemorySegmentForAddress(aggregation_query_table_vc_3.getDataBufferAddress(), arrowVectorLength * aggregation_query_table_vc_3.TYPE_WIDTH);
            for (int currentVectorOffset = 0; currentVectorOffset < arrowVectorLength; currentVectorOffset += commonSIMDVectorLength) {
                jdk.incubator.vector.VectorMask<Integer> inRangeSIMDMask = IntVectorSpecies.indexInRange(currentVectorOffset, arrowVectorLength);
                // DIFF: call on class instead of object
                jdk.incubator.vector.IntVector SIMD_Key_Vector_Int = OptimisationContext.createIntVector(IntVectorSpecies, col_0_ms, currentVectorOffset * aggregation_query_table_vc_0.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                // DIFF: call on class instead of object
                jdk.incubator.vector.LongVector SIMD_Key_Vector_Long = ((jdk.incubator.vector.LongVector) SIMD_Key_Vector_Int.castShape(OptimisationContext.getVectorSpeciesLong(), 0));
                jdk.incubator.vector.LongVector SIMD_a_mul_key_vector = SIMD_Key_Vector_Long.mul(Int_Hash_Function.hashConstantA);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_plus_b_vector = SIMD_a_mul_key_vector.add(Int_Hash_Function.hashConstantB);
                long[] pre_hash_values = SIMD_a_mul_key_plus_b_vector.toLongArray();
                for (int simd_vector_i = 0; simd_vector_i < commonSIMDVectorLength; simd_vector_i++) {
                    if (!(inRangeSIMDMask.laneIsSet(simd_vector_i))) {
                        continue;
                    }
                    int flattened_key = aggregation_query_table_vc_0.get(currentVectorOffset + simd_vector_i);
                    long pre_hash_value = (pre_hash_values[simd_vector_i] % Int_Hash_Function.hashConstantP);
                    aggregation_state_map.incrementForKey(flattened_key, pre_hash_value, aggregation_query_table_vc_1.get(currentVectorOffset + simd_vector_i), aggregation_query_table_vc_2.get(currentVectorOffset + simd_vector_i), aggregation_query_table_vc_3.get(currentVectorOffset + simd_vector_i));
                }
            }
        }
        for (int key_i = 0; key_i < aggregation_state_map.numberOfRecords; key_i++) {
            int groupKey = aggregation_state_map.keys[key_i];
            long aggregation_0_value = aggregation_state_map.values_ord_0[key_i];
            long aggregation_1_value = aggregation_state_map.values_ord_1[key_i];
            long aggregation_2_value = aggregation_state_map.values_ord_2[key_i];
            // System.out.print(groupKey + ", ");                       // DIFF: removed
            // System.out.print(aggregation_0_value + ", ");            // DIFF: removed
            // System.out.print(aggregation_1_value + ", ");            // DIFF: removed
            // System.out.println(aggregation_2_value);                 // DIFF: removed
            this.keyResult[key_i] = groupKey;                           // DIFF: added
            this.col2SumResult[key_i] = aggregation_0_value;            // DIFF: updated
            this.col3SumResult[key_i] = aggregation_1_value;            // DIFF: updated
            this.col4SumResult[key_i] = aggregation_2_value;            // DIFF: updated
        }

    }

}
