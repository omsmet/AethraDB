package benchmarks.join_query_hard_coded;

import benchmarks.join_query_hard_coded.VectorisedGenSupport.KeyMultiRecordMap_1214032527;
import benchmarks.join_query_hard_coded.VectorisedGenSupport.KeyMultiRecordMap_378227888;
import evaluation.codegen.infrastructure.data.AllocationManager;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.BufferPoolAllocationManager;
import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.vector_support.VectorisedHashOperators;
import evaluation.vector_support.VectorisedOperators;
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
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using the code generated
 * by its vectorised query code generation with SIMD-ed operators, but while not actually
 * invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class VectorisedSimd {

    /**
     * We want to test the query processing performance for different table instances, where the
     * selectivity of the join condition between different columns varies.
     */
    @Param({
            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.6",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.2_C_0.8",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.6",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.4_C_0.8",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.6",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.6_C_0.8",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.2",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.4",
//            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.6",
            "/nvtmp/AethraTestData/join_query_int/A_B_0.8_C_0.8"
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
     * State: the {@link ArrowTableReader} used for reading from the first table.
     */
    private ArrowTableReader table_A;

    /**
     * State: the {@link ArrowTableReader} used for reading from the second table.
     */
    private ArrowTableReader table_B;

    /**
     * State: the {@link ArrowTableReader} used for reading from the third table.
     */
    private ArrowTableReader table_C;

    /**
     * State: the result of the query (-1 if the query has not been executed yet).
     */
    private long result;

    /**
     * State: the expected result of the query.
     */
    private long expectedResult;

    /**
     * State: the table_C join map.
     * DIFF: usually part of the query execution itself.
     */
    private KeyMultiRecordMap_378227888 join_map;

    /**
     * State: the table_A join map.
     * DIFF: usually part of the query execution itself.
     */
    private KeyMultiRecordMap_1214032527 join_map_0;

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        this.table_A = new ABQArrowTableReader(new File(this.tableFilePath + "/table_A.arrow"), this.rootAllocator);
        this.table_B = new ABQArrowTableReader(new File(this.tableFilePath + "/table_B.arrow"), this.rootAllocator);
        this.table_C = new ABQArrowTableReader(new File(this.tableFilePath + "/table_C.arrow"), this.rootAllocator);

        // Compute the hash-table sizes as the correct power of two size
        int hashTableSize = Integer.highestOneBit(3 * 1024 * 1024) << 2;

        // Allocate the hash-tables
        this.join_map = new KeyMultiRecordMap_378227888(hashTableSize);
        this.join_map_0 = new KeyMultiRecordMap_1214032527(hashTableSize);

        // Setup the allocation manager
        this.allocationManager = new BufferPoolAllocationManager(16);

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
        this.table_A.reset();
        this.table_B.reset();
        this.table_C.reset();
        // Reset the join maps
        this.join_map.reset();
        this.join_map_0.reset();
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void teardown() {
        if (result != expectedResult)
            throw new RuntimeException("The computed result is incorrect");
        result = -1; // reset the result after verifying it

        // let the allocation manager perform maintenance
        this.allocationManager.performMaintenance();
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
        long count = 0;
        long[] pre_hash_vector = this.allocationManager.getLongVector();                            // DIFF: hard-coded
        // KeyMultiRecordMap_378227888 join_map = new KeyMultiRecordMap_378227888();                // DIFF: hard-coded
        // ArrowTableReader table_C = cCtx.getArrowReader(0);                                       // DIFF: hard-coded
        while (table_C.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_C_vc_0 = ((org.apache.arrow.vector.IntVector) table_C.getVector(0));
            org.apache.arrow.vector.IntVector table_C_vc_1 = ((org.apache.arrow.vector.IntVector) table_C.getVector(1));
            org.apache.arrow.vector.IntVector table_C_vc_2 = ((org.apache.arrow.vector.IntVector) table_C.getVector(2));
            VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector, table_C_vc_0, false);
            int recordCount = table_C_vc_0.getValueCount();
            for (int i = 0; i < recordCount; i++) {
                int left_join_record_key = table_C_vc_0.get(i);
                join_map.associate(left_join_record_key, pre_hash_vector[i], left_join_record_key, table_C_vc_1.get(i), table_C_vc_2.get(i));
            }
        }
        int[] join_result_vector_ord_0 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_1 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_2 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_3 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_4 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_5 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_6 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_7 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        int[] join_result_vector_ord_8 = this.allocationManager.getIntVector();                     // DIFF: hard-coded
        long[] pre_hash_vector_0 = this.allocationManager.getLongVector();                          // DIFF: hard-coded
        // KeyMultiRecordMap_1214032527 join_map_0 = new KeyMultiRecordMap_1214032527();            // DIFF: hard-coded
        // ArrowTableReader table_A = cCtx.getArrowReader(1);                                       // DIFF: hard-coded
        while (table_A.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_A_vc_0 = ((org.apache.arrow.vector.IntVector) table_A.getVector(0));
            org.apache.arrow.vector.IntVector table_A_vc_1 = ((org.apache.arrow.vector.IntVector) table_A.getVector(1));
            org.apache.arrow.vector.IntVector table_A_vc_2 = ((org.apache.arrow.vector.IntVector) table_A.getVector(2));
            VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector_0, table_A_vc_0, false);
            int recordCount = table_A_vc_0.getValueCount();
            for (int i = 0; i < recordCount; i++) {
                int left_join_record_key = table_A_vc_0.get(i);
                join_map_0.associate(left_join_record_key, pre_hash_vector_0[i], left_join_record_key, table_A_vc_1.get(i), table_A_vc_2.get(i));
            }
        }
        int[] join_result_vector_ord_0_0 = this.allocationManager.getIntVector();                   // DIFF: hard-coded
        int[] join_result_vector_ord_1_0 = this.allocationManager.getIntVector();                   // DIFF: hard-coded
        int[] join_result_vector_ord_2_0 = this.allocationManager.getIntVector();                   // DIFF: hard-coded
        int[] join_result_vector_ord_3_0 = this.allocationManager.getIntVector();                   // DIFF: hard-coded
        int[] join_result_vector_ord_4_0 = this.allocationManager.getIntVector();                   // DIFF: hard-coded
        int[] join_result_vector_ord_5_0 = this.allocationManager.getIntVector();                   // DIFF: hard-coded
        // ArrowTableReader table_B = cCtx.getArrowReader(2);                                       // DIFF: hard-coded
        while (table_B.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_B_vc_0 = ((org.apache.arrow.vector.IntVector) table_B.getVector(0));
            org.apache.arrow.vector.IntVector table_B_vc_1 = ((org.apache.arrow.vector.IntVector) table_B.getVector(1));
            org.apache.arrow.vector.IntVector table_B_vc_2 = ((org.apache.arrow.vector.IntVector) table_B.getVector(2));
            VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector_0, table_B_vc_0, false);
            int recordCount = table_B_vc_0.getValueCount();
            int currentRecordIndex = 0;
            while (currentRecordIndex < recordCount) {
                int currentResultIndex = 0;
                while (currentRecordIndex < recordCount) {
                    int right_join_key = table_B_vc_0.get(currentRecordIndex);
                    long right_join_key_pre_hash = pre_hash_vector_0[currentRecordIndex];
                    int records_to_join_index = join_map_0.getIndex(right_join_key, right_join_key_pre_hash);
                    if ((records_to_join_index == -1)) {
                        currentRecordIndex++;
                        continue;
                    }
                    int left_join_record_count = join_map_0.keysRecordCount[records_to_join_index];
                    if ((left_join_record_count > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))) {
                        break;
                    }
                    int right_join_ord_1 = table_B_vc_1.get(currentRecordIndex);
                    int right_join_ord_2 = table_B_vc_2.get(currentRecordIndex);
                    for (int i = 0; i < left_join_record_count; i++) {
                        join_result_vector_ord_0_0[currentResultIndex] = join_map_0.values_record_ord_0[records_to_join_index][i];
                        join_result_vector_ord_1_0[currentResultIndex] = join_map_0.values_record_ord_1[records_to_join_index][i];
                        join_result_vector_ord_2_0[currentResultIndex] = join_map_0.values_record_ord_2[records_to_join_index][i];
                        join_result_vector_ord_3_0[currentResultIndex] = right_join_key;
                        join_result_vector_ord_4_0[currentResultIndex] = right_join_ord_1;
                        join_result_vector_ord_5_0[currentResultIndex] = right_join_ord_2;
                        currentResultIndex++;
                    }
                    currentRecordIndex++;
                }
                VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector, join_result_vector_ord_1_0, currentResultIndex, false);
                int recordCount_0 = currentResultIndex;
                int currentRecordIndex_0 = 0;
                while (currentRecordIndex_0 < recordCount_0) {
                    int currentResultIndex_0 = 0;
                    while (currentRecordIndex_0 < recordCount_0) {
                        int right_join_key_0 = join_result_vector_ord_1_0[currentRecordIndex_0];
                        long right_join_key_0_pre_hash = pre_hash_vector[currentRecordIndex_0];
                        int records_to_join_index_0 = join_map.getIndex(right_join_key_0, right_join_key_0_pre_hash);
                        if ((records_to_join_index_0 == -1)) {
                            currentRecordIndex_0++;
                            continue;
                        }
                        int left_join_record_count_0 = join_map.keysRecordCount[records_to_join_index_0];
                        if ((left_join_record_count_0 > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex_0))) {
                            break;
                        }
                        int right_join_ord_0 = join_result_vector_ord_0_0[currentRecordIndex_0];
                        int right_join_ord_2_0 = join_result_vector_ord_2_0[currentRecordIndex_0];
                        int right_join_ord_3 = join_result_vector_ord_3_0[currentRecordIndex_0];
                        int right_join_ord_4 = join_result_vector_ord_4_0[currentRecordIndex_0];
                        int right_join_ord_5 = join_result_vector_ord_5_0[currentRecordIndex_0];
                        for (int i_0 = 0; i_0 < left_join_record_count_0; i_0++) {
                            join_result_vector_ord_0[currentResultIndex_0] = join_map.values_record_ord_0[records_to_join_index_0][i_0];
                            join_result_vector_ord_1[currentResultIndex_0] = join_map.values_record_ord_1[records_to_join_index_0][i_0];
                            join_result_vector_ord_2[currentResultIndex_0] = join_map.values_record_ord_2[records_to_join_index_0][i_0];
                            join_result_vector_ord_3[currentResultIndex_0] = right_join_ord_0;
                            join_result_vector_ord_4[currentResultIndex_0] = right_join_key_0;
                            join_result_vector_ord_5[currentResultIndex_0] = right_join_ord_2_0;
                            join_result_vector_ord_6[currentResultIndex_0] = right_join_ord_3;
                            join_result_vector_ord_7[currentResultIndex_0] = right_join_ord_4;
                            join_result_vector_ord_8[currentResultIndex_0] = right_join_ord_5;
                            currentResultIndex_0++;
                        }
                        currentRecordIndex_0++;
                    }
                    count += currentResultIndex_0;
                }
            }
        }
        this.allocationManager.release(pre_hash_vector_0);                                          // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_0_0);                                 // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_1_0);                                 // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_2_0);                                 // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_3_0);                                 // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_4_0);                                 // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_5_0);                                 // DIFF: hard-coded
        this.allocationManager.release(pre_hash_vector);                                            // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_0);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_1);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_2);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_3);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_4);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_5);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_6);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_7);                                   // DIFF: hard-coded
        this.allocationManager.release(join_result_vector_ord_8);                                   // DIFF: hard-coded
        // System.out.println(count);                                                               // DIFF: removed
        this.result = count;                                                                        // DIFF: added
    }

}
