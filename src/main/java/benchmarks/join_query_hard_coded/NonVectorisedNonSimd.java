package benchmarks.join_query_hard_coded;

import benchmarks.join_query_hard_coded.NonVectorisedNonSimdGenSupport.KeyMultiRecordMap_10523395;
import benchmarks.join_query_hard_coded.NonVectorisedNonSimdGenSupport.KeyMultiRecordMap_1123573668;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.CachingArrowTableReader;
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
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using the code generated
 * by its non-vectorised query code generation without SIMD-ed operators, but while not actually
 * invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class NonVectorisedNonSimd {

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
     * State: the size at which the table_A hash-table should be initialised.
     */
    private int table_A_hashTable_size;

    /**
     * State: the size at which the (table_A x table_B) join-result hash-table should be initialised.
     */
    private int table_A_x_table_B_hashTable_size;

    /**
     * State: the result of the query (-1 if the query has not been executed yet).
     */
    private long result;

    /**
     * State: the expected result of the query.
     */
    private long expectedResult;

    /**
     * State: the table_A_x_table_B join map.
     * DIFF: usually part of the query execution itself.
     */
    private KeyMultiRecordMap_1123573668 join_map;

    /**
     * State: the table_A join map.
     * DIFF: usually part of the query execution itself.
     */
    private KeyMultiRecordMap_10523395 join_map_0;

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        this.table_A = new CachingArrowTableReader(new File(this.tableFilePath + "/table_A.arrow"), this.rootAllocator);
        this.table_B = new CachingArrowTableReader(new File(this.tableFilePath + "/table_B.arrow"), this.rootAllocator);
        this.table_C = new CachingArrowTableReader(new File(this.tableFilePath + "/table_C.arrow"), this.rootAllocator);

        // Compute the hash-table sizes as the correct power of two size
        this.table_A_hashTable_size = Integer.highestOneBit(3 * 1024 * 1024) << 1;
        double conversionFactor = Double.parseDouble(this.tableFilePath.split("B_")[1].split("_C_")[0]);
        int expectedJoinSize = (int) (conversionFactor * (3 * 1024 * 1024));
        this.table_A_x_table_B_hashTable_size = Integer.highestOneBit(expectedJoinSize) << 1;

        // Allocate the hash-tables
        this.join_map = new KeyMultiRecordMap_1123573668(this.table_A_x_table_B_hashTable_size);
        this.join_map_0 = new KeyMultiRecordMap_10523395(this.table_A_hashTable_size);

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
        int agg_0_count = 0;
        // DIFF: class definition is moved outside query, different capacity, allocated before query
        // KeyMultiRecordMap_1123573668 join_map = new KeyMultiRecordMap_1123573668(this.table_A_x_table_B_hashTable_size);
        // DIFF: class definition is moved outside query, different capacity, allocated before query
        // KeyMultiRecordMap_10523395 join_map_0 = new KeyMultiRecordMap_10523395(this.table_A_hashTable_size);

        // ArrowTableReader table_A = cCtx.getArrowReader(0);                          DIFF: hard-coded in the setup phase
        while (table_A.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_A_vc_0 = ((org.apache.arrow.vector.IntVector) table_A.getVector(0));
            org.apache.arrow.vector.IntVector table_A_vc_1 = ((org.apache.arrow.vector.IntVector) table_A.getVector(1));
            org.apache.arrow.vector.IntVector table_A_vc_2 = ((org.apache.arrow.vector.IntVector) table_A.getVector(2));
            int recordCount = table_A_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = table_A_vc_0.get(aviv);
                long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                int ordinal_value_0 = table_A_vc_1.get(aviv);
                int ordinal_value_1 = table_A_vc_2.get(aviv);
                join_map_0.associate(ordinal_value, left_join_key_prehash, ordinal_value, ordinal_value_0, ordinal_value_1);
            }
        }
        // ArrowTableReader table_B = cCtx.getArrowReader(1);                          DIFF: hard-coded in the setup phase
        while (table_B.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_B_vc_0 = ((org.apache.arrow.vector.IntVector) table_B.getVector(0));
            org.apache.arrow.vector.IntVector table_B_vc_1 = ((org.apache.arrow.vector.IntVector) table_B.getVector(1));
            org.apache.arrow.vector.IntVector table_B_vc_2 = ((org.apache.arrow.vector.IntVector) table_B.getVector(2));
            int recordCount = table_B_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = table_B_vc_0.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                int records_to_join_index = join_map_0.getIndex(ordinal_value, right_join_key_prehash);
                if ((records_to_join_index == -1)) {
                    continue;
                }
                int ordinal_value_0 = table_B_vc_1.get(aviv);
                int ordinal_value_1 = table_B_vc_2.get(aviv);
                int left_join_record_count = join_map_0.keysRecordCount[records_to_join_index];
                for (int i = 0; i < left_join_record_count; i++) {
                    int left_join_ord_0 = join_map_0.values_record_ord_0[records_to_join_index][i];
                    int left_join_ord_1 = join_map_0.values_record_ord_1[records_to_join_index][i];
                    int left_join_ord_2 = join_map_0.values_record_ord_2[records_to_join_index][i];
                    long left_join_key_prehash = Int_Hash_Function.preHash(left_join_ord_1);
                    join_map.associate(left_join_ord_1, left_join_key_prehash, left_join_ord_0, left_join_ord_1, left_join_ord_2, ordinal_value, ordinal_value_0, ordinal_value_1);
                }
            }
        }
        // ArrowTableReader table_C = cCtx.getArrowReader(2);                          DIFF: hard-coded in the setup phase
        while (table_C.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_C_vc_0 = ((org.apache.arrow.vector.IntVector) table_C.getVector(0));
            org.apache.arrow.vector.IntVector table_C_vc_1 = ((org.apache.arrow.vector.IntVector) table_C.getVector(1));
            org.apache.arrow.vector.IntVector table_C_vc_2 = ((org.apache.arrow.vector.IntVector) table_C.getVector(2));
            int recordCount = table_C_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = table_C_vc_0.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                int records_to_join_index = join_map.getIndex(ordinal_value, right_join_key_prehash);
                if ((records_to_join_index == -1)) {
                    continue;
                }
                int ordinal_value_0 = table_C_vc_1.get(aviv);
                int ordinal_value_1 = table_C_vc_2.get(aviv);
                int left_join_record_count = join_map.keysRecordCount[records_to_join_index];
                for (int i = 0; i < left_join_record_count; i++) {
                    int left_join_ord_0 = join_map.values_record_ord_0[records_to_join_index][i];
                    int left_join_ord_1 = join_map.values_record_ord_1[records_to_join_index][i];
                    int left_join_ord_2 = join_map.values_record_ord_2[records_to_join_index][i];
                    int left_join_ord_3 = join_map.values_record_ord_3[records_to_join_index][i];
                    int left_join_ord_4 = join_map.values_record_ord_4[records_to_join_index][i];
                    int left_join_ord_5 = join_map.values_record_ord_5[records_to_join_index][i];
                    agg_0_count++;
                }
            }
        }
        // System.out.println(agg_0_count);                                            DIFF: replaced with line below
        this.result = agg_0_count;                                                  // DIFF: introduced to replace line above
    }

}
