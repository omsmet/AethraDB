package benchmarks.join_query_hard_coded;

import benchmarks.join_query_hard_coded.UnoptimisedSupport.JoinMap0Type;
import benchmarks.join_query_hard_coded.UnoptimisedSupport.JoinMapType;
import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.general_support.hashmaps.Int_Hash_Function;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.util.ImmutableIntList;
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
    private JoinMapType join_map;

    /**
     * State: the table_A join map.
     * DIFF: usually part of the query execution itself.
     */
    private JoinMap0Type join_map_0;

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        ImmutableIntList threeColumnIdentity = ImmutableIntList.identity(3);
        this.table_A = new ABQArrowTableReader(new File(this.tableFilePath + "/table_A.arrow"), this.rootAllocator, threeColumnIdentity);
        this.table_B = new ABQArrowTableReader(new File(this.tableFilePath + "/table_B.arrow"), this.rootAllocator, threeColumnIdentity);
        this.table_C = new ABQArrowTableReader(new File(this.tableFilePath + "/table_C.arrow"), this.rootAllocator, threeColumnIdentity);

        // Compute the hash-table sizes as the correct power of two size
        int hashTableSize = 3 * 1024 * 1024;
        if (this.tableFilePath.contains("sf10/"))
            hashTableSize *= 10;
        else if (this.tableFilePath.contains("sf20/"))
            hashTableSize *= 20;
        hashTableSize = Integer.highestOneBit(hashTableSize) << 2;

        // Allocate the hash-tables
        this.join_map = new JoinMapType(hashTableSize);
        this.join_map_0 = new JoinMap0Type(hashTableSize);

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
            "-Xmx32g",
            "-Xms16g"
    })
    public void executeQuery() throws IOException {
        long count = 0;
        // DIFF: hard-coded
        // KeyMultiRecordMap_1998224723 join_map = new KeyMultiRecordMap_1998224723();
        // ArrowTableReader table_C = cCtx.getArrowReader(0);
        while (table_C.loadNextBatch()) {
            org.apache.arrow.vector.IntVector table_C_vc_0 = ((org.apache.arrow.vector.IntVector) table_C.getVector(0));
            org.apache.arrow.vector.IntVector table_C_vc_1 = ((org.apache.arrow.vector.IntVector) table_C.getVector(1));
            org.apache.arrow.vector.IntVector table_C_vc_2 = ((org.apache.arrow.vector.IntVector) table_C.getVector(2));
            int recordCount = table_C_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = table_C_vc_0.get(aviv);
                long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                int ordinal_value_0 = table_C_vc_1.get(aviv);
                int ordinal_value_1 = table_C_vc_2.get(aviv);
                join_map.associate(ordinal_value, left_join_key_prehash, ordinal_value_0, ordinal_value_1);
            }
        }
        // DIFF: hard-coded
        // KeyMultiRecordMap_1086508626 join_map_0 = new KeyMultiRecordMap_1086508626();
        // ArrowTableReader table_A = cCtx.getArrowReader(1);
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
                join_map_0.associate(ordinal_value, left_join_key_prehash, ordinal_value_0, ordinal_value_1);
            }
        }
        // DIFF: hard-coded
        // ArrowTableReader table_B = cCtx.getArrowReader(2);
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
                    long right_join_key_prehash_0 = Int_Hash_Function.preHash(left_join_ord_0);
                    int records_to_join_index_0 = join_map.getIndex(left_join_ord_0, right_join_key_prehash_0);
                    if ((records_to_join_index_0 == -1)) {
                        continue;
                    }
                    int left_join_record_count_0 = join_map.keysRecordCount[records_to_join_index_0];
                    for (int i_0 = 0; i_0 < left_join_record_count_0; i_0++) {
                        int left_join_ord_0_0 = join_map.values_record_ord_0[records_to_join_index_0][i_0];
                        int left_join_ord_1_0 = join_map.values_record_ord_1[records_to_join_index_0][i_0];
                        count++;
                    }
                }
            }
        }
        // System.out.println(count);                                                               // DIFF: removed
        this.result = count;                                                                        // DIFF: added
    }

}
