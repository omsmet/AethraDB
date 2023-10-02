package AethraDB.benchmarks.tpch.q1_no_sort_hard_coded;

import AethraDB.evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader;
import AethraDB.evaluation.general_support.ArrowOptimisations;
import AethraDB.evaluation.general_support.hashmaps.Char_Arr_Hash_Function;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates the performance of AethraDB using its non-vectorised query code
 * generation without SIMD-ed operators, but while not actually invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class NonVectorisedNonSimd {

    /**
     * Different instances of the TPC-H database can be tested using this benchmark.
     */
    @Param({
            "/nvtmp/AethraTestData/tpch/sf-1",
            "/nvtmp/AethraTestData/tpch/sf-10",
            "/nvtmp/AethraTestData/tpch/sf-100"
    })
    private String tpchInstance;

    /**
     * State: the {@link RootAllocator} used for reading Arrow files.
     */
    private RootAllocator rootAllocator;

    /**
     * State: the {@link ArrowTableReader} used for reading the lineitem table.
     */
    private ArrowTableReader lineitem;

    /**
     * State: the hash-table which is used by the query to aggregate.
     */
    private AggregationMap aggregation_state_map;

    /**
     * State: the result of the query.
     */
    private byte[][] resultReturnFlag;
    private byte[][] resultLineStatus;
    private double[] resultSumQuantity;
    private double[] resultSumBasePrice;
    private double[] resultSumDiscPrice;
    private double[] resultSumCharge;
    private double[] resultAvgQuantity;
    private double[] resultAvgPrice;
    private double[] resultAvgDisc;
    private int[] resultCountOrder;

    /**
     * State: the result verifier for the query.
     */
    private ResultVerifier resultVerifier;

    /**
     * Method to set up teh state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        int[] columnsToProject = new int[] { 4, 5, 6, 7, 8, 9, 10 };
        this.lineitem = new ABQArrowTableReader(
                new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator, true, columnsToProject);

        // Initialise the hash-table
        this.aggregation_state_map = new AggregationMap();

        // Initialise the result verifier
        this.resultVerifier = new ResultVerifier(this.tpchInstance + "/q1_result.csv");

        // Initialise the result
        int resultSize = this.resultVerifier.getResultSize();
        this.resultReturnFlag = new byte[resultSize][];
        Arrays.fill(this.resultReturnFlag, null);
        this.resultLineStatus = new byte[resultSize][];
        Arrays.fill(this.resultLineStatus, null);
        this.resultSumQuantity = new double[resultSize];
        Arrays.fill(this.resultSumQuantity, -1);
        this.resultSumBasePrice = new double[resultSize];
        Arrays.fill(this.resultSumBasePrice, -1);
        this.resultSumDiscPrice = new double[resultSize];
        Arrays.fill(this.resultSumDiscPrice, -1);
        this.resultSumCharge = new double[resultSize];
        Arrays.fill(this.resultSumCharge, -1);
        this.resultAvgQuantity = new double[resultSize];
        Arrays.fill(this.resultAvgQuantity, -1);
        this.resultAvgPrice = new double[resultSize];
        Arrays.fill(this.resultAvgPrice, -1);
        this.resultAvgDisc = new double[resultSize];
        Arrays.fill(this.resultAvgDisc, -1);
        this.resultCountOrder = new int[resultSize];
        Arrays.fill(this.resultCountOrder, -1);
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationStetup() throws Exception {
        // Reset the table
        this.lineitem.reset();

        // Reset the aggregation map
        this.aggregation_state_map.reset();
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void teardown() {
        // Verify the result
        if (!this.resultVerifier.resultCorrect(
                this.resultReturnFlag,
                this.resultLineStatus,
                this.resultSumQuantity,
                this.resultSumBasePrice,
                this.resultSumDiscPrice,
                this.resultSumCharge,
                this.resultAvgQuantity,
                this.resultAvgPrice,
                this.resultAvgDisc,
                this.resultCountOrder)
        ) {
            throw new RuntimeException("The computed result is incorrect");
        }

        // Reset the result after verifying it
        Arrays.fill(this.resultReturnFlag, null);
        Arrays.fill(this.resultLineStatus, null);
        Arrays.fill(this.resultSumQuantity, -1);
        Arrays.fill(this.resultSumBasePrice, -1);
        Arrays.fill(this.resultSumDiscPrice, -1);
        Arrays.fill(this.resultSumCharge, -1);
        Arrays.fill(this.resultAvgQuantity, -1);
        Arrays.fill(this.resultAvgPrice, -1);
        Arrays.fill(this.resultAvgDisc, -1);
        Arrays.fill(this.resultCountOrder, -1);
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
    public void executeQuery(Blackhole bh) throws IOException {
        byte[] byte_array_cache = null;
        byte[] byte_array_cache_0 = null;
        // DIFF: hard-coded
        // KeyValueMap_2062888647 aggregation_state_map = new KeyValueMap_2062888647();
        // ArrowTableReader lineitem = cCtx.getArrowReader(0);
        while (lineitem.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_vc_0 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(6));
            org.apache.arrow.vector.Float8Vector lineitem_vc_3 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(7));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_4 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(8));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_5 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(9));
            org.apache.arrow.vector.DateDayVector lineitem_vc_6 = ((org.apache.arrow.vector.DateDayVector) lineitem.getVector(10));
            int recordCount = lineitem_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = lineitem_vc_6.get(aviv);
                if (!((ordinal_value <= 10471))) {
                    continue;
                }
                int projection_literal = 1;
                double ordinal_value_0 = lineitem_vc_2.get(aviv);
                double projection_computation_result = (projection_literal - ordinal_value_0);
                double ordinal_value_1 = lineitem_vc_1.get(aviv);
                double projection_computation_result_0 = (ordinal_value_1 * projection_computation_result);
                int projection_literal_0 = 1;
                double ordinal_value_2 = lineitem_vc_2.get(aviv);
                double projection_computation_result_1 = (projection_literal_0 - ordinal_value_2);
                double ordinal_value_3 = lineitem_vc_1.get(aviv);
                double projection_computation_result_2 = (ordinal_value_3 * projection_computation_result_1);
                int projection_literal_1 = 1;
                double ordinal_value_4 = lineitem_vc_3.get(aviv);
                double projection_computation_result_3 = (projection_literal_1 + ordinal_value_4);
                double projection_computation_result_4 = (projection_computation_result_2 * projection_computation_result_3);
                byte_array_cache = ArrowOptimisations.getFixedSizeBinaryValue(lineitem_vc_4, aviv, byte_array_cache);
                byte_array_cache_0 = ArrowOptimisations.getFixedSizeBinaryValue(lineitem_vc_5, aviv, byte_array_cache_0);
                long group_key_pre_hash = Char_Arr_Hash_Function.preHash(byte_array_cache);
                group_key_pre_hash ^= Char_Arr_Hash_Function.preHash(byte_array_cache_0);
                double ordinal_value_5 = lineitem_vc_0.get(aviv);
                double ordinal_value_6 = lineitem_vc_1.get(aviv);
                double ordinal_value_7 = lineitem_vc_2.get(aviv);
                aggregation_state_map.incrementForKey(byte_array_cache, byte_array_cache_0, group_key_pre_hash, ordinal_value_5, ordinal_value_6, projection_computation_result_0, projection_computation_result_4, 1, ordinal_value_7);
            }
        }
        for (int key_i = 0; key_i < aggregation_state_map.numberOfRecords; key_i++) {
            byte[] groupKey_0 = aggregation_state_map.keys_ord_0[key_i];
            byte[] groupKey_1 = aggregation_state_map.keys_ord_1[key_i];
            double aggregation_0_value = aggregation_state_map.values_ord_0[key_i];
            double aggregation_1_value = aggregation_state_map.values_ord_1[key_i];
            double aggregation_2_value = aggregation_state_map.values_ord_2[key_i];
            double aggregation_3_value = aggregation_state_map.values_ord_3[key_i];
            int aggregation_4_value = aggregation_state_map.values_ord_4[key_i];
            double aggregation_5_value = aggregation_state_map.values_ord_5[key_i];
            double projection_computation_result = (aggregation_0_value / aggregation_4_value);
            double projection_computation_result_0 = (aggregation_1_value / aggregation_4_value);
            double projection_computation_result_1 = (aggregation_5_value / aggregation_4_value);
            // DIFF: replaced
            // System.out.print((new java.lang.String(groupKey_0) + ", "));
            // System.out.print((new java.lang.String(groupKey_1) + ", "));
            // System.out.print((String.format("%.2f", aggregation_0_value) + ", "));
            // System.out.print((String.format("%.2f", aggregation_1_value) + ", "));
            // System.out.print((String.format("%.2f", aggregation_2_value) + ", "));
            // System.out.print((String.format("%.2f", aggregation_3_value) + ", "));
            // System.out.print((String.format("%.2f", projection_computation_result) + ", "));
            // System.out.print((String.format("%.2f", projection_computation_result_0) + ", "));
            // System.out.print((String.format("%.2f", projection_computation_result_1) + ", "));
            // System.out.println(aggregation_4_value);
            this.resultReturnFlag[key_i] = groupKey_0;
            this.resultLineStatus[key_i] = groupKey_1;
            this.resultSumQuantity[key_i] = aggregation_0_value;
            this.resultSumBasePrice[key_i] = aggregation_1_value;
            this.resultSumDiscPrice[key_i] = aggregation_2_value;
            this.resultSumCharge[key_i] = aggregation_3_value;
            this.resultAvgQuantity[key_i] = projection_computation_result;
            this.resultAvgPrice[key_i] = projection_computation_result_0;
            this.resultAvgDisc[key_i] = projection_computation_result_1;
            this.resultCountOrder[key_i] = aggregation_4_value;
        }

        // DIFF: prevent optimising the result away
        bh.consume(this.resultReturnFlag);
        bh.consume(this.resultLineStatus);
        bh.consume(this.resultSumQuantity);
        bh.consume(this.resultSumBasePrice);
        bh.consume(this.resultSumDiscPrice);
        bh.consume(this.resultSumCharge);
        bh.consume(this.resultAvgQuantity);
        bh.consume(this.resultAvgPrice);
        bh.consume(this.resultAvgDisc);
        bh.consume(this.resultCountOrder);
    }

}
