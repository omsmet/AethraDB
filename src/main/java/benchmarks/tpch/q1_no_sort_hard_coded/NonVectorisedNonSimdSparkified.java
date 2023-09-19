package benchmarks.tpch.q1_no_sort_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.general_support.ArrowOptimisations;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.KVIterator;
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
public class NonVectorisedNonSimdSparkified {

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
    private ArrowTableReader lineitem_table;

    /**
     * State: the hash-table which is used by the query to aggregate.
     */
    private SparkAggregationMap aggregation_state_map;

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
        ImmutableIntList columnsToProject = ImmutableIntList.of(4, 5, 6, 7, 8, 9, 10);
        this.lineitem_table = new ABQArrowTableReader(
                new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator, columnsToProject);

        // Initialise the hash-table
        this.aggregation_state_map = new SparkAggregationMap();

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
    public void invocationSetup() throws Exception {
        // Reset the table
        this.lineitem_table.reset();

        // Reset the aggregation map
        this.aggregation_state_map.reset();
    }

    /**
     * This method clears up after this benchmark for has finished.
     */
    @TearDown(Level.Trial)
    public void trialTeardown() {
        this.aggregation_state_map.stop();
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void invocationTeardown() {
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
            "-Xms16g",
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    })
    public void executeQuery(Blackhole bh) throws IOException {
        byte[] byte_array_cache = null;
        byte[] byte_array_cache_0 = null;
        while (lineitem_table.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_vc_0 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(6));
            org.apache.arrow.vector.Float8Vector lineitem_vc_3 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(7));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_4 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem_table.getVector(8));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_5 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem_table.getVector(9));
            org.apache.arrow.vector.DateDayVector lineitem_vc_6 = ((org.apache.arrow.vector.DateDayVector) lineitem_table.getVector(10));
            int recordCount = lineitem_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = lineitem_vc_6.get(aviv);
                if (!((ordinal_value <= 10471))) {
                    continue;
                }
                byte_array_cache = ArrowOptimisations.getFixedSizeBinaryValue(lineitem_vc_4, aviv, byte_array_cache);
                byte_array_cache_0 = ArrowOptimisations.getFixedSizeBinaryValue(lineitem_vc_5, aviv, byte_array_cache_0);
                double ordinal_value_5 = lineitem_vc_0.get(aviv);
                double ordinal_value_6 = lineitem_vc_1.get(aviv);
                double ordinal_value_7 = lineitem_vc_2.get(aviv);
                double ordinal_value_8 = lineitem_vc_3.get(aviv);
                this.aggregation_state_map.hashAgg_doConsume_0(ordinal_value_5, ordinal_value_6, ordinal_value_7, ordinal_value_8, byte_array_cache[0], byte_array_cache_0[0]);
            }
        }

        int key_i = 0;
        KVIterator<UnsafeRow, UnsafeRow> resultIterator = this.aggregation_state_map.resultIterator();
        while (resultIterator.next()) {
            UnsafeRow resultKey = resultIterator.getKey();
            UnsafeRow resultAggregationState = resultIterator.getValue();

            byte[] groupKey_0 = new byte[] { resultKey.getByte(0) };
            byte[] groupKey_1 = new byte[] { resultKey.getByte(1) };
            double aggregation_0_value = resultAggregationState.getDouble(0);
            double aggregation_1_value = resultAggregationState.getDouble(1);
            double aggregation_2_value = resultAggregationState.getDouble(2);
            double aggregation_3_value = resultAggregationState.getDouble(3);
            double projection_computation_result = resultAggregationState.getDouble(4) / resultAggregationState.getLong(5);
            double projection_computation_result_0 = resultAggregationState.getDouble(6) / resultAggregationState.getLong(7);
            double projection_computation_result_1 = resultAggregationState.getDouble(8) / resultAggregationState.getLong(9);
            int aggregation_4_value = (int) resultAggregationState.getLong(10);

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

            key_i++;
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
