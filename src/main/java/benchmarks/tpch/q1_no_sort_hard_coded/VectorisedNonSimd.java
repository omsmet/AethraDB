package benchmarks.tpch.q1_no_sort_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.AllocationManager;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.BufferPoolAllocationManager;
import evaluation.vector_support.VectorisedAggregationOperators;
import evaluation.vector_support.VectorisedArithmeticOperators;
import evaluation.vector_support.VectorisedFilterOperators;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates the performance of AethraDB using its vectorised query code
 * generation without SIMD-ed operators, but while not actually invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class VectorisedNonSimd {

    /**
     * Different instances of the TPC-H database can be tested using this benchmark.
     */
    @Param({
            "/nvtmp/AethraTestData/tpch/sf-1",
            "/nvtmp/AethraTestData/tpch/sf-10"
    })
    private String tpchInstance;

    /**
     * State: the {@link RootAllocator} used for reading Arrow files.
     */
    private RootAllocator rootAllocator;

    /**
     * State: the {@link AllocationManager} used for performing query-specific allocations.
     */
    private AllocationManager allocationManager;

    /**
     * State: the {@link ArrowTableReader} used for reading the lineitem table.
     */
    private ArrowTableReader lineitem_table;

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
        this.lineitem_table = new ABQArrowTableReader(
                new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator);

        // Setup the allocation manager
        this.allocationManager = new BufferPoolAllocationManager(16);

        // Initialise the hash-table
        this.aggregation_state_map = new AggregationMap();

        // Initialise the result
        int resultSize = 4;
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

        // Initialise the result verifier
        this.resultVerifier = new ResultVerifier(this.tpchInstance + "/q1_result.csv");
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationStetup() throws Exception {
        // Reset the table
        this.lineitem_table.reset();

        // Perform allocation manager maintenance
        this.allocationManager.performMaintenance();

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
        // DIFF: hard-coded allocation manager
        int[] ordinal_6_sel_vec = this.allocationManager.getIntVector();
        double[] projection_computation_result = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_0 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_1 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_2 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_3 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_4 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_5 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_6 = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_7 = this.allocationManager.getDoubleVector();
        long[] groupKeyPreHashVector = this.allocationManager.getLongVector();

        // DIFF: hard-coded
        // KeyValueMap_1456006896 aggregation_state_map = new KeyValueMap_1456006896();
        // ArrowTableReader lineitem = cCtx.getArrowReader(0);
        while (lineitem_table.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_vc_0 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(6));
            org.apache.arrow.vector.Float8Vector lineitem_vc_3 = ((org.apache.arrow.vector.Float8Vector) lineitem_table.getVector(7));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_4 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem_table.getVector(8));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_5 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem_table.getVector(9));
            org.apache.arrow.vector.DateDayVector lineitem_vc_6 = ((org.apache.arrow.vector.DateDayVector) lineitem_table.getVector(10));
            int ordinal_6_sel_vec_length = VectorisedFilterOperators.le(lineitem_vc_6, 10471, ordinal_6_sel_vec);
            int projection_literal = 1;
            int projection_computation_result_length;
            projection_computation_result_length = VectorisedArithmeticOperators.subtract(projection_literal, lineitem_vc_2, ordinal_6_sel_vec, ordinal_6_sel_vec_length, projection_computation_result);
            int projection_computation_result_0_length;
            projection_computation_result_0_length = VectorisedArithmeticOperators.multiply(lineitem_vc_1, projection_computation_result, projection_computation_result_length, ordinal_6_sel_vec, ordinal_6_sel_vec_length, projection_computation_result_0);
            int projection_literal_0 = 1;
            int projection_computation_result_1_length;
            projection_computation_result_1_length = VectorisedArithmeticOperators.subtract(projection_literal_0, lineitem_vc_2, ordinal_6_sel_vec, ordinal_6_sel_vec_length, projection_computation_result_1);
            int projection_computation_result_2_length;
            projection_computation_result_2_length = VectorisedArithmeticOperators.multiply(lineitem_vc_1, projection_computation_result_1, projection_computation_result_1_length, ordinal_6_sel_vec, ordinal_6_sel_vec_length, projection_computation_result_2);
            int projection_literal_1 = 1;
            int projection_computation_result_3_length;
            projection_computation_result_3_length = VectorisedArithmeticOperators.add(projection_literal_1, lineitem_vc_3, ordinal_6_sel_vec, ordinal_6_sel_vec_length, projection_computation_result_3);
            int projection_computation_result_4_length;
            projection_computation_result_4_length = VectorisedArithmeticOperators.multiply(projection_computation_result_2, projection_computation_result_2_length, projection_computation_result_3, projection_computation_result_3_length, ordinal_6_sel_vec, ordinal_6_sel_vec_length, projection_computation_result_4);
            VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, lineitem_vc_4, ordinal_6_sel_vec, ordinal_6_sel_vec_length, false);
            VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, lineitem_vc_5, ordinal_6_sel_vec, ordinal_6_sel_vec_length, true);
            int recordCount = ordinal_6_sel_vec_length;
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int aviv_record_index = ordinal_6_sel_vec[aviv];
                aggregation_state_map.incrementForKey(lineitem_vc_4.get(aviv_record_index), lineitem_vc_5.get(aviv_record_index), groupKeyPreHashVector[aviv_record_index], lineitem_vc_0.get(aviv_record_index), lineitem_vc_1.get(aviv_record_index), projection_computation_result_0[aviv_record_index], projection_computation_result_4[aviv_record_index], 1, lineitem_vc_2.get(aviv_record_index));
            }
        }
        int aggregationResultVectorLength;
        // DIFF: hard-coded allocation manager
        byte[][] groupKeyVector_0 = this.allocationManager.getNestedByteVector();
        byte[][] groupKeyVector_1 = this.allocationManager.getNestedByteVector();
        double[] agg_G_SUM_0_vector = this.allocationManager.getDoubleVector();
        double[] agg_G_SUM_1_vector = this.allocationManager.getDoubleVector();
        double[] agg_G_SUM_2_vector = this.allocationManager.getDoubleVector();
        double[] agg_G_SUM_3_vector = this.allocationManager.getDoubleVector();
        int[] agg_G_COUNT_4_vector = this.allocationManager.getIntVector();
        double[] agg_G_SUM_5_vector = this.allocationManager.getDoubleVector();
        int current_key_offset = 0;
        int number_of_records = aggregation_state_map.numberOfRecords;
        while (current_key_offset < number_of_records) {
            aggregationResultVectorLength = VectorisedAggregationOperators.constructVector(groupKeyVector_0, aggregation_state_map.keys_ord_0, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_1, aggregation_state_map.keys_ord_1, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_0_vector, aggregation_state_map.values_ord_0, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_1_vector, aggregation_state_map.values_ord_1, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_2_vector, aggregation_state_map.values_ord_2, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_3_vector, aggregation_state_map.values_ord_3, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_COUNT_4_vector, aggregation_state_map.values_ord_4, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_5_vector, aggregation_state_map.values_ord_5, number_of_records, current_key_offset);
            int projection_computation_result_5_length;
            projection_computation_result_5_length = VectorisedArithmeticOperators.divide(agg_G_SUM_0_vector, aggregationResultVectorLength, agg_G_COUNT_4_vector, aggregationResultVectorLength, projection_computation_result_5);
            int projection_computation_result_6_length;
            projection_computation_result_6_length = VectorisedArithmeticOperators.divide(agg_G_SUM_1_vector, aggregationResultVectorLength, agg_G_COUNT_4_vector, aggregationResultVectorLength, projection_computation_result_6);
            int projection_computation_result_7_length;
            projection_computation_result_7_length = VectorisedArithmeticOperators.divide(agg_G_SUM_5_vector, aggregationResultVectorLength, agg_G_COUNT_4_vector, aggregationResultVectorLength, projection_computation_result_7);
            // DIFF: changed for result verification
            // VectorisedPrintOperators.print(groupKeyVector_0, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_1, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(agg_G_SUM_0_vector, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(agg_G_SUM_1_vector, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(agg_G_SUM_2_vector, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(agg_G_SUM_3_vector, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(projection_computation_result_5, projection_computation_result_5_length);
            // VectorisedPrintOperators.print(projection_computation_result_6, projection_computation_result_6_length);
            // VectorisedPrintOperators.print(projection_computation_result_7, projection_computation_result_7_length);
            // VectorisedPrintOperators.print(agg_G_COUNT_4_vector, aggregationResultVectorLength);
            // System.out.println();
            System.arraycopy(groupKeyVector_0, 0, this.resultReturnFlag, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_1, 0, this.resultLineStatus, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(agg_G_SUM_0_vector, 0, this.resultSumQuantity, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(agg_G_SUM_1_vector, 0, this.resultSumBasePrice, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(agg_G_SUM_2_vector, 0, this.resultSumDiscPrice, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(agg_G_SUM_3_vector, 0, this.resultSumCharge, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(projection_computation_result_5, 0, this.resultAvgQuantity, current_key_offset, projection_computation_result_5_length);
            System.arraycopy(projection_computation_result_6, 0, this.resultAvgPrice, current_key_offset, projection_computation_result_6_length);
            System.arraycopy(projection_computation_result_7, 0, this.resultAvgDisc, current_key_offset, projection_computation_result_7_length);
            System.arraycopy(agg_G_COUNT_4_vector, 0, this.resultCountOrder, current_key_offset, aggregationResultVectorLength);
            // DIFF: moved around
            current_key_offset += aggregationResultVectorLength;
        }
        // DIFF: hard-coded allocation manager
        this.allocationManager.release(groupKeyVector_0);
        this.allocationManager.release(groupKeyVector_1);
        this.allocationManager.release(agg_G_SUM_0_vector);
        this.allocationManager.release(agg_G_SUM_1_vector);
        this.allocationManager.release(agg_G_SUM_2_vector);
        this.allocationManager.release(agg_G_SUM_3_vector);
        this.allocationManager.release(agg_G_COUNT_4_vector);
        this.allocationManager.release(agg_G_SUM_5_vector);
        this.allocationManager.release(groupKeyPreHashVector);
        this.allocationManager.release(ordinal_6_sel_vec);
        this.allocationManager.release(projection_computation_result);
        this.allocationManager.release(projection_computation_result_0);
        this.allocationManager.release(projection_computation_result_1);
        this.allocationManager.release(projection_computation_result_2);
        this.allocationManager.release(projection_computation_result_3);
        this.allocationManager.release(projection_computation_result_4);
        this.allocationManager.release(projection_computation_result_5);
        this.allocationManager.release(projection_computation_result_6);
        this.allocationManager.release(projection_computation_result_7);

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
