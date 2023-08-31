package benchmarks.tpch.q3_no_sort_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.AllocationManager;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.BufferPoolAllocationManager;
import evaluation.vector_support.VectorisedAggregationOperators;
import evaluation.vector_support.VectorisedArithmeticOperators;
import evaluation.vector_support.VectorisedFilterOperators;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates the performance of AethraDB using its vectorised query code
 * generation with SIMD-ed operators, but while not actually invoking the code generator itself.
 * This specific benchmark does not use validity masks in arithmetic operations whenever possible.
 */
@State(Scope.Benchmark)
public class VectorisedSimdReducedMaskUse {

    /**
     * Different instances of the TPC-H database can be tested using this benchmark.
     */
    @Param({
        "/nvtmp/AethraTestData/tpch/sf-1"
    })
    private String tpchInstance;

    /**
     * State: the {@link RootAllocator} used for reading Arrow files.
     */
    private RootAllocator rootAllocator;

    /**
     * State: the {@link AllocationManager} used by the query.
     */
    private AllocationManager allocationManager;

    /**
     * State: the {@link ArrowTableReader} used for reading the customer table.
     */
    private ArrowTableReader customer;

    /**
     * State: the {@link ArrowTableReader} used for reading the orders table.
     */
    private ArrowTableReader orders;

    /**
     * State: the {@link ArrowTableReader} used for reading the lineitem table.
     */
    private ArrowTableReader lineitem;

    /**
     * State: the hash-table which is used by the query to aggregate.
     */
    private AggregationMap aggregation_state_map;

    /**
     * State: the hash-table which is used for the outer-most join.
     */
    private OuterMostJoinMap join_map;

    /**
     * State the hash-table which is used for the inner-most join.
     */
    private InnerMostJoinMap join_map_0;

    /**
     * State: the result of the query.
     */
    private int[] resultLOrderKey;
    private double[] resultRevenue;
    private int[] resultOOrderDate;
    private int[] resultOShippriority;

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
        this.customer = new ABQArrowTableReader(new File(this.tpchInstance + "/customer.arrow"), this.rootAllocator);
        this.orders = new ABQArrowTableReader(new File(this.tpchInstance + "/orders.arrow"), this.rootAllocator);
        this.lineitem = new ABQArrowTableReader(new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator);

        // Initialise the allocation manager
        this.allocationManager = new BufferPoolAllocationManager(32);

        // Initialise the hash-table
        this.aggregation_state_map = new AggregationMap();

        // Initialise the join maps
        this.join_map = new OuterMostJoinMap();
        this.join_map_0 = new InnerMostJoinMap();

        // Initialise the result
        int resultSize = 11620;
        this.resultLOrderKey = new int[resultSize];
        Arrays.fill(this.resultLOrderKey, -1);
        this.resultRevenue = new double[resultSize];
        Arrays.fill(this.resultRevenue, -1d);
        this.resultOOrderDate = new int[resultSize];
        Arrays.fill(this.resultOOrderDate, -1);
        this.resultOShippriority = new int[resultSize];
        Arrays.fill(this.resultOShippriority, -1);

        // Initialise the result verifier
        this.resultVerifier = new ResultVerifier(this.tpchInstance + "/q3_result.csv");
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationStetup() throws Exception {
        // Reset the table
        this.customer.reset();
        this.orders.reset();
        this.lineitem.reset();

        // Perform allocation manager maintenance
        this.allocationManager.performMaintenance();

        // Reset the aggregation map
        this.aggregation_state_map.reset();

        // Reset the join maps
        this.join_map.reset();
        this.join_map_0.reset();
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void teardown() {
        // Verify the result
        if (!this.resultVerifier.resultCorrect(
                this.resultLOrderKey,
                this.resultRevenue,
                this.resultOOrderDate,
                this.resultOShippriority)
        ) {
            throw new RuntimeException("The computed result is incorrect");
        }

        // Reset the result after verifying it
        Arrays.fill(this.resultLOrderKey, -1);
        Arrays.fill(this.resultRevenue, -1d);
        Arrays.fill(this.resultOOrderDate, -1);
        Arrays.fill(this.resultOShippriority, -1);
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
    public void executeQuery(Blackhole bh) throws IOException {
        // DIFF: hard-coded allocation manager
        boolean[] ordinal_6_val_mask = this.allocationManager.getBooleanVector();
        boolean[] ordinal_4_val_mask = this.allocationManager.getBooleanVector();
        boolean[] ordinal_10_val_mask = this.allocationManager.getBooleanVector();
        double[] projection_computation_result = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_0 = this.allocationManager.getDoubleVector();
        long[] groupKeyPreHashVector = this.allocationManager.getLongVector();

        // DIFF: hard-coded allocation manager
        long[] pre_hash_vector = this.allocationManager.getLongVector();
        long[] pre_hash_vector_0 = this.allocationManager.getLongVector();

        // DIFF: hard-coded
        // KeyValueMap_690070378 aggregation_state_map = new KeyValueMap_690070378();
        // KeyMultiRecordMap_2082740895 join_map = new KeyMultiRecordMap_2082740895();
        // KeyMultiRecordMap_2066478917 join_map_0 = new KeyMultiRecordMap_2066478917();

        // DIFF: hard-coded
        // ArrowTableReader customer = cCtx.getArrowReader(0);
        while (customer.loadNextBatch()) {
            org.apache.arrow.vector.IntVector customer_vc_0 = ((org.apache.arrow.vector.IntVector) customer.getVector(0));
            org.apache.arrow.vector.VarCharVector customer_vc_1 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(1));
            org.apache.arrow.vector.VarCharVector customer_vc_2 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(2));
            org.apache.arrow.vector.IntVector customer_vc_3 = ((org.apache.arrow.vector.IntVector) customer.getVector(3));
            org.apache.arrow.vector.FixedSizeBinaryVector customer_vc_4 = ((org.apache.arrow.vector.FixedSizeBinaryVector) customer.getVector(4));
            org.apache.arrow.vector.Float8Vector customer_vc_5 = ((org.apache.arrow.vector.Float8Vector) customer.getVector(5));
            org.apache.arrow.vector.FixedSizeBinaryVector customer_vc_6 = ((org.apache.arrow.vector.FixedSizeBinaryVector) customer.getVector(6));
            org.apache.arrow.vector.VarCharVector customer_vc_7 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(7));
            int ordinal_6_val_mask_length = VectorisedFilterOperators.eqSIMD(customer_vc_6, new byte[] { 66, 85, 73, 76, 68, 73, 78, 71, 32, 32 }, ordinal_6_val_mask);
            // DIFF: removed mask
            VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector_0, customer_vc_0, false);
            int recordCount = customer_vc_0.getValueCount();
            for (int i = 0; i < recordCount; i++) {
                if (!(ordinal_6_val_mask[i])) {
                    continue;
                }
                int left_join_record_key = customer_vc_0.get(i);
                join_map_0.associate(left_join_record_key, pre_hash_vector_0[i], left_join_record_key);
            }
        }
        // DIFF: hard-coded allocation manager
        int[] join_result_vector_ord_0_0 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_1_0 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_2_0 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_3_0 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_4_0 = this.allocationManager.getIntVector();
        // DIFF: hard-coded
        // ArrowTableReader orders = cCtx.getArrowReader(1);
        while (orders.loadNextBatch()) {
            org.apache.arrow.vector.IntVector orders_vc_0 = ((org.apache.arrow.vector.IntVector) orders.getVector(0));
            org.apache.arrow.vector.IntVector orders_vc_1 = ((org.apache.arrow.vector.IntVector) orders.getVector(1));
            org.apache.arrow.vector.FixedSizeBinaryVector orders_vc_2 = ((org.apache.arrow.vector.FixedSizeBinaryVector) orders.getVector(2));
            org.apache.arrow.vector.Float8Vector orders_vc_3 = ((org.apache.arrow.vector.Float8Vector) orders.getVector(3));
            org.apache.arrow.vector.DateDayVector orders_vc_4 = ((org.apache.arrow.vector.DateDayVector) orders.getVector(4));
            org.apache.arrow.vector.FixedSizeBinaryVector orders_vc_5 = ((org.apache.arrow.vector.FixedSizeBinaryVector) orders.getVector(5));
            org.apache.arrow.vector.FixedSizeBinaryVector orders_vc_6 = ((org.apache.arrow.vector.FixedSizeBinaryVector) orders.getVector(6));
            org.apache.arrow.vector.IntVector orders_vc_7 = ((org.apache.arrow.vector.IntVector) orders.getVector(7));
            org.apache.arrow.vector.VarCharVector orders_vc_8 = ((org.apache.arrow.vector.VarCharVector) orders.getVector(8));
            int ordinal_4_val_mask_length = VectorisedFilterOperators.ltSIMD(orders_vc_4, 9204, ordinal_4_val_mask);
            // DIFF: removed mask
            VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector_0, orders_vc_1, false);
            int recordCount = orders_vc_1.getValueCount();
            int currentLoopIndex = 0;
            while ((currentLoopIndex < recordCount)) {
                int currentResultIndex = 0;
                while ((currentLoopIndex < recordCount)) {
                    if (!(ordinal_4_val_mask[currentLoopIndex])) {
                        currentLoopIndex++;
                        continue;
                    }
                    int right_join_key = orders_vc_1.get(currentLoopIndex);
                    long right_join_key_pre_hash = pre_hash_vector_0[currentLoopIndex];
                    int records_to_join_index = join_map_0.getIndex(right_join_key, right_join_key_pre_hash);
                    if ((records_to_join_index == -1)) {
                        currentLoopIndex++;
                        continue;
                    }
                    int left_join_record_count = join_map_0.keysRecordCount[records_to_join_index];
                    if ((left_join_record_count > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))) {
                        break;
                    }
                    int right_join_ord_0 = orders_vc_0.get(currentLoopIndex);
                    int right_join_ord_2 = orders_vc_4.get(currentLoopIndex);
                    int right_join_ord_3 = orders_vc_7.get(currentLoopIndex);
                    for (int i = 0; i < left_join_record_count; i++) {
                        join_result_vector_ord_0_0[currentResultIndex] = join_map_0.values_record_ord_0[records_to_join_index][i];
                        join_result_vector_ord_1_0[currentResultIndex] = right_join_ord_0;
                        join_result_vector_ord_2_0[currentResultIndex] = right_join_key;
                        join_result_vector_ord_3_0[currentResultIndex] = right_join_ord_2;
                        join_result_vector_ord_4_0[currentResultIndex] = right_join_ord_3;
                        currentResultIndex++;
                    }
                    currentLoopIndex++;
                }
                VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector, join_result_vector_ord_1_0, currentResultIndex, false);
                for (int i_0 = 0; i_0 < currentResultIndex; i_0++) {
                    int left_join_record_key = join_result_vector_ord_1_0[i_0];
                    join_map.associate(left_join_record_key, pre_hash_vector[i_0], left_join_record_key, join_result_vector_ord_3_0[i_0], join_result_vector_ord_4_0[i_0]);
                }
            }
        }
        // DIFF: hard-coded allocation manager
        this.allocationManager.release(pre_hash_vector_0);
        this.allocationManager.release(join_result_vector_ord_0_0);
        this.allocationManager.release(join_result_vector_ord_1_0);
        this.allocationManager.release(join_result_vector_ord_2_0);
        this.allocationManager.release(join_result_vector_ord_3_0);
        this.allocationManager.release(join_result_vector_ord_4_0);
        int[] join_result_vector_ord_0 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_1 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_2 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_3 = this.allocationManager.getIntVector();
        double[] join_result_vector_ord_4 = this.allocationManager.getDoubleVector();
        // DIFF: hard-coded
        // ArrowTableReader lineitem = cCtx.getArrowReader(2);
        while (lineitem.loadNextBatch()) {
            org.apache.arrow.vector.IntVector lineitem_vc_0 = ((org.apache.arrow.vector.IntVector) lineitem.getVector(0));
            org.apache.arrow.vector.IntVector lineitem_vc_1 = ((org.apache.arrow.vector.IntVector) lineitem.getVector(1));
            org.apache.arrow.vector.IntVector lineitem_vc_2 = ((org.apache.arrow.vector.IntVector) lineitem.getVector(2));
            org.apache.arrow.vector.IntVector lineitem_vc_3 = ((org.apache.arrow.vector.IntVector) lineitem.getVector(3));
            org.apache.arrow.vector.Float8Vector lineitem_vc_4 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_5 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_6 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(6));
            org.apache.arrow.vector.Float8Vector lineitem_vc_7 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(7));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_8 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(8));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_9 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(9));
            org.apache.arrow.vector.DateDayVector lineitem_vc_10 = ((org.apache.arrow.vector.DateDayVector) lineitem.getVector(10));
            org.apache.arrow.vector.DateDayVector lineitem_vc_11 = ((org.apache.arrow.vector.DateDayVector) lineitem.getVector(11));
            org.apache.arrow.vector.DateDayVector lineitem_vc_12 = ((org.apache.arrow.vector.DateDayVector) lineitem.getVector(12));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_13 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(13));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_14 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(14));
            org.apache.arrow.vector.VarCharVector lineitem_vc_15 = ((org.apache.arrow.vector.VarCharVector) lineitem.getVector(15));
            int ordinal_10_val_mask_length = VectorisedFilterOperators.gtSIMD(lineitem_vc_10, 9204, ordinal_10_val_mask);
            int projection_literal = 1;
            int projection_computation_result_length;
            // DIFF: removed mask
            projection_computation_result_length = VectorisedArithmeticOperators.subtractSIMD(projection_literal, lineitem_vc_6, projection_computation_result);
            int projection_computation_result_0_length;
            // DIFF: removed mask
            projection_computation_result_0_length = VectorisedArithmeticOperators.multiplySIMD(lineitem_vc_5, projection_computation_result, projection_computation_result_length, projection_computation_result_0);
            // DIFF: removed mask
            VectorisedHashOperators.constructPreHashKeyVectorSIMD(pre_hash_vector, lineitem_vc_0, false);
            int recordCount = lineitem_vc_0.getValueCount();
            int currentLoopIndex = 0;
            while ((currentLoopIndex < recordCount)) {
                int currentResultIndex = 0;
                while ((currentLoopIndex < recordCount)) {
                    if (!(ordinal_10_val_mask[currentLoopIndex])) {
                        currentLoopIndex++;
                        continue;
                    }
                    int right_join_key = lineitem_vc_0.get(currentLoopIndex);
                    long right_join_key_pre_hash = pre_hash_vector[currentLoopIndex];
                    int records_to_join_index = join_map.getIndex(right_join_key, right_join_key_pre_hash);
                    if ((records_to_join_index == -1)) {
                        currentLoopIndex++;
                        continue;
                    }
                    int left_join_record_count = join_map.keysRecordCount[records_to_join_index];
                    if ((left_join_record_count > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))) {
                        break;
                    }
                    double right_join_ord_1 = projection_computation_result_0[currentLoopIndex];
                    for (int i = 0; i < left_join_record_count; i++) {
                        join_result_vector_ord_0[currentResultIndex] = join_map.values_record_ord_0[records_to_join_index][i];
                        join_result_vector_ord_1[currentResultIndex] = join_map.values_record_ord_1[records_to_join_index][i];
                        join_result_vector_ord_2[currentResultIndex] = join_map.values_record_ord_2[records_to_join_index][i];
                        join_result_vector_ord_3[currentResultIndex] = right_join_key;
                        join_result_vector_ord_4[currentResultIndex] = right_join_ord_1;
                        currentResultIndex++;
                    }
                    currentLoopIndex++;
                }
                VectorisedHashOperators.constructPreHashKeyVectorSIMD(groupKeyPreHashVector, join_result_vector_ord_3, currentResultIndex, false);
                VectorisedHashOperators.constructPreHashKeyVectorSIMD(groupKeyPreHashVector, join_result_vector_ord_1, currentResultIndex, true);
                VectorisedHashOperators.constructPreHashKeyVectorSIMD(groupKeyPreHashVector, join_result_vector_ord_2, currentResultIndex, true);
                int recordCount_0 = currentResultIndex;
                for (int aviv = 0; aviv < recordCount_0; aviv++) {
                    aggregation_state_map.incrementForKey(join_result_vector_ord_3[aviv], join_result_vector_ord_1[aviv], join_result_vector_ord_2[aviv], groupKeyPreHashVector[aviv], join_result_vector_ord_4[aviv]);
                }
            }
        }
        // DIFF: hard-coded allocation manager
        this.allocationManager.release(pre_hash_vector);
        this.allocationManager.release(join_result_vector_ord_0);
        this.allocationManager.release(join_result_vector_ord_1);
        this.allocationManager.release(join_result_vector_ord_2);
        this.allocationManager.release(join_result_vector_ord_3);
        this.allocationManager.release(join_result_vector_ord_4);
        int aggregationResultVectorLength;
        // DIFF: hard-coded allocation manager
        int[] groupKeyVector_0 = this.allocationManager.getIntVector();
        int[] groupKeyVector_1 = this.allocationManager.getIntVector();
        int[] groupKeyVector_2 = this.allocationManager.getIntVector();
        double[] agg_G_SUM_0_vector = this.allocationManager.getDoubleVector();
        int current_key_offset = 0;
        int number_of_records = aggregation_state_map.numberOfRecords;
        while ((current_key_offset < number_of_records)) {
            aggregationResultVectorLength = VectorisedAggregationOperators.constructVector(groupKeyVector_0, aggregation_state_map.keys_ord_0, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_1, aggregation_state_map.keys_ord_1, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_2, aggregation_state_map.keys_ord_2, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_0_vector, aggregation_state_map.values_ord_0, number_of_records, current_key_offset);
            // DIFF: replaced by result verification
            // VectorisedPrintOperators.print(groupKeyVector_0, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(agg_G_SUM_0_vector, aggregationResultVectorLength);
            // VectorisedPrintOperators.printDate(groupKeyVector_1, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_2, aggregationResultVectorLength);
            // System.out.println();
            System.arraycopy(groupKeyVector_0, 0, this.resultLOrderKey, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(agg_G_SUM_0_vector, 0, this.resultRevenue, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_1, 0, this.resultOOrderDate, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_2, 0, this.resultOShippriority, current_key_offset, aggregationResultVectorLength);
            // DIFF: moved around
            current_key_offset += aggregationResultVectorLength;
        }
        // DIFF: hard-coded allocation manager
        this.allocationManager.release(groupKeyVector_0);
        this.allocationManager.release(groupKeyVector_1);
        this.allocationManager.release(groupKeyVector_2);
        this.allocationManager.release(agg_G_SUM_0_vector);
        this.allocationManager.release(groupKeyPreHashVector);
        this.allocationManager.release(ordinal_6_val_mask);
        this.allocationManager.release(ordinal_4_val_mask);
        this.allocationManager.release(ordinal_10_val_mask);
        this.allocationManager.release(projection_computation_result);
        this.allocationManager.release(projection_computation_result_0);

        // DIFF: prevent optimising the result away
        bh.consume(this.resultLOrderKey);
        bh.consume(this.resultRevenue);
        bh.consume(this.resultOOrderDate);
        bh.consume(this.resultOShippriority);
    }

}
