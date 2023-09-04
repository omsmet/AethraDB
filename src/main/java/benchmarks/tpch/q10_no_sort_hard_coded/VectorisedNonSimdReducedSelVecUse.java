package benchmarks.tpch.q10_no_sort_hard_coded;

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
 * generation without SIMD-ed operators, but while not actually invoking the code generator itself.
 * This specific benchmark does not use selection vectors in arithmetic operations whenever possible.
 */
@State(Scope.Benchmark)
public class VectorisedNonSimdReducedSelVecUse {

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
     * State: the {@link AllocationManager} used for query processing.
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
     * State: the {@link ArrowTableReader} used for reading the nation table.
     */
    private ArrowTableReader nation;

    /**
     * State: the hash-table which is used by the query to aggregate.
     */
    private AggregationMap aggregation_state_map;

    /**
     * State: the hash-table which is used for one of the joins.
     */
    private JoinMapType join_map;

    /**
     * State: the hash-table which is used for one of the joins.
     */
    private JoinMapType0 join_map_0;

    /**
     * State: the hash-table which is used for one of the joins.
     */
    private JoinMapType1 join_map_1;

    /**
     * State: the result of the query.
     */
    private int[] resultCustKey;
    private byte[][] resultCName;
    private double[] resultRevenue;
    private double[] resultCAcctbal;
    private byte[][] resultNName;
    private byte[][] resultCAddress;
    private byte[][] resultCPhone;
    private byte[][] resultCComment;

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
        this.nation = new ABQArrowTableReader(new File(this.tpchInstance + "/nation.arrow"), this.rootAllocator);

        // Setup the allocation manager
        this.allocationManager = new BufferPoolAllocationManager(32);

        // Initialise the hash-table
        this.aggregation_state_map = new AggregationMap();

        // Initialise the join maps
        this.join_map = new JoinMapType();
        this.join_map_0 = new JoinMapType0();
        this.join_map_1 = new JoinMapType1();

        // Initialise the result
        int resultSize = 37967;
        this.resultCustKey = new int[resultSize];
        this.resultCName = new byte[resultSize][];
        this.resultRevenue = new double[resultSize];
        this.resultCAcctbal = new double[resultSize];
        this.resultNName = new byte[resultSize][];
        this.resultCAddress = new byte[resultSize][];
        this.resultCPhone = new byte[resultSize][];
        this.resultCComment = new byte[resultSize][];

        Arrays.fill(this.resultCustKey, -1);
        Arrays.fill(this.resultCName, null);
        Arrays.fill(this.resultRevenue, -1d);
        Arrays.fill(this.resultCAcctbal, -1d);
        Arrays.fill(this.resultNName, null);
        Arrays.fill(this.resultCAddress, null);
        Arrays.fill(this.resultCPhone, null);
        Arrays.fill(this.resultCComment, null);

        // Initialise the result verifier
        this.resultVerifier = new ResultVerifier(this.tpchInstance + "/q10_result.csv");
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
        this.nation.reset();

        // Perform allocation manager maintenance
        this.allocationManager.performMaintenance();

        // Reset the aggregation map
        this.aggregation_state_map.reset();

        // Reset the join maps
        this.join_map.reset();
        this.join_map_0.reset();
        this.join_map_1.reset();
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void teardown() {
        // Verify the result
        if (!this.resultVerifier.resultCorrect(
                this.resultCustKey,
                this.resultCName,
                this.resultRevenue,
                this.resultCAcctbal,
                this.resultNName,
                this.resultCAddress,
                this.resultCPhone,
                this.resultCComment)
        ) {
            throw new RuntimeException("The computed result is incorrect");
        }

        // Reset the result after verifying it
        Arrays.fill(this.resultCustKey, -1);
        Arrays.fill(this.resultCName, null);
        Arrays.fill(this.resultRevenue, -1d);
        Arrays.fill(this.resultCAcctbal, -1d);
        Arrays.fill(this.resultNName, null);
        Arrays.fill(this.resultCAddress, null);
        Arrays.fill(this.resultCPhone, null);
        Arrays.fill(this.resultCComment, null);
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
    public void executeQuery(Blackhole bh) throws IOException {
        // DIFF: all allocation manager references are hard-coded
        int[] ordinal_4_sel_vec = this.allocationManager.getIntVector();
        int[] ordinal_4_sel_vec_0 = this.allocationManager.getIntVector();
        int[] ordinal_8_sel_vec = this.allocationManager.getIntVector();
        double[] projection_computation_result = this.allocationManager.getDoubleVector();
        double[] projection_computation_result_0 = this.allocationManager.getDoubleVector();
        long[] groupKeyPreHashVector = this.allocationManager.getLongVector();

        // DIFF: hard-coded
        // KeyValueMap_1694102613 aggregation_state_map = new KeyValueMap_1694102613();
        // KeyMultiRecordMap_1406763631 join_map = new KeyMultiRecordMap_1406763631();
        // KeyMultiRecordMap_1082640380 join_map_1 = new KeyMultiRecordMap_1082640380();

        long[] pre_hash_vector = this.allocationManager.getLongVector();
        long[] pre_hash_vector_0 = this.allocationManager.getLongVector();
        long[] pre_hash_vector_1 = this.allocationManager.getLongVector();

        // DIFF: hard-coded
        // ArrowTableReader customer = cCtx.getArrowReader(0);
        while (customer.loadNextBatch()) {
            org.apache.arrow.vector.IntVector customer_vc_0 = ((org.apache.arrow.vector.IntVector) customer.getVector(0));
            org.apache.arrow.vector.VarCharVector customer_vc_1 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(1));
            org.apache.arrow.vector.VarCharVector customer_vc_2 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(2));
            org.apache.arrow.vector.IntVector customer_vc_3 = ((org.apache.arrow.vector.IntVector) customer.getVector(3));
            org.apache.arrow.vector.FixedSizeBinaryVector customer_vc_4 = ((org.apache.arrow.vector.FixedSizeBinaryVector) customer.getVector(4));
            org.apache.arrow.vector.Float8Vector customer_vc_5 = ((org.apache.arrow.vector.Float8Vector) customer.getVector(5));
            org.apache.arrow.vector.VarCharVector customer_vc_6 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(7));
            VectorisedHashOperators.constructPreHashKeyVector(pre_hash_vector_1, customer_vc_0, false);
            int recordCount = customer_vc_0.getValueCount();
            for (int i = 0; i < recordCount; i++) {
                int left_join_record_key = customer_vc_0.get(i);
                join_map_1.associate(left_join_record_key, pre_hash_vector_1[i], left_join_record_key, customer_vc_1.get(i), customer_vc_2.get(i), customer_vc_3.get(i), customer_vc_4.get(i), customer_vc_5.get(i), customer_vc_6.get(i));
            }
        }
        int[] join_result_vector_ord_0_1 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_1_1 = this.allocationManager.getNestedByteVector();
        byte[][] join_result_vector_ord_2_1 = this.allocationManager.getNestedByteVector();
        int[] join_result_vector_ord_3_1 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_4_1 = this.allocationManager.getNestedByteVector();
        double[] join_result_vector_ord_5_1 = this.allocationManager.getDoubleVector();
        byte[][] join_result_vector_ord_6_1 = this.allocationManager.getNestedByteVector();
        int[] join_result_vector_ord_7_1 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_8_1 = this.allocationManager.getIntVector();
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
            int ordinal_4_sel_vec_length = VectorisedFilterOperators.ge(orders_vc_4, 8674, ordinal_4_sel_vec);
            int ordinal_4_sel_vec_0_length = VectorisedFilterOperators.lt(orders_vc_4, 8766, ordinal_4_sel_vec_0, ordinal_4_sel_vec, ordinal_4_sel_vec_length);
            // DIFF: removed selection vector
            VectorisedHashOperators.constructPreHashKeyVector(pre_hash_vector_1, orders_vc_1, false);
            int recordCount = ordinal_4_sel_vec_0_length;
            int currentLoopIndex = 0;
            while ((currentLoopIndex < recordCount)) {
                int currentResultIndex = 0;
                while ((currentLoopIndex < recordCount)) {
                    int selected_record_index = ordinal_4_sel_vec_0[currentLoopIndex];
                    int right_join_key = orders_vc_1.get(selected_record_index);
                    long right_join_key_pre_hash = pre_hash_vector_1[selected_record_index];
                    int records_to_join_index = join_map_1.getIndex(right_join_key, right_join_key_pre_hash);
                    if ((records_to_join_index == -1)) {
                        currentLoopIndex++;
                        continue;
                    }
                    int left_join_record_count = join_map_1.keysRecordCount[records_to_join_index];
                    if ((left_join_record_count > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))) {
                        break;
                    }
                    int right_join_ord_0 = orders_vc_0.get(selected_record_index);
                    for (int i = 0; i < left_join_record_count; i++) {
                        join_result_vector_ord_0_1[currentResultIndex] = join_map_1.values_record_ord_0[records_to_join_index][i];
                        join_result_vector_ord_1_1[currentResultIndex] = join_map_1.values_record_ord_1[records_to_join_index][i];
                        join_result_vector_ord_2_1[currentResultIndex] = join_map_1.values_record_ord_2[records_to_join_index][i];
                        join_result_vector_ord_3_1[currentResultIndex] = join_map_1.values_record_ord_3[records_to_join_index][i];
                        join_result_vector_ord_4_1[currentResultIndex] = join_map_1.values_record_ord_4[records_to_join_index][i];
                        join_result_vector_ord_5_1[currentResultIndex] = join_map_1.values_record_ord_5[records_to_join_index][i];
                        join_result_vector_ord_6_1[currentResultIndex] = join_map_1.values_record_ord_6[records_to_join_index][i];
                        join_result_vector_ord_7_1[currentResultIndex] = right_join_ord_0;
                        join_result_vector_ord_8_1[currentResultIndex] = right_join_key;
                        currentResultIndex++;
                    }
                    currentLoopIndex++;
                }
                VectorisedHashOperators.constructPreHashKeyVector(pre_hash_vector_0, join_result_vector_ord_7_1, currentResultIndex, false);
                for (int i_0 = 0; i_0 < currentResultIndex; i_0++) {
                    int left_join_record_key = join_result_vector_ord_7_1[i_0];
                    join_map_0.associate(left_join_record_key, pre_hash_vector_0[i_0], join_result_vector_ord_0_1[i_0], join_result_vector_ord_1_1[i_0], join_result_vector_ord_2_1[i_0], join_result_vector_ord_3_1[i_0], join_result_vector_ord_4_1[i_0], join_result_vector_ord_5_1[i_0], join_result_vector_ord_6_1[i_0], left_join_record_key);
                }
            }
        }
        this.allocationManager.release(pre_hash_vector_1);
        this.allocationManager.release(join_result_vector_ord_0_1);
        this.allocationManager.release(join_result_vector_ord_1_1);
        this.allocationManager.release(join_result_vector_ord_2_1);
        this.allocationManager.release(join_result_vector_ord_3_1);
        this.allocationManager.release(join_result_vector_ord_4_1);
        this.allocationManager.release(join_result_vector_ord_5_1);
        this.allocationManager.release(join_result_vector_ord_6_1);
        this.allocationManager.release(join_result_vector_ord_7_1);
        this.allocationManager.release(join_result_vector_ord_8_1);
        int[] join_result_vector_ord_0_0 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_1_0 = this.allocationManager.getNestedByteVector();
        byte[][] join_result_vector_ord_2_0 = this.allocationManager.getNestedByteVector();
        int[] join_result_vector_ord_3_0 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_4_0 = this.allocationManager.getNestedByteVector();
        double[] join_result_vector_ord_5_0 = this.allocationManager.getDoubleVector();
        byte[][] join_result_vector_ord_6_0 = this.allocationManager.getNestedByteVector();
        int[] join_result_vector_ord_7_0 = this.allocationManager.getIntVector();
        int[] join_result_vector_ord_8_0 = this.allocationManager.getIntVector();
        double[] join_result_vector_ord_9_0 = this.allocationManager.getDoubleVector();
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
            int ordinal_8_sel_vec_length = VectorisedFilterOperators.eq(lineitem_vc_8, new byte[] { 82 }, ordinal_8_sel_vec);
            int projection_literal = 1;
            int projection_computation_result_length;
            // DIFF: removed selection vector
            projection_computation_result_length = VectorisedArithmeticOperators.subtract(projection_literal, lineitem_vc_6, projection_computation_result);
            int projection_computation_result_0_length;
            // DIFF: removed selection vector
            projection_computation_result_0_length = VectorisedArithmeticOperators.multiply(lineitem_vc_5, projection_computation_result, projection_computation_result_length, projection_computation_result_0);
            // DIFF: removed selection vector
            VectorisedHashOperators.constructPreHashKeyVector(pre_hash_vector_0, lineitem_vc_0, false);
            int recordCount = ordinal_8_sel_vec_length;
            int currentLoopIndex = 0;
            while ((currentLoopIndex < recordCount)) {
                int currentResultIndex = 0;
                while ((currentLoopIndex < recordCount)) {
                    int selected_record_index = ordinal_8_sel_vec[currentLoopIndex];
                    int right_join_key = lineitem_vc_0.get(selected_record_index);
                    long right_join_key_pre_hash = pre_hash_vector_0[selected_record_index];
                    int records_to_join_index = join_map_0.getIndex(right_join_key, right_join_key_pre_hash);
                    if ((records_to_join_index == -1)) {
                        currentLoopIndex++;
                        continue;
                    }
                    int left_join_record_count = join_map_0.keysRecordCount[records_to_join_index];
                    if ((left_join_record_count > (VectorisedOperators.VECTOR_LENGTH - currentResultIndex))) {
                        break;
                    }
                    double right_join_ord_1 = projection_computation_result_0[selected_record_index];
                    for (int i = 0; i < left_join_record_count; i++) {
                        join_result_vector_ord_0_0[currentResultIndex] = join_map_0.values_record_ord_0[records_to_join_index][i];
                        join_result_vector_ord_1_0[currentResultIndex] = join_map_0.values_record_ord_1[records_to_join_index][i];
                        join_result_vector_ord_2_0[currentResultIndex] = join_map_0.values_record_ord_2[records_to_join_index][i];
                        join_result_vector_ord_3_0[currentResultIndex] = join_map_0.values_record_ord_3[records_to_join_index][i];
                        join_result_vector_ord_4_0[currentResultIndex] = join_map_0.values_record_ord_4[records_to_join_index][i];
                        join_result_vector_ord_5_0[currentResultIndex] = join_map_0.values_record_ord_5[records_to_join_index][i];
                        join_result_vector_ord_6_0[currentResultIndex] = join_map_0.values_record_ord_6[records_to_join_index][i];
                        join_result_vector_ord_7_0[currentResultIndex] = join_map_0.values_record_ord_7[records_to_join_index][i];
                        join_result_vector_ord_8_0[currentResultIndex] = right_join_key;
                        join_result_vector_ord_9_0[currentResultIndex] = right_join_ord_1;
                        currentResultIndex++;
                    }
                    currentLoopIndex++;
                }
                VectorisedHashOperators.constructPreHashKeyVector(pre_hash_vector, join_result_vector_ord_3_0, currentResultIndex, false);
                for (int i_0 = 0; i_0 < currentResultIndex; i_0++) {
                    int left_join_record_key = join_result_vector_ord_3_0[i_0];
                    join_map.associate(left_join_record_key, pre_hash_vector[i_0], join_result_vector_ord_0_0[i_0], join_result_vector_ord_1_0[i_0], join_result_vector_ord_2_0[i_0], left_join_record_key, join_result_vector_ord_4_0[i_0], join_result_vector_ord_5_0[i_0], join_result_vector_ord_6_0[i_0], join_result_vector_ord_9_0[i_0]);
                }
            }
        }
        this.allocationManager.release(pre_hash_vector_0);
        this.allocationManager.release(join_result_vector_ord_0_0);
        this.allocationManager.release(join_result_vector_ord_1_0);
        this.allocationManager.release(join_result_vector_ord_2_0);
        this.allocationManager.release(join_result_vector_ord_3_0);
        this.allocationManager.release(join_result_vector_ord_4_0);
        this.allocationManager.release(join_result_vector_ord_5_0);
        this.allocationManager.release(join_result_vector_ord_6_0);
        this.allocationManager.release(join_result_vector_ord_7_0);
        this.allocationManager.release(join_result_vector_ord_8_0);
        this.allocationManager.release(join_result_vector_ord_9_0);
        int[] join_result_vector_ord_0 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_1 = this.allocationManager.getNestedByteVector();
        byte[][] join_result_vector_ord_2 = this.allocationManager.getNestedByteVector();
        int[] join_result_vector_ord_3 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_4 = this.allocationManager.getNestedByteVector();
        double[] join_result_vector_ord_5 = this.allocationManager.getDoubleVector();
        byte[][] join_result_vector_ord_6 = this.allocationManager.getNestedByteVector();
        double[] join_result_vector_ord_7 = this.allocationManager.getDoubleVector();
        int[] join_result_vector_ord_8 = this.allocationManager.getIntVector();
        byte[][] join_result_vector_ord_9 = this.allocationManager.getNestedByteVector();
        // DIFF: hard-coded
        // ArrowTableReader nation = cCtx.getArrowReader(3);
        while (nation.loadNextBatch()) {
            org.apache.arrow.vector.IntVector nation_vc_0 = ((org.apache.arrow.vector.IntVector) nation.getVector(0));
            org.apache.arrow.vector.FixedSizeBinaryVector nation_vc_1 = ((org.apache.arrow.vector.FixedSizeBinaryVector) nation.getVector(1));
            VectorisedHashOperators.constructPreHashKeyVector(pre_hash_vector, nation_vc_0, false);
            int recordCount = nation_vc_0.getValueCount();
            int currentLoopIndex = 0;
            while ((currentLoopIndex < recordCount)) {
                int currentResultIndex = 0;
                while ((currentLoopIndex < recordCount)) {
                    int right_join_key = nation_vc_0.get(currentLoopIndex);
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
                    byte[] right_join_ord_1 = nation_vc_1.get(currentLoopIndex);
                    for (int i = 0; i < left_join_record_count; i++) {
                        join_result_vector_ord_0[currentResultIndex] = join_map.values_record_ord_0[records_to_join_index][i];
                        join_result_vector_ord_1[currentResultIndex] = join_map.values_record_ord_1[records_to_join_index][i];
                        join_result_vector_ord_2[currentResultIndex] = join_map.values_record_ord_2[records_to_join_index][i];
                        join_result_vector_ord_3[currentResultIndex] = join_map.values_record_ord_3[records_to_join_index][i];
                        join_result_vector_ord_4[currentResultIndex] = join_map.values_record_ord_4[records_to_join_index][i];
                        join_result_vector_ord_5[currentResultIndex] = join_map.values_record_ord_5[records_to_join_index][i];
                        join_result_vector_ord_6[currentResultIndex] = join_map.values_record_ord_6[records_to_join_index][i];
                        join_result_vector_ord_7[currentResultIndex] = join_map.values_record_ord_7[records_to_join_index][i];
                        join_result_vector_ord_8[currentResultIndex] = right_join_key;
                        join_result_vector_ord_9[currentResultIndex] = right_join_ord_1;
                        currentResultIndex++;
                    }
                    currentLoopIndex++;
                }
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_0, currentResultIndex, false);
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_1, currentResultIndex, true);
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_5, currentResultIndex, true);
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_4, currentResultIndex, true);
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_9, currentResultIndex, true);
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_2, currentResultIndex, true);
                VectorisedHashOperators.constructPreHashKeyVector(groupKeyPreHashVector, join_result_vector_ord_6, currentResultIndex, true);
                int recordCount_0 = currentResultIndex;
                for (int aviv = 0; aviv < recordCount_0; aviv++) {
                    aggregation_state_map.incrementForKey(join_result_vector_ord_0[aviv], join_result_vector_ord_1[aviv], join_result_vector_ord_5[aviv], join_result_vector_ord_4[aviv], join_result_vector_ord_9[aviv], join_result_vector_ord_2[aviv], join_result_vector_ord_6[aviv], groupKeyPreHashVector[aviv], join_result_vector_ord_7[aviv]);
                }
            }
        }
        this.allocationManager.release(pre_hash_vector);
        this.allocationManager.release(join_result_vector_ord_0);
        this.allocationManager.release(join_result_vector_ord_1);
        this.allocationManager.release(join_result_vector_ord_2);
        this.allocationManager.release(join_result_vector_ord_3);
        this.allocationManager.release(join_result_vector_ord_4);
        this.allocationManager.release(join_result_vector_ord_5);
        this.allocationManager.release(join_result_vector_ord_6);
        this.allocationManager.release(join_result_vector_ord_7);
        this.allocationManager.release(join_result_vector_ord_8);
        this.allocationManager.release(join_result_vector_ord_9);
        int aggregationResultVectorLength;
        int[] groupKeyVector_0 = this.allocationManager.getIntVector();
        byte[][] groupKeyVector_1 = this.allocationManager.getNestedByteVector();
        double[] groupKeyVector_2 = this.allocationManager.getDoubleVector();
        byte[][] groupKeyVector_3 = this.allocationManager.getNestedByteVector();
        byte[][] groupKeyVector_4 = this.allocationManager.getNestedByteVector();
        byte[][] groupKeyVector_5 = this.allocationManager.getNestedByteVector();
        byte[][] groupKeyVector_6 = this.allocationManager.getNestedByteVector();
        double[] agg_G_SUM_0_vector = this.allocationManager.getDoubleVector();
        int current_key_offset = 0;
        int number_of_records = aggregation_state_map.numberOfRecords;
        while ((current_key_offset < number_of_records)) {
            aggregationResultVectorLength = VectorisedAggregationOperators.constructVector(groupKeyVector_0, aggregation_state_map.keys_ord_0, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_1, aggregation_state_map.keys_ord_1, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_2, aggregation_state_map.keys_ord_2, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_3, aggregation_state_map.keys_ord_3, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_4, aggregation_state_map.keys_ord_4, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_5, aggregation_state_map.keys_ord_5, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(groupKeyVector_6, aggregation_state_map.keys_ord_6, number_of_records, current_key_offset);
            VectorisedAggregationOperators.constructVector(agg_G_SUM_0_vector, aggregation_state_map.values_ord_0, number_of_records, current_key_offset);
            // DIFF: replaced by result verification
            // VectorisedPrintOperators.print(groupKeyVector_0, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_1, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(agg_G_SUM_0_vector, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_2, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_4, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_5, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_3, aggregationResultVectorLength);
            // VectorisedPrintOperators.print(groupKeyVector_6, aggregationResultVectorLength);
            // System.out.println();
            System.arraycopy(groupKeyVector_0, 0, this.resultCustKey, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_1, 0, this.resultCName, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(agg_G_SUM_0_vector, 0, this.resultRevenue, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_2, 0, this.resultCAcctbal, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_4, 0, this.resultNName, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_5, 0, this.resultCAddress, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_3, 0, this.resultCPhone, current_key_offset, aggregationResultVectorLength);
            System.arraycopy(groupKeyVector_6, 0, this.resultCComment, current_key_offset, aggregationResultVectorLength);
            // DIFF: moved around
            current_key_offset += aggregationResultVectorLength;
        }
        this.allocationManager.release(groupKeyVector_0);
        this.allocationManager.release(groupKeyVector_1);
        this.allocationManager.release(groupKeyVector_2);
        this.allocationManager.release(groupKeyVector_3);
        this.allocationManager.release(groupKeyVector_4);
        this.allocationManager.release(groupKeyVector_5);
        this.allocationManager.release(groupKeyVector_6);
        this.allocationManager.release(agg_G_SUM_0_vector);
        this.allocationManager.release(groupKeyPreHashVector);
        this.allocationManager.release(ordinal_4_sel_vec);
        this.allocationManager.release(ordinal_4_sel_vec_0);
        this.allocationManager.release(ordinal_8_sel_vec);
        this.allocationManager.release(projection_computation_result);
        this.allocationManager.release(projection_computation_result_0);

        // DIFF: prevent optimising the result away
        bh.consume(this.resultCustKey);
        bh.consume(this.resultCName);
        bh.consume(this.resultRevenue);
        bh.consume(this.resultCAcctbal);
        bh.consume(this.resultNName);
        bh.consume(this.resultCAddress);
        bh.consume(this.resultCPhone);
        bh.consume(this.resultCComment);
    }

}
