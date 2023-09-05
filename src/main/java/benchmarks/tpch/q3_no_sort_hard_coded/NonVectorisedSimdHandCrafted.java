package benchmarks.tpch.q3_no_sort_hard_coded;

import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.general_support.hashmaps.Int_Hash_Function;
import jdk.incubator.vector.VectorOperators;
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
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates what the performance of AethraDB could look like when it could generate
 * non-vectorised query code with SIMD-ed operators, for this query.
 */
@State(Scope.Benchmark)
public class NonVectorisedSimdHandCrafted {

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

        // Initialise the hash-table
        this.aggregation_state_map = new AggregationMap();

        // Initialise the join maps
        this.join_map = new OuterMostJoinMap();
        this.join_map_0 = new InnerMostJoinMap();

        // Initialise the result verifier
        this.resultVerifier = new ResultVerifier(this.tpchInstance + "/q3_result.csv");

        // Initialise the result
        int resultSize = this.resultVerifier.getResultSize();
        this.resultLOrderKey = new int[resultSize];
        Arrays.fill(this.resultLOrderKey, -1);
        this.resultRevenue = new double[resultSize];
        Arrays.fill(this.resultRevenue, -1d);
        this.resultOOrderDate = new int[resultSize];
        Arrays.fill(this.resultOOrderDate, -1);
        this.resultOShippriority = new int[resultSize];
        Arrays.fill(this.resultOShippriority, -1);
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
            "-Xmx32g",
            "-Xms16g"
    })
    public void executeQuery(Blackhole bh) throws IOException {
        // Based on NonVectorisedNonSimd generated code
        // Manual optimisation
        jdk.incubator.vector.VectorSpecies<Long> LongVectorSpecies = OptimisationContext.getVectorSpeciesLong();
        jdk.incubator.vector.VectorSpecies<Integer> IntVectorSpecies = OptimisationContext.getVectorSpeciesInt();
        jdk.incubator.vector.VectorSpecies<Double> DoubleVectorSpecies = OptimisationContext.getVectorSpeciesDouble();
        // End manual optimisation

        // DIFF: not necessary
        // java.time.format.DateTimeFormatter dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");
        // java.time.LocalDate day_zero = java.time.LocalDate.parse("1970-01-01", dateTimeFormatter);

        // DIFF: hard-coded
        // KeyValueMap_731979931 aggregation_state_map = new KeyValueMap_731979931();
        // KeyMultiRecordMap_697001207 join_map = new KeyMultiRecordMap_697001207();
        // KeyMultiRecordMap_388005723 join_map_0 = new KeyMultiRecordMap_388005723();

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
            int recordCount = customer_vc_0.getValueCount();

            // Manual optimisation: perform SIMD-ed hashing
            MemorySegment col_0_ms = MemorySegment.ofAddress(customer_vc_0.getDataBufferAddress(), recordCount * customer_vc_0.TYPE_WIDTH);
            boolean[] passesSegmentPredicate = new boolean[LongVectorSpecies.length()];
            long[] preHashValues = new long[LongVectorSpecies.length()];

            for (int currentIndex = 0; currentIndex < recordCount; currentIndex += LongVectorSpecies.length()) {
                Arrays.fill(passesSegmentPredicate, false);

                // We cannot SIMD the array equals call, so we perform that manually
                int iterationLength = Math.min(LongVectorSpecies.length(), (recordCount - currentIndex));
                for (int i = 0; i < iterationLength; i++) {
                    int aviv = currentIndex + i;
                    byte[] ordinal_value = customer_vc_6.get(aviv);
                    passesSegmentPredicate[i] = (Arrays.equals(ordinal_value, new byte[] { 66, 85, 73, 76, 68, 73, 78, 71, 32, 32 }));
                }

                // Now perform the SIMD-ed hashing
                jdk.incubator.vector.VectorMask<Long> inRangeSIMDMask = LongVectorSpecies.indexInRange(currentIndex, recordCount);
                jdk.incubator.vector.VectorMask<Long> applicationMask = LongVectorSpecies.loadMask(passesSegmentPredicate, 0).and(inRangeSIMDMask);
                jdk.incubator.vector.VectorMask<Integer> applicationMaskInteger = applicationMask.cast(IntVectorSpecies);

                jdk.incubator.vector.IntVector col_0_SIMDVector = jdk.incubator.vector.IntVector.fromMemorySegment(IntVectorSpecies, col_0_ms, currentIndex * customer_vc_0.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, applicationMaskInteger);
                jdk.incubator.vector.LongVector col_0_SIMDVector_Long = (jdk.incubator.vector.LongVector) col_0_SIMDVector.castShape(LongVectorSpecies, 0);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_vector = col_0_SIMDVector_Long.mul(Int_Hash_Function.hashConstantA, applicationMask);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_plus_b_vector = SIMD_a_mul_key_vector.add(Int_Hash_Function.hashConstantB, applicationMask);
                SIMD_a_mul_key_plus_b_vector.intoArray(preHashValues, 0, applicationMask);

                // Insert the passing records into the join map
                for (int i = 0; i < iterationLength; i++) {
                    if (passesSegmentPredicate[i]) {
                        int flattened_key = customer_vc_0.get(currentIndex + i);
                        long pre_hash_value = preHashValues[i] % Int_Hash_Function.hashConstantP;
                        join_map_0.associate(flattened_key, pre_hash_value, flattened_key);
                    }
                }
            }
            // End manual optimisation
        }
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
            int recordCount = orders_vc_0.getValueCount();

            // Manual optimisation: perform SIMDed filtering and hashing
            MemorySegment col_4_ms = MemorySegment.ofAddress(orders_vc_4.getDataBufferAddress(), recordCount * orders_vc_4.TYPE_WIDTH);
            MemorySegment col_1_ms = MemorySegment.ofAddress(orders_vc_1.getDataBufferAddress(), recordCount * orders_vc_1.TYPE_WIDTH);
            boolean[] passesDateFilter = new boolean[IntVectorSpecies.length()];
            long[] preHashValues = new long[IntVectorSpecies.length()];

            for (int currentIndex = 0; currentIndex < recordCount; currentIndex += IntVectorSpecies.length()) {
                // First perform SIMDed filtering
                jdk.incubator.vector.VectorMask<Integer> inRangeSIMDMask = IntVectorSpecies.indexInRange(currentIndex, recordCount);
                jdk.incubator.vector.IntVector col_4_SIMDVector = jdk.incubator.vector.IntVector.fromMemorySegment(IntVectorSpecies, col_4_ms, currentIndex * orders_vc_4.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                jdk.incubator.vector.VectorMask<Integer> col_4_SIMDVector_sel_mask = col_4_SIMDVector.compare(VectorOperators.LT, 9204, inRangeSIMDMask);
                col_4_SIMDVector_sel_mask.intoArray(passesDateFilter, 0);

                // Now perform SIMDed hashing
                jdk.incubator.vector.VectorMask<Long> col_4_SIMDVector_sel_mask_long = col_4_SIMDVector_sel_mask.cast(LongVectorSpecies);
                jdk.incubator.vector.IntVector col_1_SIMDVector = jdk.incubator.vector.IntVector.fromMemorySegment(IntVectorSpecies, col_1_ms, currentIndex * orders_vc_1.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, col_4_SIMDVector_sel_mask);
                jdk.incubator.vector.LongVector col_1_SIMDVector_Long = (jdk.incubator.vector.LongVector) col_1_SIMDVector.castShape(LongVectorSpecies, 0);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_vector = col_1_SIMDVector_Long.mul(Int_Hash_Function.hashConstantA, col_4_SIMDVector_sel_mask_long);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_plus_b_vector = SIMD_a_mul_key_vector.add(Int_Hash_Function.hashConstantB, col_4_SIMDVector_sel_mask_long);
                SIMD_a_mul_key_plus_b_vector.intoArray(preHashValues, 0, col_4_SIMDVector_sel_mask_long);

                // Look up records in the join map, and insert them into the new join map
                for (int i = 0; i < IntVectorSpecies.length(); i++) {
                    if (passesDateFilter[i]) {
                        int ordinal_value_0 = orders_vc_1.get(currentIndex + i);
                        long right_join_key_prehash = preHashValues[i] % Int_Hash_Function.hashConstantP;
                        int records_to_join_index = join_map_0.getIndex(ordinal_value_0, right_join_key_prehash);
                        if ((records_to_join_index == -1)) {
                            continue;
                        }
                        int ordinal_value = orders_vc_4.get(currentIndex + i);
                        int ordinal_value_1 = orders_vc_0.get(currentIndex + i);
                        int ordinal_value_2 = orders_vc_7.get(currentIndex + i);
                        int left_join_record_count = join_map_0.keysRecordCount[records_to_join_index];
                        for (int j = 0; j < left_join_record_count; j++) {
                            int left_join_ord_0 = join_map_0.values_record_ord_0[records_to_join_index][j];
                            // TODO: did not SIMD the below call to preHash since this has little odds of being advantageous, but is a bit challenging
                            long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_1);
                            join_map.associate(ordinal_value_1, left_join_key_prehash, ordinal_value_1, ordinal_value, ordinal_value_2);
                        }
                    }
                }
            }
            // End manual optimisation
        }
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
            int recordCount = lineitem_vc_0.getValueCount();

            // Manual optimisation: perform SIMDed filtering, arithmetic and hashing
            MemorySegment col_10_ms = MemorySegment.ofAddress(lineitem_vc_10.getDataBufferAddress(), recordCount * lineitem_vc_10.TYPE_WIDTH);
            MemorySegment col_6_ms = MemorySegment.ofAddress(lineitem_vc_6.getDataBufferAddress(), recordCount * lineitem_vc_6.TYPE_WIDTH);
            MemorySegment col_5_ms = MemorySegment.ofAddress(lineitem_vc_5.getDataBufferAddress(), recordCount * lineitem_vc_5.TYPE_WIDTH);
            MemorySegment col_0_ms = MemorySegment.ofAddress(lineitem_vc_0.getDataBufferAddress(), recordCount * lineitem_vc_0.TYPE_WIDTH);
            boolean[] passesDateFilter = new boolean[IntVectorSpecies.length()];
            double[] projection_computation_result_0_array = new double[IntVectorSpecies.length()];
            long[] preHashValues = new long[IntVectorSpecies.length()];

            for (int currentIndex = 0; currentIndex < recordCount; currentIndex += IntVectorSpecies.length()) {
                // Perform SIMDed filtering
                jdk.incubator.vector.VectorMask<Integer> inRangeSIMDMask = IntVectorSpecies.indexInRange(currentIndex, recordCount);
                jdk.incubator.vector.IntVector col_10_SIMDVector = jdk.incubator.vector.IntVector.fromMemorySegment(IntVectorSpecies, col_10_ms, currentIndex * lineitem_vc_10.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                jdk.incubator.vector.VectorMask<Integer> col_10_SIMDVector_sel_mask = col_10_SIMDVector.compare(VectorOperators.GT, 9204, inRangeSIMDMask);
                col_10_SIMDVector_sel_mask.intoArray(passesDateFilter, 0);

                // Perform SIMDed arithmetic
                jdk.incubator.vector.VectorMask<Double> applicationMask = col_10_SIMDVector_sel_mask.cast(DoubleVectorSpecies);
                jdk.incubator.vector.DoubleVector projectionLiteral = jdk.incubator.vector.DoubleVector.broadcast(DoubleVectorSpecies, 1d);
                jdk.incubator.vector.DoubleVector col_6_SIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(DoubleVectorSpecies, col_6_ms, currentIndex * lineitem_vc_6.TYPE_WIDTH, ByteOrder.LITTLE_ENDIAN, applicationMask);
                jdk.incubator.vector.DoubleVector projection_computation_result = projectionLiteral.sub(col_6_SIMDVector, applicationMask);
                jdk.incubator.vector.DoubleVector col_5_SIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(DoubleVectorSpecies, col_5_ms, currentIndex * lineitem_vc_5.TYPE_WIDTH, ByteOrder.LITTLE_ENDIAN, applicationMask);
                jdk.incubator.vector.DoubleVector projection_computation_result_0 = col_5_SIMDVector.mul(projection_computation_result, applicationMask);
                projection_computation_result_0.intoArray(projection_computation_result_0_array, 0, applicationMask);

                // Perform SIMDed hashing
                jdk.incubator.vector.VectorMask<Long> col_10_SIMDVector_sel_mask_long = col_10_SIMDVector_sel_mask.cast(LongVectorSpecies);
                jdk.incubator.vector.IntVector col_0_SIMDVector = jdk.incubator.vector.IntVector.fromMemorySegment(IntVectorSpecies, col_0_ms, currentIndex * lineitem_vc_0.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, col_10_SIMDVector_sel_mask);
                jdk.incubator.vector.LongVector col_0_SIMDVector_Long = (jdk.incubator.vector.LongVector) col_0_SIMDVector.castShape(LongVectorSpecies, 0);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_vector = col_0_SIMDVector_Long.mul(Int_Hash_Function.hashConstantA, col_10_SIMDVector_sel_mask_long);
                jdk.incubator.vector.LongVector SIMD_a_mul_key_plus_b_vector = SIMD_a_mul_key_vector.add(Int_Hash_Function.hashConstantB, col_10_SIMDVector_sel_mask_long);
                SIMD_a_mul_key_plus_b_vector.intoArray(preHashValues, 0, col_10_SIMDVector_sel_mask_long);

                // Look up records in the join map, and insert them into the aggregation map
                for (int i = 0; i < IntVectorSpecies.length(); i++) {
                    if (passesDateFilter[i]) {
                        int ordinal_value_0 = lineitem_vc_0.get(currentIndex + i);
                        long right_join_key_prehash = preHashValues[i] % Int_Hash_Function.hashConstantP;
                        int records_to_join_index = join_map.getIndex(ordinal_value_0, right_join_key_prehash);
                        if ((records_to_join_index == -1)) {
                            continue;
                        }
                        int left_join_record_count = join_map.keysRecordCount[records_to_join_index];
                        for (int j = 0; j < left_join_record_count; j++) {
                            int left_join_ord_0 = join_map.values_record_ord_0[records_to_join_index][j];
                            int left_join_ord_1 = join_map.values_record_ord_1[records_to_join_index][j];
                            int left_join_ord_2 = join_map.values_record_ord_2[records_to_join_index][j];
                            // TODO: did not SIMD the below call to preHash since this has little odds of being advantageous, but is a bit challenging
                            long group_key_pre_hash = Int_Hash_Function.preHash(ordinal_value_0);
                            group_key_pre_hash ^= Int_Hash_Function.preHash(left_join_ord_1);
                            group_key_pre_hash ^= Int_Hash_Function.preHash(left_join_ord_2);
                            aggregation_state_map.incrementForKey(ordinal_value_0, left_join_ord_1, left_join_ord_2, group_key_pre_hash, projection_computation_result_0_array[i]);
                        }
                    }
                }
            }

            // End manual optimisation
        }
        for (int key_i = 0; key_i < aggregation_state_map.numberOfRecords; key_i++) {
            int groupKey_0 = aggregation_state_map.keys_ord_0[key_i];
            int groupKey_1 = aggregation_state_map.keys_ord_1[key_i];
            int groupKey_2 = aggregation_state_map.keys_ord_2[key_i];
            double aggregation_0_value = aggregation_state_map.values_ord_0[key_i];
            // DIFF: replaced by result verification
            // System.out.print((groupKey_0 + ", "));
            // System.out.print((String.format("%.2f", aggregation_0_value) + ", "));
            // System.out.print((day_zero.plusDays(groupKey_1) + ", "));
            // System.out.println(groupKey_2);
            this.resultLOrderKey[key_i] = groupKey_0;
            this.resultRevenue[key_i] = aggregation_0_value;
            this.resultOOrderDate[key_i] = groupKey_1;
            this.resultOShippriority[key_i] = groupKey_2;
        }

        // DIFF: prevent optimising the result away
        bh.consume(this.resultLOrderKey);
        bh.consume(this.resultRevenue);
        bh.consume(this.resultOOrderDate);
        bh.consume(this.resultOShippriority);
    }

}
