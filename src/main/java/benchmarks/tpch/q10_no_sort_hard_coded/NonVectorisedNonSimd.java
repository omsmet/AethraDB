package benchmarks.tpch.q10_no_sort_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.general_support.ArrowOptimisations;
import evaluation.general_support.hashmaps.Char_Arr_Hash_Function;
import evaluation.general_support.hashmaps.Double_Hash_Function;
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
//            "/nvtmp/AethraTestData/tpch/sf-100",
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
        this.customer = new ABQArrowTableReader(
                new File(this.tpchInstance + "/customer.arrow"), this.rootAllocator, true, ImmutableIntList.of(0, 1, 2, 3, 4, 5, 7));
        this.orders = new ABQArrowTableReader(
                new File(this.tpchInstance + "/orders.arrow"), this.rootAllocator, true, ImmutableIntList.of(0, 1, 4));
        this.lineitem = new ABQArrowTableReader(
                new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator, true, ImmutableIntList.of(0, 5, 6, 8));
        this.nation = new ABQArrowTableReader(
                new File(this.tpchInstance + "/nation.arrow"), this.rootAllocator, true, ImmutableIntList.of(0, 1));

        // Initialise the hash-table
        this.aggregation_state_map = new AggregationMap();

        // Initialise the join maps
        this.join_map = new JoinMapType();
        this.join_map_0 = new JoinMapType0();
        this.join_map_1 = new JoinMapType1();

        // Initialise the result verifier
        this.resultVerifier = new ResultVerifier(this.tpchInstance + "/q10_result.csv");

        // Initialise the result
        int resultSize = this.resultVerifier.getResultSize();
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
            "-Xmx32g",
            "-Xms16g"
    })
    public void executeQuery(Blackhole bh) throws IOException {
        byte[] byte_array_cache = null;
        byte[][] byte_array_caches = new byte[200][];
        byte[][] byte_array_caches_0 = new byte[200][];
        byte[] byte_array_cache_0 = null;
        byte[][] byte_array_caches_1 = new byte[200][];
        byte[] byte_array_cache_1 = null;

        // DIFF: hard-coded
        // KeyValueMap_2093834526 aggregation_state_map = new KeyValueMap_2093834526();
        // KeyMultiRecordMap_2007031139 join_map = new KeyMultiRecordMap_2007031139();
        // KeyMultiRecordMap_1708786164 join_map_0 = new KeyMultiRecordMap_1708786164();
        // KeyMultiRecordMap_1144653485 join_map_1 = new KeyMultiRecordMap_1144653485();
        // ArrowTableReader nation = cCtx.getArrowReader(0);
        while (nation.loadNextBatch()) {
            org.apache.arrow.vector.IntVector nation_vc_0 = ((org.apache.arrow.vector.IntVector) nation.getVector(0));
            org.apache.arrow.vector.FixedSizeBinaryVector nation_vc_1 = ((org.apache.arrow.vector.FixedSizeBinaryVector) nation.getVector(1));
            int recordCount = nation_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = nation_vc_0.get(aviv);
                long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                byte_array_cache = ArrowOptimisations.getFixedSizeBinaryValue(nation_vc_1, aviv, byte_array_cache);
                join_map_1.associate(ordinal_value, left_join_key_prehash, byte_array_cache);
            }
        }
        // DIFF: hard-coded
        // ArrowTableReader customer = cCtx.getArrowReader(1);
        while (customer.loadNextBatch()) {
            org.apache.arrow.vector.IntVector customer_vc_0 = ((org.apache.arrow.vector.IntVector) customer.getVector(0));
            org.apache.arrow.vector.VarCharVector customer_vc_1 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(1));
            org.apache.arrow.vector.VarCharVector customer_vc_2 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(2));
            org.apache.arrow.vector.IntVector customer_vc_3 = ((org.apache.arrow.vector.IntVector) customer.getVector(3));
            org.apache.arrow.vector.FixedSizeBinaryVector customer_vc_4 = ((org.apache.arrow.vector.FixedSizeBinaryVector) customer.getVector(4));
            org.apache.arrow.vector.Float8Vector customer_vc_5 = ((org.apache.arrow.vector.Float8Vector) customer.getVector(5));
            org.apache.arrow.vector.VarCharVector customer_vc_6 = ((org.apache.arrow.vector.VarCharVector) customer.getVector(7));
            int recordCount = customer_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = customer_vc_3.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                int records_to_join_index = join_map_1.getIndex(ordinal_value, right_join_key_prehash);
                if ((records_to_join_index == -1)) {
                    continue;
                }
                int ordinal_value_0 = customer_vc_0.get(aviv);
                byte[] ordinal_value_1 = ArrowOptimisations.getVarCharBinaryValue(customer_vc_1, aviv, byte_array_caches);
                byte[] ordinal_value_2 = ArrowOptimisations.getVarCharBinaryValue(customer_vc_2, aviv, byte_array_caches_0);
                byte_array_cache_0 = ArrowOptimisations.getFixedSizeBinaryValue(customer_vc_4, aviv, byte_array_cache_0);
                double ordinal_value_3 = customer_vc_5.get(aviv);
                byte[] ordinal_value_4 = ArrowOptimisations.getVarCharBinaryValue(customer_vc_6, aviv, byte_array_caches_1);
                int left_join_record_count = join_map_1.keysRecordCount[records_to_join_index];
                for (int i = 0; i < left_join_record_count; i++) {
                    byte[] left_join_ord_0 = join_map_1.values_record_ord_0[records_to_join_index][i];
                    long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_0);
                    join_map_0.associate(ordinal_value_0, left_join_key_prehash, left_join_ord_0, ordinal_value_1, ordinal_value_2, byte_array_cache_0, ordinal_value_3, ordinal_value_4);
                }
            }
        }
        // DIFF: hard-coded
        // ArrowTableReader orders = cCtx.getArrowReader(2);
        while (orders.loadNextBatch()) {
            org.apache.arrow.vector.IntVector orders_vc_0 = ((org.apache.arrow.vector.IntVector) orders.getVector(0));
            org.apache.arrow.vector.IntVector orders_vc_1 = ((org.apache.arrow.vector.IntVector) orders.getVector(1));
            org.apache.arrow.vector.DateDayVector orders_vc_2 = ((org.apache.arrow.vector.DateDayVector) orders.getVector(4));
            int recordCount = orders_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = orders_vc_2.get(aviv);
                if (!((ordinal_value >= 8674))) {
                    continue;
                }
                if (!((ordinal_value < 8766))) {
                    continue;
                }
                int ordinal_value_0 = orders_vc_1.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_0);
                int records_to_join_index = join_map_0.getIndex(ordinal_value_0, right_join_key_prehash);
                if ((records_to_join_index == -1)) {
                    continue;
                }
                int ordinal_value_1 = orders_vc_0.get(aviv);
                int left_join_record_count = join_map_0.keysRecordCount[records_to_join_index];
                for (int i = 0; i < left_join_record_count; i++) {
                    byte[] left_join_ord_0 = join_map_0.values_record_ord_0[records_to_join_index][i];
                    byte[] left_join_ord_1 = join_map_0.values_record_ord_1[records_to_join_index][i];
                    byte[] left_join_ord_2 = join_map_0.values_record_ord_2[records_to_join_index][i];
                    byte[] left_join_ord_3 = join_map_0.values_record_ord_3[records_to_join_index][i];
                    double left_join_ord_4 = join_map_0.values_record_ord_4[records_to_join_index][i];
                    byte[] left_join_ord_5 = join_map_0.values_record_ord_5[records_to_join_index][i];
                    long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_1);
                    join_map.associate(ordinal_value_1, left_join_key_prehash, left_join_ord_0, ordinal_value_0, left_join_ord_1, left_join_ord_2, left_join_ord_3, left_join_ord_4, left_join_ord_5);
                }
            }
        }
        // DIFF: hard-coded
        // ArrowTableReader lineitem = cCtx.getArrowReader(3);
        while (lineitem.loadNextBatch()) {
            org.apache.arrow.vector.IntVector lineitem_vc_0 = ((org.apache.arrow.vector.IntVector) lineitem.getVector(0));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(6));
            org.apache.arrow.vector.FixedSizeBinaryVector lineitem_vc_3 = ((org.apache.arrow.vector.FixedSizeBinaryVector) lineitem.getVector(8));
            int recordCount = lineitem_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                byte_array_cache_1 = ArrowOptimisations.getFixedSizeBinaryValue(lineitem_vc_3, aviv, byte_array_cache_1);
                if (!(Arrays.equals(byte_array_cache_1, new byte[] { 82 }))) {
                    continue;
                }
                int projection_literal = 1;
                double ordinal_value = lineitem_vc_2.get(aviv);
                double projection_computation_result = (projection_literal - ordinal_value);
                double ordinal_value_0 = lineitem_vc_1.get(aviv);
                double projection_computation_result_0 = (ordinal_value_0 * projection_computation_result);
                int ordinal_value_1 = lineitem_vc_0.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_1);
                int records_to_join_index = join_map.getIndex(ordinal_value_1, right_join_key_prehash);
                if ((records_to_join_index == -1)) {
                    continue;
                }
                int left_join_record_count = join_map.keysRecordCount[records_to_join_index];
                for (int i = 0; i < left_join_record_count; i++) {
                    byte[] left_join_ord_0 = join_map.values_record_ord_0[records_to_join_index][i];
                    int left_join_ord_1 = join_map.values_record_ord_1[records_to_join_index][i];
                    byte[] left_join_ord_2 = join_map.values_record_ord_2[records_to_join_index][i];
                    byte[] left_join_ord_3 = join_map.values_record_ord_3[records_to_join_index][i];
                    byte[] left_join_ord_4 = join_map.values_record_ord_4[records_to_join_index][i];
                    double left_join_ord_5 = join_map.values_record_ord_5[records_to_join_index][i];
                    byte[] left_join_ord_6 = join_map.values_record_ord_6[records_to_join_index][i];
                    long group_key_pre_hash = Int_Hash_Function.preHash(left_join_ord_1);
                    group_key_pre_hash ^= Char_Arr_Hash_Function.preHash(left_join_ord_2);
                    group_key_pre_hash ^= Double_Hash_Function.preHash(left_join_ord_5);
                    group_key_pre_hash ^= Char_Arr_Hash_Function.preHash(left_join_ord_4);
                    group_key_pre_hash ^= Char_Arr_Hash_Function.preHash(left_join_ord_0);
                    group_key_pre_hash ^= Char_Arr_Hash_Function.preHash(left_join_ord_3);
                    group_key_pre_hash ^= Char_Arr_Hash_Function.preHash(left_join_ord_6);
                    aggregation_state_map.incrementForKey(left_join_ord_1, left_join_ord_2, left_join_ord_5, left_join_ord_4, left_join_ord_0, left_join_ord_3, left_join_ord_6, group_key_pre_hash, projection_computation_result_0);
                }
            }
        }
        for (int key_i = 0; key_i < aggregation_state_map.numberOfRecords; key_i++) {
            int groupKey_0 = aggregation_state_map.keys_ord_0[key_i];
            byte[] groupKey_1 = aggregation_state_map.keys_ord_1[key_i];
            double groupKey_2 = aggregation_state_map.keys_ord_2[key_i];
            byte[] groupKey_3 = aggregation_state_map.keys_ord_3[key_i];
            byte[] groupKey_4 = aggregation_state_map.keys_ord_4[key_i];
            byte[] groupKey_5 = aggregation_state_map.keys_ord_5[key_i];
            byte[] groupKey_6 = aggregation_state_map.keys_ord_6[key_i];
            double aggregation_0_value = aggregation_state_map.values_ord_0[key_i];
            // DIFF: replaced by result verification
            // System.out.print((groupKey_0 + ", "));
            // System.out.print((new java.lang.String(groupKey_1) + ", "));
            // System.out.print((String.format("%.2f", aggregation_0_value) + ", "));
            // System.out.print((String.format("%.2f", groupKey_2) + ", "));
            // System.out.print((new java.lang.String(groupKey_4) + ", "));
            // System.out.print((new java.lang.String(groupKey_5) + ", "));
            // System.out.print((new java.lang.String(groupKey_3) + ", "));
            // System.out.println(new java.lang.String(groupKey_6));
            this.resultCustKey[key_i] = groupKey_0;
            this.resultCName[key_i] = groupKey_1;
            this.resultRevenue[key_i] = aggregation_0_value;
            this.resultCAcctbal[key_i] = groupKey_2;
            this.resultNName[key_i] = groupKey_4;
            this.resultCAddress[key_i] = groupKey_5;
            this.resultCPhone[key_i] = groupKey_3;
            this.resultCComment[key_i] = groupKey_6;
        }

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
