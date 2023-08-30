package benchmarks.tpch.q6_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.AllocationManager;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.BufferPoolAllocationManager;
import evaluation.vector_support.VectorisedAggregationOperators;
import evaluation.vector_support.VectorisedArithmeticOperators;
import evaluation.vector_support.VectorisedFilterOperators;
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
     * State: the {@link AllocationManager} used for performing query-specific allocations.
     */
    private AllocationManager allocationManager;

    /**
     * State: the {@link ArrowTableReader} used for reading the lineitem table.
     */
    private ArrowTableReader lineitem_table;

    /**
     * State: the result of the query.
     */
    private double sumResult;

    /**
     * State: the expected result for the query.
     */
    private double expectedSumResult;

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

        // Initialise the result
        this.sumResult = -1d;

        // Initialise the result verifier
        String datasetIdentifier = this.tpchInstance.substring(this.tpchInstance.lastIndexOf("/") + 1);
        this.expectedSumResult = switch (datasetIdentifier) {
            case "sf-1" -> 123141078.23d;

            default -> throw new UnsupportedOperationException(
                    "tpch.q6_hard_coded.VectorisedSimd.trialSetup received an unexpected datasetIdentifier: " + datasetIdentifier);
        };
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
    }

    /**
     * This method verifies successful completion of the previous benchmark and cleans up after it.
     */
    @TearDown(Level.Invocation)
    public void teardown() {
        // Verify the result
        if (Math.abs(this.sumResult - this.expectedSumResult) > 0.01) {
            throw new RuntimeException("The computed result is incorrect");
        }

        // Reset the result after verifying it
        this.sumResult = -1d;
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
        boolean[] ordinal_3_val_mask = this.allocationManager.getBooleanVector();
        boolean[] ordinal_3_val_mask_0 = this.allocationManager.getBooleanVector();
        boolean[] ordinal_2_val_mask = this.allocationManager.getBooleanVector();
        boolean[] ordinal_2_val_mask_0 = this.allocationManager.getBooleanVector();
        boolean[] ordinal_0_val_mask = this.allocationManager.getBooleanVector();
        double[] projection_computation_result = this.allocationManager.getDoubleVector();
        double sum = 0;
        long count = 0;
        // DIFF: hard-coded
        // ArrowTableReader lineitem = cCtx.getArrowReader(0);
        while (this.lineitem_table.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_vc_0 = ((org.apache.arrow.vector.Float8Vector) this.lineitem_table.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) this.lineitem_table.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) this.lineitem_table.getVector(6));
            org.apache.arrow.vector.DateDayVector lineitem_vc_3 = ((org.apache.arrow.vector.DateDayVector) this.lineitem_table.getVector(10));
            int ordinal_3_val_mask_length = VectorisedFilterOperators.geSIMD(lineitem_vc_3, 8766, ordinal_3_val_mask);
            int ordinal_3_val_mask_0_length = VectorisedFilterOperators.ltSIMD(lineitem_vc_3, 9131, ordinal_3_val_mask_0, ordinal_3_val_mask, ordinal_3_val_mask_length);
            int ordinal_2_val_mask_length = VectorisedFilterOperators.geSIMD(lineitem_vc_2, 0.05, ordinal_2_val_mask, ordinal_3_val_mask_0, ordinal_3_val_mask_0_length);
            int ordinal_2_val_mask_0_length = VectorisedFilterOperators.leSIMD(lineitem_vc_2, 0.07, ordinal_2_val_mask_0, ordinal_2_val_mask, ordinal_2_val_mask_length);
            int ordinal_0_val_mask_length = VectorisedFilterOperators.ltSIMD(lineitem_vc_0, 24, ordinal_0_val_mask, ordinal_2_val_mask_0, ordinal_2_val_mask_0_length);
            int projection_computation_result_length;
            // DIFF: removed mask
            projection_computation_result_length = VectorisedArithmeticOperators.multiplySIMD(lineitem_vc_1, lineitem_vc_2, projection_computation_result);
            sum += VectorisedAggregationOperators.vectorSum(projection_computation_result, projection_computation_result_length, ordinal_0_val_mask, ordinal_0_val_mask_length);
            count += VectorisedAggregationOperators.count(ordinal_0_val_mask, ordinal_0_val_mask_length);
        }
        double projection_literal = Double.NaN;
        int projection_literal_0 = 0;
        double projection_computation_result_0 = (count == projection_literal_0) ? projection_literal : sum;
        // DIFF: replaced by result verification
        // System.out.println(projection_computation_result_0);
        this.sumResult = projection_computation_result_0;
        // DIFF: hard-coded allocation manager
        this.allocationManager.release(ordinal_3_val_mask);
        this.allocationManager.release(ordinal_3_val_mask_0);
        this.allocationManager.release(ordinal_2_val_mask);
        this.allocationManager.release(ordinal_2_val_mask_0);
        this.allocationManager.release(ordinal_0_val_mask);
        this.allocationManager.release(projection_computation_result);

        // DIFF: prevent optimising the result away
        bh.consume(this.sumResult);
    }

}