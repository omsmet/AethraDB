package benchmarks.tpch.q6_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
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
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates the performance of AethraDB using its non-vectorised query code
 * generation without SIMD-ed operators, but while not actually invoking the code generator itself.
 */
@State(Scope.Benchmark)
public class NonVectorisedNonSimdOptimalOrder {

    /**
     * Different instances of the TPC-H database can be tested using this benchmark.
     */
    @Param({
            "/nvtmp/AethraTestData/tpch/sf-1",
            "/nvtmp/AethraTestData/tpch/sf-10",
            "/nvtmp/AethraTestData/tpch/sf-100",
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
        this.lineitem = new ABQArrowTableReader(
                new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator, true, ImmutableIntList.of(4, 5, 6, 10));

        // Initialise the result
        this.sumResult = -1d;

        // Initialise the result verifier
        String datasetIdentifier = this.tpchInstance.substring(this.tpchInstance.lastIndexOf("/") + 1);
        this.expectedSumResult = switch (datasetIdentifier) {
            case "sf-1" -> 123141078.23d;
            case "sf-10" -> 1230113636.01d;
            case "sf-100" -> 12330426888.46369d;

            default -> throw new UnsupportedOperationException(
                    "tpch.q6_hard_coded.NonVectorisedNonSimd.trialSetup received an unexpected datasetIdentifier: " + datasetIdentifier);
        };
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationStetup() throws Exception {
        // Reset the table
        this.lineitem.reset();
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
            "-Xmx32g",
            "-Xms16g"
    })
    public void executeQuery(Blackhole bh) throws IOException {
        double sum = 0;
        long count = 0;
        // DIFF: hard-coded
        // ArrowTableReader lineitem = cCtx.getArrowReader(0);
        while (lineitem.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_vc_0 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(6));
            org.apache.arrow.vector.DateDayVector lineitem_vc_3 = ((org.apache.arrow.vector.DateDayVector) lineitem.getVector(10));
            int recordCount = lineitem_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                // Max date
                int ordinal_value = lineitem_vc_3.get(aviv);
                if (!((ordinal_value < 9131))) {
                    continue;
                }
                // Max quantity
                double ordinal_value_1 = lineitem_vc_0.get(aviv);
                if (!((ordinal_value_1 < 24))) {
                    continue;
                }
                // Min discount
                double ordinal_value_0 = lineitem_vc_2.get(aviv);
                if (!((ordinal_value_0 >= 0.05))) {
                    continue;
                }
                // Min date
                if (!((ordinal_value >= 8766))) {
                    continue;
                }
                // Max discount
                if (!((ordinal_value_0 <= 0.07))) {
                    continue;
                }

                double ordinal_value_2 = lineitem_vc_1.get(aviv);
                double projection_computation_result = (ordinal_value_2 * ordinal_value_0);
                sum += projection_computation_result;
                count++;
            }
        }
        double projection_literal = Double.NaN;
        int projection_literal_0 = 0;
        double projection_computation_result = (count == projection_literal_0) ? projection_literal : sum;
        // DIFF: replaced by result verification
        // System.out.println(String.format("%.2f", projection_computation_result));
        this.sumResult = projection_computation_result;

        // DIFF: prevent optimising the result away
        bh.consume(this.sumResult);
    }

}
