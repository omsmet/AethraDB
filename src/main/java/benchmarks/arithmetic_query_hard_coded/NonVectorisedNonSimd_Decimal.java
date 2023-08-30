package benchmarks.arithmetic_query_hard_coded;

import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using the code generated
 * by its non-vectorised query code generation without SIMD-ed operators, but while not actually
 * invoking the code generator itself. This microbenchmark evaluates the performance difference of
 * using the BigDecimal type instead of some 'native' type.
 */
@State(Scope.Benchmark)
public class NonVectorisedNonSimd_Decimal {

    /**
     * We want to test the query processing performance for different table instances, where different
     * instances have different number of aggregation groups and/or key skew.
     */
    @Param({
        "/nvtmp/duckdb/arrow_tpch"
    })
    private String tableFilePath;

    /**
     * State: the {@link RootAllocator} used for reading Arrow files.
     */
    private RootAllocator rootAllocator;

    /**
     * State: the {@link ArrowTableReader} used for reading from the table.
     */
    private ArrowTableReader lineitem;

    /**
     * State: the partial result of the query (null if the query has not been executed yet).
     */
    private BigDecimal[] result;

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        this.lineitem = new ABQArrowTableReader(new File(this.tableFilePath + "/lineitem.arrow"), this.rootAllocator);

        // Initialise the result
        this.result = new BigDecimal[6001215];
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationSetup() throws Exception {
        // Refresh the arrow reader of the query for the next iteration
        this.lineitem.reset();
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
        int resultIndex = 0;                                             // DIFF: added

        // ArrowTableReader lineitem_doubles = cCtx.getArrowReader(0);      DIFF: hard-coded
        while (lineitem.loadNextBatch()) {
            org.apache.arrow.vector.DecimalVector lineitem_doubles_vc_0 = ((org.apache.arrow.vector.DecimalVector) lineitem.getVector(5));
            org.apache.arrow.vector.DecimalVector lineitem_doubles_vc_1 = ((org.apache.arrow.vector.DecimalVector) lineitem.getVector(6));
            org.apache.arrow.vector.DecimalVector lineitem_doubles_vc_2 = ((org.apache.arrow.vector.DecimalVector) lineitem.getVector(7));


            int recordCount = lineitem_doubles_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                BigDecimal projection_literal = new BigDecimal(1);
                BigDecimal projection_computation_result = projection_literal.subtract(lineitem_doubles_vc_1.getObject(aviv));

                BigDecimal projection_computation_result_0 = lineitem_doubles_vc_0.getObject(aviv).multiply(projection_computation_result);

                BigDecimal projection_literal_0 = new BigDecimal(1);
                BigDecimal projection_computation_result_1 = projection_literal_0.add(lineitem_doubles_vc_2.getObject(aviv));
                BigDecimal projection_computation_result_2 = projection_computation_result_0.multiply(projection_computation_result_1);
                // System.out.println(projection_computation_result_2);        DIFF: removed
                this.result[resultIndex++] = projection_computation_result_2; // DIFF: added
            }
        }

        bh.consume(this.result);                                            // DIFF: added
    }

}
