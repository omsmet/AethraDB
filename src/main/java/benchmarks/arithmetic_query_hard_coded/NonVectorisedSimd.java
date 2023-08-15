package benchmarks.arithmetic_query_hard_coded;

import evaluation.codegen.infrastructure.context.OptimisationContext;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.CachingArrowTableReader;
import jdk.incubator.vector.DoubleVector;
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
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

/**
 * This microbenchmark evaluates the query processing performance of AethraDB using the code generated
 * by its non-vectorised query code generation with SIMD-ed operators, but while not actually
 * invoking the code generator itself.
 *
 * NOTE: This benchmark was created in a hand-written fashion to reduce implementation effort.
 */
@State(Scope.Benchmark)
public class NonVectorisedSimd {

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
    private ArrowTableReader lineitem_doubles;

    /**
     * State: the partial result of the query (null if the query has not been executed yet).
     */
    private double[] result;

    /**
     * This method sets up the state at the start of each benchmark fork.
     */
    @Setup(Level.Trial)
    public void trialSetup() throws Exception {
        // Setup the database
        this.rootAllocator = new RootAllocator();
        this.lineitem_doubles = new CachingArrowTableReader(new File(this.tableFilePath + "/lineitem_doubles.arrow"), this.rootAllocator);

        // Initialise the result
        this.result = new double[6001215];
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationSetup() throws Exception {
        // Refresh the arrow reader of the query for the next iteration
        this.lineitem_doubles.reset();
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
        int resultIndex = 0;                                             // DIFF: added

        int commonSIMDVectorLength = 4;
        // jdk.incubator.vector.VectorSpecies<Integer> IntVectorSpecies = OptimisationContext.getVectorSpeciesInt();
        jdk.incubator.vector.VectorSpecies<Double> DoubleVectorSpecies = OptimisationContext.getVectorSpeciesDouble();
        // ArrowTableReader lineitem_doubles = cCtx.getArrowReader(0);
        while (lineitem_doubles.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_doubles_vc_0 = ((org.apache.arrow.vector.Float8Vector) lineitem_doubles.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_doubles_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem_doubles.getVector(6));
            org.apache.arrow.vector.Float8Vector lineitem_doubles_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem_doubles.getVector(7));
            int arrowVectorLength = lineitem_doubles_vc_0.getValueCount();
            MemorySegment col_0_ms = OptimisationContext.createMemorySegmentForAddress(lineitem_doubles_vc_0.getDataBufferAddress(), arrowVectorLength * lineitem_doubles_vc_0.TYPE_WIDTH);
            MemorySegment col_1_ms = OptimisationContext.createMemorySegmentForAddress(lineitem_doubles_vc_1.getDataBufferAddress(), arrowVectorLength * lineitem_doubles_vc_1.TYPE_WIDTH);
            MemorySegment col_2_ms = OptimisationContext.createMemorySegmentForAddress(lineitem_doubles_vc_2.getDataBufferAddress(), arrowVectorLength * lineitem_doubles_vc_2.TYPE_WIDTH);
            for (int currentVectorOffset = 0; currentVectorOffset < arrowVectorLength; currentVectorOffset += commonSIMDVectorLength) {
                jdk.incubator.vector.VectorMask<Double> inRangeSIMDMask = DoubleVectorSpecies.indexInRange(currentVectorOffset, arrowVectorLength);

                // 1 - l_discount
                jdk.incubator.vector.DoubleVector col1_SIMD_vector = OptimisationContext.createDoubleVector(DoubleVectorSpecies, col_1_ms, currentVectorOffset * lineitem_doubles_vc_1.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                jdk.incubator.vector.DoubleVector projection_literal = DoubleVector.broadcast(DoubleVectorSpecies, 1d);
                jdk.incubator.vector.DoubleVector projection_computation_result = projection_literal.sub(col1_SIMD_vector, inRangeSIMDMask);

                // l_extendedprice * (1 - l_discount)
                jdk.incubator.vector.DoubleVector col0_SIMD_vector = OptimisationContext.createDoubleVector(DoubleVectorSpecies, col_0_ms, currentVectorOffset * lineitem_doubles_vc_0.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                jdk.incubator.vector.DoubleVector projection_computation_result_0 = col0_SIMD_vector.mul(projection_computation_result, inRangeSIMDMask);

                // 1 + l_tax
                jdk.incubator.vector.DoubleVector col2_SIMD_vector = OptimisationContext.createDoubleVector(DoubleVectorSpecies, col_2_ms, currentVectorOffset * lineitem_doubles_vc_2.TYPE_WIDTH, java.nio.ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                jdk.incubator.vector.DoubleVector projection_literal_0 = DoubleVector.broadcast(DoubleVectorSpecies, 1d);
                jdk.incubator.vector.DoubleVector projection_computation_result_1 = projection_literal_0.add(col2_SIMD_vector, inRangeSIMDMask);

                // l_extendedprice * (1 - l_discount) * (1 + l_tax)
                jdk.incubator.vector.DoubleVector projection_computation_result_2 = projection_computation_result_0.mul(projection_computation_result_1, inRangeSIMDMask);

                double[] partialResult = projection_computation_result_2.toDoubleArray();
                for (int i = 0; i < commonSIMDVectorLength; i++) {
                    if (inRangeSIMDMask.laneIsSet(i))
                        this.result[resultIndex++] = partialResult[i];
                }
            }
        }

        bh.consume(this.result);                                            // DIFF: added
    }

}
