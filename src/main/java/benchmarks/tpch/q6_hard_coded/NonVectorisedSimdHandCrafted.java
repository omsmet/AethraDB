package benchmarks.tpch.q6_hard_coded;

import evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
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
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * This benchmark evaluates what the performance of AethraDB could look like when it could generate
 * non-vectorised query code with SIMD-ed operators, for this query.
 */
@State(Scope.Benchmark)
public class NonVectorisedSimdHandCrafted {

    /**
     * Vector species to be used for double columns in this benchmark.
     */
    private final VectorSpecies<Double> DOUBLE_SPECIES_PREFERRED = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED;

    /**
     * Vector species to be used for integer columns in this benchmark.
     */
    private final VectorSpecies<Integer> INTEGER_SPECIES_DOUBLE_PREFERRED =
              (jdk.incubator.vector.IntVector.SPECIES_512.length() == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_512
            : (jdk.incubator.vector.IntVector.SPECIES_256.length() == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_256
            : (jdk.incubator.vector.IntVector.SPECIES_128.length() == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_128
            : (jdk.incubator.vector.IntVector.SPECIES_64.length()  == DOUBLE_SPECIES_PREFERRED.length()) ? jdk.incubator.vector.IntVector.SPECIES_64
            : null; // should not happen

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
                new File(this.tpchInstance + "/lineitem.arrow"), this.rootAllocator, ImmutableIntList.of(4, 5, 6, 10));

        // Initialise the result
        this.sumResult = -1d;

        // Initialise the result verifier
        String datasetIdentifier = this.tpchInstance.substring(this.tpchInstance.lastIndexOf("/") + 1);
        this.expectedSumResult = switch (datasetIdentifier) {
            case "sf-1" -> 123141078.23d;
            case "sf-10" -> 1230113636.01d;

            default -> throw new UnsupportedOperationException(
                    "tpch.q6_hard_coded.NonVectorisedSimdHandCrafted.trialSetup received an unexpected datasetIdentifier: " + datasetIdentifier);
        };
    }

    /**
     * This method sets up the state at the start of each benchmark iteration.
     */
    @Setup(Level.Invocation)
    public void invocationStetup() throws Exception {
        // Reset the table
        this.lineitem_table.reset();
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
            "-Xmx32g",
            "-Xms16g"
    })
    public void executeQuery(Blackhole bh) throws IOException {
        double sum = 0;
        long count = 0;
        // DIFF: hard-coded
//        ArrowTableReader lineitem = cCtx.getArrowReader(0);
        while (this.lineitem_table.loadNextBatch()) {
            org.apache.arrow.vector.Float8Vector lineitem_vc_0 = ((org.apache.arrow.vector.Float8Vector) this.lineitem_table.getVector(4));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) this.lineitem_table.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) this.lineitem_table.getVector(6));
            org.apache.arrow.vector.DateDayVector lineitem_vc_3 = ((org.apache.arrow.vector.DateDayVector) this.lineitem_table.getVector(10));
            int recordCount = lineitem_vc_0.getValueCount();

            // Manual optimisation: perform SIMD-ed filtering and summation
            MemorySegment col_3_ms = MemorySegment.ofAddress(lineitem_vc_3.getDataBufferAddress(), recordCount * lineitem_vc_3.TYPE_WIDTH);
            MemorySegment col_2_ms = MemorySegment.ofAddress(lineitem_vc_2.getDataBufferAddress(), recordCount * lineitem_vc_2.TYPE_WIDTH);
            MemorySegment col_0_ms = MemorySegment.ofAddress(lineitem_vc_0.getDataBufferAddress(), recordCount * lineitem_vc_0.TYPE_WIDTH);
            MemorySegment col_1_ms = MemorySegment.ofAddress(lineitem_vc_1.getDataBufferAddress(), recordCount * lineitem_vc_1.TYPE_WIDTH);

            for (int currentIndex = 0; currentIndex < recordCount; currentIndex += DOUBLE_SPECIES_PREFERRED.length()) {
                // Filter the date column
                jdk.incubator.vector.VectorMask<Integer> inRangeSIMDMask = INTEGER_SPECIES_DOUBLE_PREFERRED.indexInRange(currentIndex, recordCount);
                jdk.incubator.vector.IntVector col_3_SIMDVector = jdk.incubator.vector.IntVector.fromMemorySegment(INTEGER_SPECIES_DOUBLE_PREFERRED, col_3_ms, currentIndex * lineitem_vc_3.TYPE_WIDTH, ByteOrder.LITTLE_ENDIAN, inRangeSIMDMask);
                jdk.incubator.vector.VectorMask<Integer> datesSurvivingLB = col_3_SIMDVector.compare(VectorOperators.GE, 8766, inRangeSIMDMask);
                jdk.incubator.vector.VectorMask<Integer> datesSurvivingUB = col_3_SIMDVector.compare(VectorOperators.LT, 9131, datesSurvivingLB);

                // Filter the discount column
                jdk.incubator.vector.VectorMask<Double> inRangeDatesMask = datesSurvivingUB.cast(DOUBLE_SPECIES_PREFERRED);
                jdk.incubator.vector.DoubleVector col_2_SIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(DOUBLE_SPECIES_PREFERRED, col_2_ms, currentIndex * lineitem_vc_2.TYPE_WIDTH, ByteOrder.LITTLE_ENDIAN, inRangeDatesMask);
                jdk.incubator.vector.VectorMask<Double> discountsSurvivingLB = col_2_SIMDVector.compare(VectorOperators.GE, 0.05, inRangeDatesMask);
                jdk.incubator.vector.VectorMask<Double> discountsSurvivingUB = col_2_SIMDVector.compare(VectorOperators.LE, 0.07, discountsSurvivingLB);

                // Filter the quantity column
                jdk.incubator.vector.DoubleVector col_0_SIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(DOUBLE_SPECIES_PREFERRED, col_0_ms, currentIndex * lineitem_vc_0.TYPE_WIDTH, ByteOrder.LITTLE_ENDIAN, discountsSurvivingUB);
                jdk.incubator.vector.VectorMask<Double> quantitiesSurvivingUB = col_0_SIMDVector.compare(VectorOperators.LT, 24, discountsSurvivingUB);

                // Multiply the price and discount columns
                jdk.incubator.vector.DoubleVector col_1_SIMDVector = jdk.incubator.vector.DoubleVector.fromMemorySegment(DOUBLE_SPECIES_PREFERRED, col_1_ms, currentIndex * lineitem_vc_1.TYPE_WIDTH, ByteOrder.LITTLE_ENDIAN, quantitiesSurvivingUB);
                jdk.incubator.vector.DoubleVector priceTimesDiscount_SIMDVector = col_1_SIMDVector.mul(col_2_SIMDVector, quantitiesSurvivingUB);

                // Maintain sum and count variables
                sum += priceTimesDiscount_SIMDVector.reduceLanes(VectorOperators.ADD, quantitiesSurvivingUB);
                count += quantitiesSurvivingUB.trueCount();
            }
            // End of manual optimisation
        }
        double projection_literal = Double.NaN;
        int projection_literal_0 = 0;
        double projection_computation_result = (count == projection_literal_0) ? projection_literal : sum;
        // DIFF: replaced by result verification
        // System.out.println(projection_computation_result);
        this.sumResult = projection_computation_result;

        // DIFF: prevent optimising the result away
        bh.consume(this.sumResult);
    }

}
