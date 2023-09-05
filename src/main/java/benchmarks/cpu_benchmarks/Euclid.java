//package benchmarks.cpu_benchmarks;
//
//import org.openjdk.jmh.annotations.Benchmark;
//import org.openjdk.jmh.annotations.BenchmarkMode;
//import org.openjdk.jmh.annotations.Level;
//import org.openjdk.jmh.annotations.Mode;
//import org.openjdk.jmh.annotations.OutputTimeUnit;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.annotations.Setup;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.infra.Blackhole;
//
//import java.math.BigInteger;
//import java.util.Random;
//import java.util.concurrent.TimeUnit;
//
///**
// * Simple CPU-heavy JMH microbenchmark benchmark to test the CPU performance of our reference machines.
// */
//@State(Scope.Benchmark)
//public class Euclid {
//
//    /**
//     * The random source to use for this experiment.
//     */
//    private Random rng = new Random(307231220);
//
//    /**
//     * This parameter indicates the number {@code a} used in the computation of {@code gcd(a, b)}.
//     */
//    private BigInteger a;
//
//    /**
//     * This parameter indicates the number {@code b} used in the computation of {@code gcd (a, b)}.
//     */
//    private BigInteger b;
//
//    /**
//     * Method which sets up {@code a, b} with random values every benchmark iteration.
//     * The generation is done in such a way that a > b always holds.
//     */
//    @Setup(Level.Iteration)
//    public void pickN() {
//        // Generate random postive 16-byte number
//        byte[] firstValBinaryA = new byte[16];
//        byte[] firstValBinaryB = new byte[16];
//        rng.nextBytes(firstValBinaryA);
//        rng.nextBytes(firstValBinaryB);
//        for (int i = 0; i < 16; i++)
//            firstValBinaryA[i] |= firstValBinaryB[i];
//
//        BigInteger firstValue = new BigInteger(firstValBinaryA).abs();
//
//        // Generate random postive 16-byte number
//        byte[] secondValBinaryA = new byte[16];
//        byte[] secondValBinaryB = new byte[16];
//        rng.nextBytes(secondValBinaryA);
//        rng.nextBytes(secondValBinaryB);
//        for (int i = 0; i < 16; i++)
//            secondValBinaryA[i] |= secondValBinaryB[i];
//
//        BigInteger secondValue = new BigInteger(secondValBinaryA).abs();
//
//        this.a = (firstValue.compareTo(secondValue) > 0) ? firstValue : secondValue;
//        this.b = (firstValue.compareTo(secondValue) > 0) ? secondValue : firstValue;
//    }
//
//    /**
//     * Method to actually benchmark the performance of computing {@code gcd(a,b)} using the division
//     * based method.
//     */
//    @Benchmark
//    @BenchmarkMode(Mode.SampleTime)
//    @OutputTimeUnit(TimeUnit.NANOSECONDS)
//    public void computeGcdDiv(Blackhole blackhole) {
//        BigInteger local_a = this.a;
//        BigInteger local_b = this.b;
//
//        while (local_b.compareTo(BigInteger.ZERO) != 0) {
//            BigInteger t = local_b;
//            local_b = local_a.mod(local_b);
//            local_a = t;
//        }
//
//        blackhole.consume(local_a);
//    }
//
//    /**
//     * Method to actually benchmark the performance of computing {@code gcd(a,b)} using the subtraction
//     * based method.
//     */
//    @Benchmark
//    @BenchmarkMode(Mode.SampleTime)
//    @OutputTimeUnit(TimeUnit.NANOSECONDS)
//    public void computeGcdSub(Blackhole blackhole) {
//        BigInteger local_a = this.a;
//        BigInteger local_b = this.b;
//
//        while (local_a.compareTo(local_b) != 0) {
//            if (local_a.compareTo(local_b) > 0)
//                local_a = local_a.subtract(local_b);
//            else
//                local_b = local_b.subtract(local_a);
//        }
//
//        blackhole.consume(local_a);
//    }
//
//}
