package benchmarks.cpu_benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Simple CPU-heavy JMH microbenchmark benchmark to test the CPU performance of our reference machines.
 */
@State(Scope.Benchmark)
public class BiggestPrime {

    /**
     * The random source to use for this experiment.
     */
    private Random rng = new Random(307231253);

    /**
     * This parameter indicates the number {@code n} for which we need to compute the largest prime
     * {@code < n}.
     */
    private int n;

    /**
     * Method which sets up {@code n} with a random value in the range [750.000, 1.000.000] every benchmark iteration.
     */
    @Setup(Level.Iteration)
    public void pickN() {
        this.n = rng.nextInt(750_000, 1_000_001);
    }

    /**
     * Method to actually benchmark the performance of computing the largest prime {@code < n}.
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void computeLargestPrime(Blackhole blackhole) {
        int largestPrime = n;
        boolean isPrime;

        do {
            isPrime = true;

            for (int i = 2; i < Math.ceil(Math.sqrt(largestPrime)) + 1; i++) {
                if (n % i == 0) {
                    isPrime = false;
                    break;
                }
            }
        } while (!isPrime);

        // At this point, largestPrime is a prime number
        blackhole.consume(largestPrime);
    }

}
