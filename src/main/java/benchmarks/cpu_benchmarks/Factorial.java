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
 * Simple CPU-heavy JMH microbenchmark to test the CPU performance of our reference machines.
 */
@State(Scope.Benchmark)
public class Factorial {

    /**
     * The random source to use for this experiment.
     */
    private Random rng = new Random(307231158);

    /**
     * This parameter indicates the number {@code n} for which we need to compute {@code n!}.
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
     * Method to actually benchmark the performance of computing {@code n!}.
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void computeNFactorial(Blackhole blackhole) {
        int result = 1;

        for (int i = 1; i <= this.n; i++)
            result *= i;

        blackhole.consume(result);
    }

}
