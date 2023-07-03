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
public class Fibonacci {

    /**
     * The random source to use for this experiment.
     */
    private Random rng = new Random(307231207);

    /**
     * This parameter indicates the number {@code n} for which we need to compute {@code F_n}: the
     * n-th number in the Fibonacci sequence.
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
     * Method to actually benchmark the performance of computing {@code F_n}.
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void computeFibN(Blackhole blackhole) {
        long F_n_2 = 0;     // F_{n - 2}, initialised as F_0 = 0
        long F_n_1 = 1;     // F_{n - 1}, initialised as F_1 = 1
        long F_n = -1;      // F_n, which needs to be computed for this.n (assumed > 2)

        for (int i = 2; i <= this.n; i++) {
            F_n = F_n_1 + F_n_2;    // By definition of the Fibonacci sequence F_i = F_{i - 1} + F_{i - 2}
            F_n_2 = F_n_1;          // Prepare for the next iteration
            F_n_1 = F_n;            // Prepare for the next iteration
        }

        // At this point, F_n has the real value for the n-th Fibonacci number
        blackhole.consume(F_n);
    }

}
