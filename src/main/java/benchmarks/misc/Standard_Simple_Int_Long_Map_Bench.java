package benchmarks.misc;

import evaluation.general_support.hashmaps.Int_Hash_Function;
import evaluation.general_support.hashmaps.Simple_Int_Long_Map;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark to evaluate the performance of the {@link Simple_Int_Long_Map}.
 */
@State(Scope.Benchmark)
public class Standard_Simple_Int_Long_Map_Bench {

    /**
     * The initial size with which the maps are created.
     */
    @Param({
            "2",
            "16",
            "128",
            "1024",
            "8192",
            "65536",
            "524288"
    })
    private int initialMapSize;

    /**
     * The number of records that will be made available in the map under test.
     */
    private final int numberOfRecords = 3 * 1024 * 1024 * 10;

    /**
     * The number of distinct keys that will be used
     */
    private final int numberOfKeys = 128 * 1024;

    /**
     * The keys that will be used for the benchmark.
     */
    private int[] keys;

    /**
     * The values that will be used by the benchmark.
     */
    private long[] values;

    /**
     * The sum that should result for each key after insertion.
     */
    private long[] perKeySums;

    /**
     * The random number generator used for generating the benchmark data.
     */
    private Random rng;

    /**
     * An empty to insert records into.
     */
    private Simple_Int_Long_Map insertionMap;

    /**
     * The full map to read values from.
     */
    private Simple_Int_Long_Map retrievalMap;

    /**
     * Boolean keeping track of whether the {@code insertionMap} should be validated.
     */
    private boolean validateInsertionMap;

    /**
     * Array that can be used to write values to in the read benchmark.
     */
    private long[] readResult;

    /**
     * Boolean keeping track of whether the {@code readResult} should be validated.
     */
    private boolean validateReadResult;

    /**
     * Method which sets up (part of) the benchmark at the start of the benchmark fork.
     */
    @Setup(Level.Trial)
    public void setupFork() {
        this.rng = new Random(507231712);
        this.keys = new int[numberOfRecords];
        this.values = new long[numberOfRecords];
        this.perKeySums = new long[numberOfKeys];

        // Generate the keys
        for (int i = 0; i < this.keys.length; i++)
            this.keys[i] = this.rng.nextInt(0, numberOfKeys);

        // Generate the values and per key sums
        for (int i = 0; i < this.values.length; i++) {
            this.values[i] = this.rng.nextLong();
            this.perKeySums[this.keys[i]] += this.values[i];
        }

        // Setup the full map
        this.retrievalMap = new Simple_Int_Long_Map(initialMapSize);
        for (int i = 0; i < this.keys.length; i++) {
            int key = this.keys[i];
            long preHash = Int_Hash_Function.preHash(key);
            long value = this.values[i];
            this.retrievalMap.addToKeyOrPutIfNotExist(key, preHash, value);
        }

    }

    /**
     * Method which resets part of the benchmark at the start of each iteration.
     */
    @Setup(Level.Invocation)
    public void setupIteration() {
        // Setup the empty map
        this.insertionMap = new Simple_Int_Long_Map(initialMapSize);
        this.validateInsertionMap = false;

        // Setup the read result
        this.readResult = new long[numberOfKeys];
        this.validateReadResult = false;

    }

    /**
     * Method which performs the necessary validations at the end of each iteration.
     */
    @TearDown(Level.Invocation)
    public void verify() {
        if (this.validateReadResult) {
            for (int key = 0; key < numberOfKeys; key++) {
                if (this.perKeySums[key] != this.readResult[key]) {
                    throw new RuntimeException("The computed read result is not correct");
                }
            }
        }

        if (this.validateInsertionMap) {
            for (int key = 0; key < this.perKeySums.length; key++) {
                long prehash = Int_Hash_Function.preHash(key);
                if (this.insertionMap.get(key, prehash) != this.perKeySums[key])
                    throw new RuntimeException("The computed insertion map is not correct");
            }
        }

    }

    /**
     * Benchmark method which validates the read performance of the hash-map.
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void benchmarkReadPerformance(Blackhole blackhole) {
        this.validateReadResult = true;

        // Iterate over the keys in the map
        for (int i = 0; i < this.retrievalMap.numberOfRecords; i++) {
            int key = this.retrievalMap.keys[i];
            long prehash = Int_Hash_Function.preHash(key);
            this.readResult[key] = this.retrievalMap.get(key, prehash);
        }

        blackhole.consume(this.readResult);
    }

    /**
     * Benchmark method which validates the write performance of the hash-map.
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void benchmarkWritePerformance(Blackhole blackhole) {
        this.validateInsertionMap = true;

        // Iterate over the keys and insert the values into the map
        for (int i = 0; i < this.keys.length; i++) {
            int key = this.keys[i];
            long prehash = Int_Hash_Function.preHash(key);
            long value = this.values[i];
            this.insertionMap.addToKeyOrPutIfNotExist(key, prehash, value);
        }

        blackhole.consume(this.insertionMap);
    }

}
