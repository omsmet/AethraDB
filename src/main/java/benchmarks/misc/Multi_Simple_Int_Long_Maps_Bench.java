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
 * Benchmark to evaluate the performance of the {@link Simple_Int_Long_Map} when we need to maintain
 * multiple maps for the same key column.
 */
@State(Scope.Benchmark)
public class Multi_Simple_Int_Long_Maps_Bench {

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
    private long[] values_col_1;
    private long[] values_col_2;
    private long[] values_col_3;

    /**
     * The sum that should result for each key after insertion.
     */
    private long[] perKeySums_1;
    private long[] perKeySums_2;
    private long[] perKeySums_3;

    /**
     * The random number generator used for generating the benchmark data.
     */
    private Random rng;

    /**
     * An empty to insert records into.
     */
    private Simple_Int_Long_Map insertionMap_1;
    private Simple_Int_Long_Map insertionMap_2;
    private Simple_Int_Long_Map insertionMap_3;

    /**
     * The full map to read values from.
     */
    private Simple_Int_Long_Map retrievalMap_1;
    private Simple_Int_Long_Map retrievalMap_2;
    private Simple_Int_Long_Map retrievalMap_3;

    /**
     * Boolean keeping track of whether the {@code insertionMap} should be validated.
     */
    private boolean validateInsertionMap;

    /**
     * Array that can be used to write values to in the read benchmark.
     */
    private long[] readResult_1;
    private long[] readResult_2;
    private long[] readResult_3;

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
        this.values_col_1 = new long[numberOfRecords];
        this.values_col_2 = new long[numberOfRecords];
        this.values_col_3 = new long[numberOfRecords];
        this.perKeySums_1 = new long[numberOfKeys];
        this.perKeySums_2 = new long[numberOfKeys];
        this.perKeySums_3 = new long[numberOfKeys];

        // Generate the keys
        for (int i = 0; i < this.keys.length; i++)
            this.keys[i] = this.rng.nextInt(0, numberOfKeys);

        // Generate the values and per key sums
        for (int i = 0; i < this.values_col_1.length; i++) {
            this.values_col_1[i] = this.rng.nextLong();
            this.values_col_2[i] = this.rng.nextLong();
            this.values_col_3[i] = this.rng.nextLong();
            this.perKeySums_1[this.keys[i]] += this.values_col_1[i];
            this.perKeySums_2[this.keys[i]] += this.values_col_2[i];
            this.perKeySums_3[this.keys[i]] += this.values_col_3[i];
        }

        // Setup the full map
        this.retrievalMap_1 = new Simple_Int_Long_Map();
        this.retrievalMap_2 = new Simple_Int_Long_Map();
        this.retrievalMap_3 = new Simple_Int_Long_Map();
        for (int i = 0; i < this.keys.length; i++) {
            int key = this.keys[i];
            long preHash = Int_Hash_Function.preHash(key);

            long value_1 = this.values_col_1[i];
            long value_2 = this.values_col_2[i];
            long value_3 = this.values_col_3[i];

            this.retrievalMap_1.addToKeyOrPutIfNotExist(key, preHash, value_1);
            this.retrievalMap_2.addToKeyOrPutIfNotExist(key, preHash, value_2);
            this.retrievalMap_3.addToKeyOrPutIfNotExist(key, preHash, value_3);
        }

    }

    /**
     * Method which resets part of the benchmark at the start of each iteration.
     */
    @Setup(Level.Invocation)
    public void setupIteration() {
        // Setup the empty map
        this.insertionMap_1 = new Simple_Int_Long_Map();
        this.insertionMap_2 = new Simple_Int_Long_Map();
        this.insertionMap_3 = new Simple_Int_Long_Map();
        this.validateInsertionMap = false;

        // Setup the read result
        this.readResult_1 = new long[numberOfKeys];
        this.readResult_2 = new long[numberOfKeys];
        this.readResult_3 = new long[numberOfKeys];
        this.validateReadResult = false;
    }

    /**
     * Method which performs the necessary validations at the end of each iteration.
     */
    @TearDown(Level.Invocation)
    public void verify() {
        if (this.validateReadResult) {
            for (int key = 0; key < numberOfKeys; key++) {
                if (this.perKeySums_1[key] != this.readResult_1[key])
                    throw new RuntimeException("The first computed read result is not correct");

                if (this.perKeySums_2[key] != this.readResult_2[key])
                    throw new RuntimeException("The second computed read result is not correct");

                if (this.perKeySums_3[key] != this.readResult_3[key])
                    throw new RuntimeException("The third computed read result is not correct");
            }
        }

        if (this.validateInsertionMap) {
            for (int key = 0; key < numberOfKeys; key++) {
                long prehash = Int_Hash_Function.preHash(key);

                if (this.insertionMap_1.get(key, prehash) != this.perKeySums_1[key])
                    throw new RuntimeException("The first computed insertion map is not correct");

                if (this.insertionMap_2.get(key, prehash) != this.perKeySums_2[key])
                    throw new RuntimeException("The second computed insertion map is not correct");

                if (this.insertionMap_3.get(key, prehash) != this.perKeySums_3[key])
                    throw new RuntimeException("The third computed insertion map is not correct");
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
        for (int i = 0; i < this.retrievalMap_1.numberOfRecords; i++) {
            int key = this.retrievalMap_1.keys[i];
            long prehash = Int_Hash_Function.preHash(key);

            this.readResult_1[key] = this.retrievalMap_1.get(key, prehash);
            this.readResult_2[key] = this.retrievalMap_2.get(key, prehash);
            this.readResult_3[key] = this.retrievalMap_3.get(key, prehash);
        }

        blackhole.consume(this.readResult_1);
        blackhole.consume(this.readResult_2);
        blackhole.consume(this.readResult_3);
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

            long value_1 = this.values_col_1[i];
            long value_2 = this.values_col_2[i];
            long value_3 = this.values_col_3[i];

            this.insertionMap_1.addToKeyOrPutIfNotExist(key, prehash, value_1);
            this.insertionMap_2.addToKeyOrPutIfNotExist(key, prehash, value_2);
            this.insertionMap_3.addToKeyOrPutIfNotExist(key, prehash, value_3);
        }

        blackhole.consume(this.insertionMap_1);
        blackhole.consume(this.insertionMap_2);
        blackhole.consume(this.insertionMap_3);
    }

}
