package evaluation.general_support.hashmaps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static evaluation.general_support.hashmaps.Int_Hash_Function.preHash;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Simple_Int_Long_Map_Test {

    /**
     * The {@link Simple_Int_Long_Map} to use for the test.
     */
    private Simple_Int_Long_Map map;

    /**
     * Sets the test up with an empty map instance.
     */
    @BeforeEach
    public void setup() {
        this.map = new Simple_Int_Long_Map();
    }

    /**
     * Test to check that negative keys are rejected on all public methods.
     */
    @Test
    public void NegativeKeyFails() {
        int negativeKey = -42;

        // Test get method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.get(negativeKey, preHash(negativeKey)));

        // Test put method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.put(negativeKey, preHash(negativeKey), 42));

        // Test addToKeyOrPutIfNotExist method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.addToKeyOrPutIfNotExist(negativeKey, preHash(negativeKey), 42));

        // Test contains method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.contains(negativeKey, preHash(negativeKey)));
    }

    /**
     * Test that a given key is not contained in an empty map.
     */
    @Test
    public void TestEmtpyMapDoesNotContain() {
        int key = 42;
        assertFalse(this.map.contains(key, preHash(key)));
    }

    /**
     * Test that a map indicates that it contains a key which was inserted into it.
     */
    @Test
    public void ContainsPutTest() {
        int key = 42;
        long value = -key;
        this.map.put(key, preHash(key), value);
        assertTrue(this.map.contains(key, preHash(key)));
    }

    /**
     * Test that we can retrieve the value for two keys which collide with each other.
     * We design this test with an empty hash-table in mind.
     */
    @Test
    public void CheckCollisionResolutionWorks() {
        int key1 = 42;
        long value1 = 41;
        int key2 = 89;
        long value2 = 43;
        this.map.put(key1, preHash(key1), value1);
        this.map.put(key2, preHash(key2), value2);
        assertEquals(value1, this.map.get(key1, preHash(key1)));
        assertEquals(value2, this.map.get(key2, preHash(key2)));
    }

    /**
     * Test that we can overwrite the value for a key if we "put" a value twice.
     */
    @Test
    public void CheckValueOverwrittenOnPut() {
        int key = 42;
        long value1 = 41;
        long value2 = 43;

        this.map.put(key, preHash(key), value1);
        assertEquals(value1, this.map.get(key, preHash(key)));

        this.map.put(key, preHash(key), value2);
        assertEquals(value2, this.map.get(key, preHash(key)));
    }

    /**
     * Test to check that a value is put into the map using {@code addToKeyOrPutIfNotExist} for a
     * key that is not present in the map.
     */
    @Test
    public void CheckValuePut_AddToKeyOrPutIfNotExist_NonExistentKey() {
        int key = 42;
        long value = 41;

        assertFalse(this.map.contains(key, preHash(key)));

        this.map.addToKeyOrPutIfNotExist(key, preHash(key), value);

        assertEquals(value, this.map.get(key, preHash(key)));
    }

    /**
     * Test to check that a value is add to the existing value in the map using
     * {@code addToKeyOrPutIfNotExist} for a key that is present in the map.
     */
    @Test
    public void CheckValueAdded_AddToKeyOrPutIfNotExist_ExistentKey() {
        int key = 42;
        long value1 = 41;
        long value2 = 43;
        long sum = value1 + value2;

        this.map.put(key, preHash(key), value1);
        assertEquals(value1, this.map.get(key, preHash(key)));

        this.map.addToKeyOrPutIfNotExist(key, preHash(key), value2);
        assertEquals(sum, this.map.get(key, preHash(key)));
    }

    /**
     * Test which verifies that if we insert many random elements into a map, we can still retrieve
     * the value for each key.
     */
    @Test
    public void BigRandomMapTest() {
        int approximateNumberOfEntries = 2_000_000;
        Random random = new Random(434241);

        // Initialise a slightly larger map
        this.map = new Simple_Int_Long_Map(8192);

        // Generate unique keys
        int[] keys = random.ints(approximateNumberOfEntries, 0, Integer.MAX_VALUE)
                .distinct().toArray();
        System.out.println("Using " + keys.length + " entries");

        // Generate arbitrary values
        long[] values = random.longs(keys.length).toArray();

        System.out.println("Inserting keys");
        for (int i = 0; i < keys.length; i++)
            this.map.put(keys[i], preHash(keys[i]), values[i]);

        System.out.println("Retrieving values");
        long[] mapValues = new long[keys.length];
        for (int i = 0; i < keys.length; i++) {
            mapValues[i] = this.map.get(keys[i],  preHash(keys[i]));
        }

        System.out.println("Comparing arrays");
        assertArrayEquals(values, mapValues);
    }

}