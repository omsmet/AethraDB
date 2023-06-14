package evaluation.general_support.hashmaps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Simple_Int_Int_Map_Test {

    /**
     * The {@link Simple_Int_Int_Map} to use for the test.
     */
    private Simple_Int_Int_Map map;

    /**
     * Sets the test up with an empty map instance.
     */
    @BeforeEach
    public void setup() {
        this.map = new Simple_Int_Int_Map();
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
                () -> this.map.get(negativeKey));

        // Test put method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.put(negativeKey, 42));

        // Test addToKeyOrPutIfNotExist method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.addToKeyOrPutIfNotExist(negativeKey, 42));

        // Test contains method
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> this.map.contains(negativeKey));
    }

    /**
     * Test that a given key is not contained in an empty map.
     */
    @Test
    public void TestEmtpyMapDoesNotContain() {
        assertFalse(this.map.contains(42));
    }

    /**
     * Test that a map indicates that it contains a key which was inserted into it.
     */
    @Test
    public void ContainsPutTest() {
        int key = 42;
        this.map.put(key, -key);
        assertTrue(this.map.contains(key));
    }

    /**
     * Test that we can retrieve the value for two keys which collide with each other.
     * We design this test with an empty hash-table in mind.
     */
    @Test
    public void CheckCollisionResolutionWorks() {
        int key1 = 42;
        int value1 = 41;
        int key2 = 89;
        int value2 = 43;
        this.map.put(key1, value1);
        this.map.put(key2, value2);
        assertEquals(value1, this.map.get(key1));
        assertEquals(value2, this.map.get(key2));
    }

    /**
     * Test that we can overwrite the value for a key if we "put" a value twice.
     */
    @Test
    public void CheckValueOverwrittenOnPut() {
        int key = 42;
        int value1 = 41;
        int value2 = 43;

        this.map.put(key, value1);
        assertEquals(value1, this.map.get(key));

        this.map.put(key, value2);
        assertEquals(value2, this.map.get(key));
    }

    /**
     * Test to check that a value is put into the map using {@code addToKeyOrPutIfNotExist} for a
     * key that is not present in the map.
     */
    @Test
    public void CheckValuePut_AddToKeyOrPutIfNotExist_NonExistentKey() {
        int key = 42;
        int value = 41;

        assertFalse(this.map.contains(key));

        this.map.addToKeyOrPutIfNotExist(key, value);

        assertEquals(value, this.map.get(key));
    }

    /**
     * Test to check that a value is add to the existing value in the map using
     * {@code addToKeyOrPutIfNotExist} for a key that is present in the map.
     */
    @Test
    public void CheckValueAdded_AddToKeyOrPutIfNotExist_ExistentKey() {
        int key = 42;
        int value1 = 41;
        int value2 = 43;
        int sum = value1 + value2;

        this.map.put(key, value1);
        assertEquals(value1, this.map.get(key));

        this.map.addToKeyOrPutIfNotExist(key, value2);
        assertEquals(sum, this.map.get(key));
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
        this.map = new Simple_Int_Int_Map(8192);

        // Generate unique keys
        int[] keys = random.ints(approximateNumberOfEntries, 0, Integer.MAX_VALUE)
                .distinct().toArray();
        System.out.println("Using " + keys.length + " entries");

        // Generate arbitrary values
        int[] values = random.ints(keys.length).toArray();

        System.out.println("Inserting keys");
        for (int i = 0; i < keys.length; i++)
            this.map.put(keys[i], values[i]);

        System.out.println("Retrieving values");
        int[] mapValues = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            mapValues[i] = this.map.get(keys[i]);
        }

        System.out.println("Comparing arrays");
        assertArrayEquals(values, mapValues);
    }

}