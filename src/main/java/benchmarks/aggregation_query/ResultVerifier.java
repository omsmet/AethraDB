package benchmarks.aggregation_query;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to verify the result of the aggregation query based on the dataset and query paradigm.
 */
public class ResultVerifier {

    /**
     * Boolean indicating whether the results to be checked result from vectorised processing.
     */
    private final boolean vectorisedParadigm;

    /**
     * Contains the "groupBy" keys in the result.
     */
    private final int[] col1Keys;

    /**
     * Contains the sum for the values of column 2 per key.
     */
    private final long[] col2Sum;

    /**
     * Contains the sum for the values of column 3 per key.
     */
    private final long[] col3Sum;

    /**
     * Contains the sum for the values of column 4 per key.
     */
    private final long[] col4Sum;

    /**
     * Creates a {@link ResultVerifier} instance for a specific dataset and query paradigm.
     * @param dataset The path to the dataset on which the query is/was performed.
     * @param vectorisedParadigm Whether the vectorised query paradigm is used (as this influences
     *                           the shape of the result array).
     */
    public ResultVerifier(String dataset, boolean vectorisedParadigm) throws Exception {
        // Store the used paradigm
        this.vectorisedParadigm = vectorisedParadigm;

        // Extract the expected result size to construct the int[] packaging operator
        Pattern keysPattern = Pattern.compile("keys\\_\\d+");
        Matcher keysMatcher = keysPattern.matcher(dataset);
        keysMatcher.find();
        int numberKeys = Integer.parseInt(keysMatcher.group(0).split("_")[1]);

        // Initialise the arrays
        this.col1Keys = new int[numberKeys];
        this.col2Sum = new long[numberKeys];
        this.col3Sum = new long[numberKeys];
        this.col4Sum = new long[numberKeys];

        // Fill the arrays based on the expected result csv for the dataset
        Reader expectedResultCsvFileReader = new FileReader(dataset + "/expected_result.csv");
        BufferedReader expectedResultCsv = new BufferedReader(expectedResultCsvFileReader);

        CSVFormat expectedResultCsvFormat = CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .build();

        Iterable<CSVRecord> records = expectedResultCsvFormat.parse(expectedResultCsv);
        int currentIndex = 0;
        for (CSVRecord r : records) {
            this.col1Keys[currentIndex] = Integer.parseInt(r.get(0));
            this.col2Sum[currentIndex] = Long.parseLong(r.get(1));
            this.col3Sum[currentIndex] = Long.parseLong(r.get(2));
            this.col4Sum[currentIndex] = Long.parseLong(r.get(3));
            currentIndex++;
        }

        expectedResultCsv.close();
        expectedResultCsvFileReader.close();
    }

    /**
     * Method to check if the result computed in a query is indeed correct.
     * @param resultToVerify The result to check the correctness of.
     * @return {@code true} iff the result matches the expected result.
     */
    public boolean resultCorrect(long[] resultToVerify) {
        if (this.vectorisedParadigm)
            throw new UnsupportedOperationException("The ResultVerifier currently does not support vectorised paradigm results");

        // Check for each of the group keys if its result is correctly present in the result
        // Initially each element of the array is false, and will be marked true if it has been
        // found in the result
        boolean[] groupCorrectInResult = new boolean[this.col1Keys.length];

        // Check each of the results
        for (int i = 0; i < this.col1Keys.length; i++) {
            // First locate the index in the expected result of the current key in the result
            int resultKeyToCheck = (int) resultToVerify[4 * i];
            int expectedResultKeyIndex = -1;
            for (int j = 0; j < this.col1Keys.length; j++) {
                if (this.col1Keys[j] == resultKeyToCheck) {
                    expectedResultKeyIndex = j;
                    break;
                }
            }

            // Check that the sum columns match
            long sum2ResultValue = resultToVerify[4 * i + 1];
            long sum3ResultValue = resultToVerify[4 * i + 2];
            long sum4ResultValue = resultToVerify[4 * i + 3];
            groupCorrectInResult[expectedResultKeyIndex] =
                       this.col2Sum[expectedResultKeyIndex] == sum2ResultValue
                    && this.col3Sum[expectedResultKeyIndex] == sum3ResultValue
                    && this.col4Sum[expectedResultKeyIndex] == sum4ResultValue;
        }

        // Return whether all groups have been correctly found in the result
        for (boolean currentGroupCorrect : groupCorrectInResult)
            if (!currentGroupCorrect)
                return false;

        // All groups match
        return true;
    }

}
