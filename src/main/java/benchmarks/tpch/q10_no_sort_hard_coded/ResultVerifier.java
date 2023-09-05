package benchmarks.tpch.q10_no_sort_hard_coded;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to verify the result of TPC-H Q3 query based on the dataset in an unordered fashion.
 */
public class ResultVerifier {

    /**
     * {@link DateTimeFormatter} for parsing dates in the result file.
     */
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Contains the correct result;
     */
    private int[] correctResultCustKey;
    private byte[][] correctResultCName;
    private double[] correctResultRevenue;
    private double[] correctResultCAcctbal;
    private byte[][] correctResultNName;
    private byte[][] correctResultCAddress;
    private byte[][] correctResultCPhone;
    private byte[][] correctResultCComment;

    /**
     * Creates a {@link ResultVerifier} instance for a specific dataset and query paradigm.
     * @param resultFile The path to the CSV file containing the correct result.
     */
    public ResultVerifier(String resultFile) throws Exception {
        // Fill the arrays based on the expected result csv for the dataset
        Reader expectedResultCsvFileReader = new FileReader(resultFile);
        BufferedReader expectedResultCsv = new BufferedReader(expectedResultCsvFileReader);

        CSVFormat expectedResultCsvFormat = CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(false)
                .build();

        Iterable<CSVRecord> records = expectedResultCsvFormat.parse(expectedResultCsv);
        List<CSVRecord> recordList = new ArrayList<>();
        records.forEach(recordList::add);

        // Initialise the result arrays
        int resultSize = recordList.size();
        this.correctResultCustKey = new int[resultSize];
        this.correctResultCName = new byte[resultSize][];
        this.correctResultRevenue = new double[resultSize];
        this.correctResultCAcctbal = new double[resultSize];
        this.correctResultNName = new byte[resultSize][];
        this.correctResultCAddress = new byte[resultSize][];
        this.correctResultCPhone = new byte[resultSize][];
        this.correctResultCComment = new byte[resultSize][];

        for (int i = 0; i < resultSize; i++) {
            CSVRecord r = recordList.get(i);
            this.correctResultCustKey[i] = Integer.parseInt(r.get(0));
            this.correctResultCName[i] = r.get(1).getBytes(StandardCharsets.US_ASCII);
            this.correctResultRevenue[i] = Double.parseDouble(r.get(2));
            this.correctResultCAcctbal[i] = Double.parseDouble(r.get(3));
            this.correctResultNName[i] = r.get(4).getBytes(StandardCharsets.US_ASCII);
            this.correctResultCAddress[i] = r.get(5).getBytes(StandardCharsets.US_ASCII);
            this.correctResultCPhone[i] = r.get(6).getBytes(StandardCharsets.US_ASCII);
            this.correctResultCComment[i] = r.get(7).getBytes(StandardCharsets.US_ASCII);
        }

        expectedResultCsv.close();
        expectedResultCsvFileReader.close();
    }

    /**
     * Method to get the (expected) size of the result.
     * @return The number of groups that should exist in the result.
     */
    public int getResultSize() {
        return this.correctResultCustKey.length;
    }

    /**
     * Method to check if the result computed in a query is indeed correct.
     * @return {@code true} iff the result matches the expected result.
     */
    public boolean resultCorrect(
            int[] resultCustKey, byte[][] resultCName, double[] resultRevenue, double[] resultCAcctbal,
            byte[][] resultNName, byte[][] resultCAddress, byte[][] resultCPhone, byte[][] resultCComment)
    {
        if (resultCustKey.length != this.correctResultCustKey.length)
            return false;

        // Check for each of the group keys if its result is correctly present in the result
        // Initially each element of the array is false, and will be marked true if it has been
        // found in the result
        boolean[] groupCorrectInResult = new boolean[this.correctResultCustKey.length];

        for (int resultIndex = 0; resultIndex < resultCustKey.length; resultIndex++) {
            // First find the group index in the correct result
            int correctResultIndex = -1;
            for (int pcri = 0; pcri < this.correctResultCustKey.length; pcri++) {
                if (resultCustKey[resultIndex] == this.correctResultCustKey[pcri]
                        && Arrays.equals(resultCName[resultIndex], correctResultCName[pcri])
                        && Math.abs(resultCAcctbal[resultIndex] - correctResultCAcctbal[pcri]) < 1e-1
                        && Arrays.equals(resultNName[resultIndex], correctResultNName[pcri])
                        && Arrays.equals(resultCAddress[resultIndex], correctResultCAddress[pcri])
                        && Arrays.equals(resultCPhone[resultIndex], correctResultCPhone[pcri])
                        && Arrays.equals(resultCComment[resultIndex], correctResultCComment[pcri])
                ) {
                    correctResultIndex = pcri;
                    break;
                }
            }

            if (correctResultIndex == -1) {
                System.out.println("group key not found");
                return false;
            }

            // Check the revenue column
            groupCorrectInResult[correctResultIndex] =
                    (Math.abs(resultRevenue[resultIndex] - correctResultRevenue[correctResultIndex]) < 1e-2);

        }

        // Return whether all groups have been correctly found in the result
        for (boolean currentGroupCorrect : groupCorrectInResult)
            if (!currentGroupCorrect) {
                System.out.println("Group missing");
                return false;
            }

        // All groups match
        return true;
    }

}
