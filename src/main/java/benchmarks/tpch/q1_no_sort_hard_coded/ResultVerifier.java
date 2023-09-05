package benchmarks.tpch.q1_no_sort_hard_coded;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to verify the result of TPC-H Q1 query based on the dataset in an unordered fashion.
 */
public class ResultVerifier {

    /**
     * Contains the correct result;
     */
    private final byte[][] correctResultReturnFlag;
    private final byte[][] correctResultLineStatus;
    private final double[] correctResultSumQuantity;
    private final double[] correctResultSumBasePrice;
    private final double[] correctResultSumDiscPrice;
    private final double[] correctResultSumCharge;
    private final double[] correctResultAvgQuantity;
    private final double[] correctResultAvgPrice;
    private final double[] correctResultAvgDisc;
    private final int[] correctResultCountOrder;

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
                .setIgnoreSurroundingSpaces(true)
                .build();

        Iterable<CSVRecord> records = expectedResultCsvFormat.parse(expectedResultCsv);
        List<CSVRecord> recordList = new ArrayList<>();
        records.forEach(recordList::add);

        // Initialise the result arrays
        int resultSize = recordList.size();
        this.correctResultReturnFlag = new byte[resultSize][];
        this.correctResultLineStatus = new byte[resultSize][];
        this.correctResultSumQuantity = new double[resultSize];
        this.correctResultSumBasePrice = new double[resultSize];
        this.correctResultSumDiscPrice = new double[resultSize];
        this.correctResultSumCharge = new double[resultSize];
        this.correctResultAvgQuantity = new double[resultSize];
        this.correctResultAvgPrice = new double[resultSize];
        this.correctResultAvgDisc = new double[resultSize];
        this.correctResultCountOrder = new int[resultSize];

        for (int i = 0; i < resultSize; i++) {
            CSVRecord r = recordList.get(i);
            this.correctResultReturnFlag[i] = r.get(0).getBytes(StandardCharsets.US_ASCII);
            this.correctResultLineStatus[i] = r.get(1).getBytes(StandardCharsets.US_ASCII);
            this.correctResultSumQuantity[i] = Double.parseDouble(r.get(2));
            this.correctResultSumBasePrice[i] = Double.parseDouble(r.get(3));
            this.correctResultSumDiscPrice[i] = Double.parseDouble(r.get(4));
            this.correctResultSumCharge[i] = Double.parseDouble(r.get(5));
            this.correctResultAvgQuantity[i] = Double.parseDouble(r.get(6));
            this.correctResultAvgPrice[i] = Double.parseDouble(r.get(7));
            this.correctResultAvgDisc[i] = Double.parseDouble(r.get(8));
            this.correctResultCountOrder[i] = Integer.parseInt(r.get(9));
        }

        expectedResultCsv.close();
        expectedResultCsvFileReader.close();
    }

    /**
     * Method to get the (expected) size of the result.
     * @return The number of groups that should exist in the result.
     */
    public int getResultSize() {
        return this.correctResultReturnFlag.length;
    }

    /**
     * Method to check if the result computed in a query is indeed correct.
     * @return {@code true} iff the result matches the expected result.
     */
    public boolean resultCorrect(
            byte[][] resultReturnFlag,
            byte[][] resultLineStatus,
            double[] resultSumQuantity,
            double[] resultSumBasePrice,
            double[] resultSumDiscPrice,
            double[] resultSumCharge,
            double[] resultAvgQuantity,
            double[] resultAvgPrice,
            double[] resultAvgDisc,
            int[] resultCountOrder)
    {
        if (resultReturnFlag.length != this.correctResultReturnFlag.length)
            return false;

        // Check for each of the group keys if its result is correctly present in the result
        // Initially each element of the array is false, and will be marked true if it has been
        // found in the result
        boolean[] groupCorrectInResult = new boolean[this.correctResultReturnFlag.length];

        for (int resultIndex = 0; resultIndex < resultReturnFlag.length; resultIndex++) {
            // First find the group index in the correct result
            int correctResultIndex = -1;
            for (int pcri = 0; pcri < this.correctResultReturnFlag.length; pcri++) {
                if (Arrays.equals(resultReturnFlag[resultIndex], this.correctResultReturnFlag[pcri])
                        && Arrays.equals(resultLineStatus[resultIndex], this.correctResultLineStatus[pcri])) {
                    correctResultIndex = pcri;
                    break;
                }
            }

            if (correctResultIndex == -1)
                return false;

            // Check the remaining columns
            groupCorrectInResult[correctResultIndex] =
                       resultSumQuantity[resultIndex] == correctResultSumQuantity[correctResultIndex]
                    && (Math.abs(resultSumBasePrice[resultIndex] - correctResultSumBasePrice[correctResultIndex]) < 3e-1)
                    && (Math.abs(resultSumDiscPrice[resultIndex] - correctResultSumDiscPrice[correctResultIndex]) < 3e-1)
                    && (Math.abs(resultSumCharge[resultIndex] - correctResultSumCharge[correctResultIndex]) < 3e-1)
                    && (Math.abs(resultAvgQuantity[resultIndex] - correctResultAvgQuantity[correctResultIndex]) < 3e-1)
                    && (Math.abs(resultAvgPrice[resultIndex] - correctResultAvgPrice[correctResultIndex]) < 3e-1)
                    && (Math.abs(resultAvgDisc[resultIndex] - correctResultAvgDisc[correctResultIndex])) < 3e-1
                    && resultCountOrder[resultIndex] == correctResultCountOrder[correctResultIndex];
        }

        // Return whether all groups have been correctly found in the result
        for (boolean currentGroupCorrect : groupCorrectInResult)
            if (!currentGroupCorrect)
                return false;

        // All groups match
        return true;
    }

}
