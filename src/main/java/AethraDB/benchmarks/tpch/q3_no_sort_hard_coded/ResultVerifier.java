package AethraDB.benchmarks.tpch.q3_no_sort_hard_coded;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
     * A {@link LocalDate} representing the first UNIX day.
     */
    private final LocalDateTime day_zero = LocalDate.parse("1970-01-01", dateTimeFormatter).atStartOfDay();

    /**
     * Contains the correct result;
     */
    private final int[] correctResultLOrderKey;
    private final double[] correctResultRevenue;
    private final int[] correctResultOOrderDate;
    private final int[] correctResultOShippriority;

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
        this.correctResultLOrderKey = new int[resultSize];
        this.correctResultRevenue = new double[resultSize];
        this.correctResultOOrderDate = new int[resultSize];
        this.correctResultOShippriority = new int[resultSize];

        for (int i = 0; i < resultSize; i++) {
            CSVRecord r = recordList.get(i);
            this.correctResultLOrderKey[i] = Integer.parseInt(r.get(0));

            this.correctResultRevenue[i] = Double.parseDouble(r.get(1));

            LocalDateTime parsedDate = LocalDate.parse(r.get(2), dateTimeFormatter).atStartOfDay();
            int unixDay = (int) Duration.between(day_zero, parsedDate).toDays();
            this.correctResultOOrderDate[i] = unixDay;
            this.correctResultOShippriority[i] = Integer.parseInt(r.get(3));
        }

        expectedResultCsv.close();
        expectedResultCsvFileReader.close();
    }

    /**
     * Method to get the (expected) size of the result.
     * @return The number of groups that should exist in the result.
     */
    public int getResultSize() {
        return this.correctResultLOrderKey.length;
    }

    /**
     * Method to check if the result computed in a query is indeed correct.
     * @return {@code true} iff the result matches the expected result.
     */
    public boolean resultCorrect(
            int[] resultLOrderKey, double[] resultRevenue, int[] resultOOrderDate, int[] resultOShippriority)
    {
        if (resultLOrderKey.length != this.correctResultLOrderKey.length)
            return false;

        // Check for each of the group keys if its result is correctly present in the result
        // Initially each element of the array is false, and will be marked true if it has been
        // found in the result
        boolean[] groupCorrectInResult = new boolean[this.correctResultLOrderKey.length];

        for (int resultIndex = 0; resultIndex < resultLOrderKey.length; resultIndex++) {
            // First find the group index in the correct result
            int correctResultIndex = -1;
            for (int pcri = 0; pcri < this.correctResultLOrderKey.length; pcri++) {
                if (resultLOrderKey[resultIndex] == this.correctResultLOrderKey[pcri]
                        && resultOOrderDate[resultIndex] == this.correctResultOOrderDate[pcri]
                        && resultOShippriority[resultIndex] == this.correctResultOShippriority[pcri]
                ) {
                    correctResultIndex = pcri;
                    break;
                }
            }

            if (correctResultIndex == -1)
                return false;

            // Check the revenue column
            groupCorrectInResult[correctResultIndex] =
                    (Math.abs(resultRevenue[resultIndex] - correctResultRevenue[correctResultIndex]) < 1e-2);

        }

        // Return whether all groups have been correctly found in the result
        for (boolean currentGroupCorrect : groupCorrectInResult)
            if (!currentGroupCorrect)
                return false;

        // All groups match
        return true;
    }

}
