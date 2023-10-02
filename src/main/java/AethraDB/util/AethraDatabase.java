package AethraDB.util;

import AethraDB.evaluation.codegen.operators.CodeGenOperator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Class which wraps all functionality for planning a query over an Aethra database.
 * Aethra databases are "implemented" as folders containing Arrow IPC files.
 */
public class AethraDatabase {

    private static boolean libraryLoaded = false;

    /**
     * Method to plan and optimise a query over a given database.
     * @param databasePath The path of the database to execute the planned query over.
     * @param queryPath The path of the query file which ought to be planned.
     * @return A {@link CodeGenOperator} representing the root of the planned query.
     */
    public static CodeGenOperator planQuery(String databasePath, String queryPath) {
        // Plan the query via the native library
        if (!libraryLoaded)
            System.load("/usr/lib/AethraDB/AethraDB-Planner-Lib.so");

        long isolateThread = createIsolate();
        final String queryPlan = plan(isolateThread, databasePath, queryPath);

        // Decode the query plan into a tree of codegen operators
        return AethraQueryDecoder.decode(databasePath, queryPlan);
    }

    /**
     * Method to plan and optimise a query over a given database.
     * @param databasePath The path of the database to execute the planned query over.
     * @param queryString The String representation of the query which ought to be planned.
     * @return A {@link CodeGenOperator} representing the root of the planned query.
     */
    public static CodeGenOperator planQueryString(String databasePath, String queryString) {
        // First write the query to a temporary file
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        String tempFilename = "AethraTempQuery_" + System.nanoTime();
        File tempQueryFile;
        try {
            tempQueryFile = File.createTempFile(tempFilename, ".txt", tempDir);
            FileWriter tempQueryWriter = new FileWriter(tempQueryFile, false);
            BufferedWriter bufferedTempQueryWriter = new BufferedWriter(tempQueryWriter);
            bufferedTempQueryWriter.write(queryString);
            bufferedTempQueryWriter.close();
            tempQueryWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Plan the query
        final CodeGenOperator plannedQuery = planQuery(databasePath, tempQueryFile.getAbsolutePath());

        // Remove the temporary file
        if (!tempQueryFile.delete())
            System.out.println("Was unable to remove the temporary query file");

        // Return the planned query
        return plannedQuery;
    }

    /**
     * Method mapping for the plan method of the native planning library.
     * @param isolateThreadId Parameter for isolating the native library thread calls.
     * @param databasePath The path of the database for which the query should be planned.
     * @param queryPath The path of the query that should be planned.
     * @return A {@link String} representation of the query plan that was constructed.
     */
    private static native String plan(long isolateThreadId, final String databasePath, final String queryPath);

    /**
     * Method for mapping the native method creating an isolation identifier.
     * @return The isolation identifier.
     */
    private static native long createIsolate();

}
