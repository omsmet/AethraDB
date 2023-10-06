package AethraDB.util;

import AethraDB.AethraDB;
import AethraDB.evaluation.codegen.GeneratedQuery;
import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader;
import org.apache.arrow.memory.RootAllocator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Class which wraps all functionality for the AethraDBGenerator library.
 */
public class AethraGenerator {

    private static boolean libraryLoaded = false;

    /**
     * Method to plan a query, generate code from it, and compile the query.
     * @param rootAllocator The {@link RootAllocator} that will be used for executing the query.
     * @param databasePath The path of the database to execute the planned query over.
     * @param queryPath The path of the query file which ought to be planned.
     * @param useVectorisedProcessing Whether to use vectorised query processing (true)
     *                                or data-centric query processing (false).
     * @param summariseResultAsCount Whether to only return the number of results,
     *                               instead of the actual results.
     * @return A {@link GeneratedQuery} representing the root of the planned query.
     */
    public static GeneratedQuery planGenerateCompileQuery(
            RootAllocator rootAllocator,
            String databasePath,
            String queryPath,
            boolean useVectorisedProcessing,
            boolean summariseResultAsCount
    ) throws Exception {
        // Load the library
        if (!libraryLoaded) {
            System.load("/home/olivier/Repositories/AethraDB/lib/AethraDB-Lib.so");
            libraryLoaded = true;
        }

        // Create the library isolation ID
        long isolateThread = createIsolate();

        // Plan the query
        AethraDB.queryPlanningStart = System.nanoTime();
        plan(isolateThread, databasePath, queryPath);
        AethraDB.queryPlanningEnd = System.nanoTime();

        // Perform code generation
        AethraDB.codeGenerationStart = System.nanoTime();
        codeGen(isolateThread, useVectorisedProcessing, summariseResultAsCount);
        AethraDB.codeGenerationEnd = System.nanoTime();

        // Perform compilation
        AethraDB.codeCompilationStart = System.nanoTime();
        final String runDescriptor = compile(isolateThread);

        // Create the context with the appropriate data readers
        final String[] runDescriptorLines = runDescriptor.split("\n");
        CodeGenContext cCtx = new CodeGenContext(rootAllocator);
        OptimisationContext oCtx = new OptimisationContext();

        for (int i = 1; i < runDescriptorLines.length; i++) {
            final String arrowLine = runDescriptorLines[i];
            final String[] arrowDescription = arrowLine.split(";");

            File arrowFile = new File(arrowDescription[0]);
            boolean isProjectingReader = Boolean.parseBoolean(arrowDescription[1]);
            String projectionDescription = arrowDescription[2];
            String[] columnIndices = projectionDescription.substring(1, projectionDescription.length() - 1).split(", ");
            int[] projectionColumns = new int [columnIndices.length];
            for (int c = 0; c < columnIndices.length; c++) {
                projectionColumns[c] = Integer.parseInt(columnIndices[c]);
            }

            ArrowTableReader reader = new ABQArrowTableReader(
                    arrowFile,
                    cCtx.getArrowRootAllocator(),
                    isProjectingReader,
                    projectionColumns
            );
            cCtx.addArrowReader(reader);
        }

        // Load the classes from disk
        File temporaryFolder = new File(runDescriptorLines[0]);
        temporaryFolder.deleteOnExit();
        URLClassLoader classLoader = new URLClassLoader(
                new URL[]{ temporaryFolder.toURI().toURL() },
                AethraDB.class.getClassLoader());

        Class<?> generatedQueryClass = null;
        for (File classFile : temporaryFolder.listFiles()) {
            classFile.deleteOnExit();
            String filename = classFile.getName();

            if (!filename.contains(".class"))
                continue;

            String className = filename.replace(".class", "");
            Class<?> loadedClass = classLoader.loadClass(className);

            if (className.contains("GeneratedQuery_") && !className.contains("$")) {
                generatedQueryClass = loadedClass;
            }
        }
        if (generatedQueryClass == null)
            throw new RuntimeException("Could not load the generated query class from disk");

        // Create instance of the generated query class
        Constructor<?> generatedQueryConstructor =
                generatedQueryClass.getDeclaredConstructor(CodeGenContext.class, OptimisationContext.class);
        GeneratedQuery generatedQueryInstance =
                (GeneratedQuery) generatedQueryConstructor.newInstance(cCtx, oCtx);

        AethraDB.codeCompilationEnd = System.nanoTime();
        return generatedQueryInstance;
    }

    /**
     * Method to plan a query (given by a String), generate code from it, and compile the query.
     * @param rootAllocator The {@link RootAllocator} that will be used for executing the query.
     * @param databasePath The path of the database to execute the planned query over.
     * @param queryString The string representing the query which ought to be planned.
     * @param useVectorisedProcessing Whether to use vectorised query processing (true)
     *                                or data-centric query processing (false).
     * @param summariseResultAsCount Whether to only return the number of results,
     *                               instead of the actual results.
     * @return A {@link GeneratedQuery} representing the root of the planned query.
     */
    public static GeneratedQuery planGenerateCompileQueryString(
            RootAllocator rootAllocator,
            String databasePath,
            String queryString,
            boolean useVectorisedProcessing,
            boolean summariseResultAsCount
    ) throws Exception {
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
        final GeneratedQuery generatedQuery = planGenerateCompileQuery(
                rootAllocator,
                databasePath,
                tempQueryFile.getAbsolutePath(),
                useVectorisedProcessing,
                summariseResultAsCount
        );

        // Remove the temporary file
        if (!tempQueryFile.delete())
            System.out.println("Was unable to remove the temporary query file");

        // Return the planned query
        return generatedQuery;
    }

    /**
     * Method mapping for the plan method of the native generator library.
     * @param isolateThreadId Parameter for isolating the native library thread calls.
     * @param databasePath The path of the database for which the query should be planned.
     * @param queryPath The path of the query that should be planned.
     */
    private static native void plan(long isolateThreadId, final String databasePath, final String queryPath);

    /**
     * Method mapping for the codeGen method of the native generator library.
     * @param isolateThreadId Parameter for isolating the native library thread calls.
     * @param useVectorisedProcessing Whether to use vectorised query processing (true)
     *                                or data-centric query processing (false).
     * @param summariseResultAsCount Whether to only return the number of results,
     *                               instead of the actual results.
     */
    private static native void codeGen(long isolateThreadId, boolean useVectorisedProcessing, boolean summariseResultAsCount);

    /**
     * Method mapping for the compile method of the native generator library.
     * @return A {@link String} containing the run descriptor for the generated query classes.
     */
    private static native String compile(long isolateThreadId);

    /**
     * Method for mapping the native method creating an isolation identifier.
     * @return The isolation identifier.
     */
    private static native long createIsolate();

}
