package AethraDB;

import AethraDB.evaluation.codegen.QueryCodeGenerator;
import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import AethraDB.evaluation.codegen.operators.QueryResultCountOperator;
import AethraDB.evaluation.codegen.operators.QueryResultPrinterOperator;
import AethraDB.evaluation.general_support.QueryCodePrinter;
import AethraDB.util.AethraDatabase;
import AethraDB.util.JNI.JNIEnv;
import org.apache.arrow.memory.RootAllocator;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.Pointer;

import java.util.Arrays;
import java.util.List;

/**
 * This class defines the entry point for the AethraDB code generation library which, given a
 * database instance and query, can perform query planning, code generation and code compilation.
 * This entry point is used via a native library to speed up this process.
 */
public class AethraDBGenerator {

    /**
     * Field which contains the planned query after {@code internalPlan} has been executed.
     */
    private static CodeGenOperator queryRootOperator;

    /**
     * Field which contains the {@link QueryCodeGenerator} after {@code internalCodegen} has been executed.
     */
    private static QueryCodeGenerator queryCodeGenerator;

    /**
     * Field which contains the generated query class after {@code internalCodegen} has been executed.
     */
    private static Java.PackageMemberClassDeclaration generatedQueryClass;

    /**
     * This entrypoint exists to perform debugging on the library and should not be used by the
     * native image.
     * @param args The library expects to be given the path to a database, the path to a query file,
     *             a boolean indicating whether the vectorised query processing paradigm should be used
     *             and a boolean indicating whether the result should be summarised.
     */
    public static void main(String[] args) throws Exception {
        String databasePath = args[0];
        String queryPath = args[1];
        boolean useVectorised = Boolean.parseBoolean(args[2]);
        boolean summariseResult = Boolean.parseBoolean(args[3]);
        internalPlan(databasePath, queryPath);
        internalCodegen(useVectorised, summariseResult);

        System.out.println("[Generated query code]");
        QueryCodePrinter.printCode((List<Java.Statement>) generatedQueryClass.getMethodDeclaration("execute").statements);
        System.out.println();

        String runDescriptor = internalCompile();
        System.out.println("[Run descriptor]");
        System.out.println(runDescriptor);
    }

    /**
     * Method which forwards query planning to the AethraDB Planner Library.
     * @param databasePath The path of the database to plan the query over.
     * @param queryPath The path of the file containing the query to plan.
     */
    private static void internalPlan(String databasePath, String queryPath) {
        queryRootOperator = AethraDatabase.planQuery(databasePath, queryPath);
    }

    /**
     * Entry point for the native image library to {@code internalPlan}.
     */
    @CEntryPoint(name = "Java_AethraDB_util_AethraGenerator_plan")
    public static void plan(JNIEnv jniEnv, Pointer clazz, IsolateThread isolateThread, JNIEnv.JString rawDatabasePath, JNIEnv.JString rawQueryPath) throws Exception {
        JNIEnv.JNINativeInterface fn = jniEnv.getFunctions();
        CCharPointer cDatabasePathPointer = fn.getGetStringUTFChars().call(jniEnv, rawDatabasePath, (byte) 0);
        String databasePath = CTypeConversion.toJavaString(cDatabasePathPointer);

        CCharPointer cQueryPathPointer = fn.getGetStringUTFChars().call(jniEnv, rawQueryPath, (byte) 0);
        String queryPath = CTypeConversion.toJavaString(cQueryPathPointer);

        internalPlan(databasePath, queryPath);
    }

    /**
     * Method which executes the steps required to perform code generation.
     * @param useVectorisedProcessing Whether to use vectorised query processing (true)
     *                                or data-centric query processing (false).
     * @param summariseResultAsCount Whether to only return the number of results,
     *                               instead of the actual results.
     */
    private static void internalCodegen(boolean useVectorisedProcessing, boolean summariseResultAsCount) throws Exception {
        // Instantiate helper objects
        RootAllocator rootAllocator = new RootAllocator();
        CodeGenContext cCtx = new CodeGenContext(rootAllocator);
        OptimisationContext oCtx = new OptimisationContext();

        // Wrap the root operator in the required summarisation and print operators
        if (summariseResultAsCount) {
            queryRootOperator = new QueryResultCountOperator(queryRootOperator);
        }
        queryRootOperator = new QueryResultPrinterOperator(queryRootOperator);

        // Generate code
        queryCodeGenerator = new QueryCodeGenerator(cCtx, oCtx, queryRootOperator, useVectorisedProcessing);
        generatedQueryClass = queryCodeGenerator.generateQueryClass();
    }

    /**
     * Entry point for the native image library to {@code internalCodegen}.
     */
    @CEntryPoint(name = "Java_AethraDB_util_AethraGenerator_codeGen")
    public static void codeGen(JNIEnv jniEnv, Pointer clazz, IsolateThread isolateThread, boolean useVectorisedProcessing, boolean summariseResultAsCount) throws Exception {
        internalCodegen(useVectorisedProcessing, summariseResultAsCount);
    }

    /**
     * Method which executes the steps required to perform code compilation.
     * @return A string containing the location of the compiled class files, as well as
     * -- for each arrow table -- the path of the file to read, whether it projects columns, and
     * which columns to project
     */
    private static String internalCompile() throws CompileException {
        final String classFilesDirectory = queryCodeGenerator.compileQueryClass();
        String runDescriptor = classFilesDirectory + "\n";
        for (ArrowTableReader arrowTableReader : queryCodeGenerator.getCCtx().getArrowReaders()) {
            runDescriptor += arrowTableReader.getArrowFile().getAbsolutePath() + ";";
            runDescriptor += arrowTableReader.projectsColumns() + ";";
            runDescriptor += Arrays.toString(arrowTableReader.getColumnsToProject()) + "\n";
        }

        return runDescriptor;
    }

    /**
     * Entry point for the native image library to {@code internalCompile}.
     */
    @CEntryPoint(name = "Java_AethraDB_util_AethraGenerator_compile")
    public static JNIEnv.JString compile(JNIEnv jniEnv, Pointer clazz, IsolateThread isolateThread) throws Exception {
        JNIEnv.JNINativeInterface fn = jniEnv.getFunctions();
        final String runDescriptor = internalCompile();
        try (final CTypeConversion.CCharPointerHolder holder = CTypeConversion.toCString(runDescriptor)) {
            return fn.getNewStringUTF().call(jniEnv, holder.get());
        }
    }

    @CEntryPoint(name = "Java_AethraDB_util_AethraGenerator_createIsolate", builtin=CEntryPoint.Builtin.CREATE_ISOLATE)
    public static native IsolateThread createIsolate();

}
