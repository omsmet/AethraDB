package evaluation.codegen.infrastructure.context;

import benchmarks.util.ResultConsumptionTarget;
import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.data.AllocationManager;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import evaluation.codegen.infrastructure.data.DirectAllocationManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.util.Pair;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * Class for keeping track of the context during code generation, as well as plugging in specific
 * objects during execution. That is, a {@link CodeGenContext} may keep track of instances of helper
 * classes like {@link ArrowTableReader}s that are created during code generation and plugged in
 * during execution.
 */
public class CodeGenContext implements AutoCloseable {

    /**
     * Stack for keeping track of the defined variables at different stages of the code generation process.
     */
    private final Stack<Set<String>> definedVariables;

    /**
     * Set keeping track of the defined variables at the current stage of the code generation process.
     */
    private Set<String> currentDefinedVariables;

    /**
     * List for keeping track of the names of variable to allocate before the innermost scan operator.
     */
    private List<String> scanSurroundingVariableNames;

    /**
     * List for keeping track of the statements to perform the actual allocations of the variables
     * that need to be allocated before the innermost scan operator.
     */
    private List<Java.Statement> scanSurroundingVariableDeclarations;

    /**
     * List of names of {@code scanSurroundingVariables} that need to be deallocated by the
     * {@link AllocationManager}.
     */
    private List<String> scanSurroundingVariablesToDeallocate;

    /**
     * Stack for keeping track of the ordinal to access path mapping at different stages of the code generation process.
     */
    private final Stack<List<AccessPath>> ordinalMapping;

    /**
     * List keeping track of the ordinal to access path mapping at the current stage of the code generation process.
     */
    private List<AccessPath> currentOrdinalMapping;

    /**
     * The {@link RootAllocator} instance used for a query that accesses Arrow files.
     */
    private RootAllocator arrowRootAllocator;

    /**
     * A collection of {@link ArrowTableReader} instances used by the query.
     */
    private List<ArrowTableReader> arrowTableReaders;

    /**
     * The {@link AllocationManager} used by this query.
     */
    private AllocationManager allocationManager;

    /**
     * A {@link ResultConsumptionTarget} for transferring the query result outside the generated query code.
     */
    private ResultConsumptionTarget resultConsumptionTarget;

    /**
     * Creates a new empty {@link CodeGenContext} instance.
     */
    public CodeGenContext() {
        this.definedVariables = new Stack<>();
        this.currentDefinedVariables = new HashSet<>();

        this.defineVariable("cCtx");
        this.defineVariable("oCtx");

        this.scanSurroundingVariableNames = new ArrayList<>();
        this.scanSurroundingVariableDeclarations = new ArrayList<>();
        this.scanSurroundingVariablesToDeallocate = new ArrayList<>();

        this.ordinalMapping = new Stack<>();
        this.currentOrdinalMapping = new ArrayList<>();

        this.arrowRootAllocator = null;
        this.arrowTableReaders = null;
        this.allocationManager = null;
        this.resultConsumptionTarget = null;
    }

    /**
     * Method for pushing the current code generation context so that it remains stable until it is needed again.
     */
    public void pushCodeGenContext() {
        // Push the current context
        this.definedVariables.push(this.currentDefinedVariables);
        // And create a copy which can be updated
        this.currentDefinedVariables = new HashSet<>(this.currentDefinedVariables);
    }

    /**
     * Method for popping the previous code generation context so that we can use it once more.
     */
    public void popCodeGenContext() {
        this.currentDefinedVariables = this.definedVariables.pop();
    }

    /**
     * Method for defining a new variable in the current code generation context.
     * @param preferredName The preferred name of the variable to be defined.
     * @return The actual name that was defined for the variable (to prevent name clashes).
     */
    public String defineVariable(String preferredName) {
        // Find an available name
        String actualDefinedName = preferredName;
        for (int i = 0; this.currentDefinedVariables.contains(actualDefinedName); i++) {
            actualDefinedName = preferredName + "_" + i;
        }

        // And store and return it
        this.currentDefinedVariables.add(actualDefinedName);
        return actualDefinedName;
    }

    /**
     * Method to define a scan-surrounding variable that will be allocated just before the innermost
     * scan operator and released after it.
     * This method temporarily registers the name of the variable globally to ease bookkeeping.
     * @param preferredName The preferred name of the variable to allocate.
     * @param typeToAllocate The type that the variable should get.
     * @param initialisationStatement The statement to initialise the scan surrounding variable.
     * @param deallocate Whether the {@link AllocationManager} should deallocate the variable too.
     * @return The actual name of the allocated variable.
     */
    public String defineScanSurroundingVariables(
            String preferredName,
            Java.Type typeToAllocate,
            Java.ArrayInitializerOrRvalue initialisationStatement,
            boolean deallocate
    ) {
        // Find a globally available name
        String actualName = preferredName;
        int postfix = 0;
        while (true) {
            // Check if the current name is already taken at some stage in the query processing
            boolean nameTaken = false;
            for (Set<String> definedNamesAtStage : this.definedVariables) {
                nameTaken |= definedNamesAtStage.contains(actualName);
            }

            if (nameTaken) {
                // Name is taken, add postfix and increment postfix for next attempt if needed
                actualName = preferredName + "_" + postfix;
                postfix++;
            } else {
                // Name is available
                break;
            }
        }

        // Define the name globally
        for (Set<String> definedNamesAtStage : this.definedVariables)
            definedNamesAtStage.add(actualName);
        this.currentDefinedVariables.add(actualName);

        // Schedule the variable for allocation
        this.scanSurroundingVariableNames.add(actualName);
        this.scanSurroundingVariableDeclarations.add(
                createLocalVariable(
                        getLocation(),
                        typeToAllocate,
                        actualName,
                        initialisationStatement
                )
        );

        // Schedule the variable for dealloation if necessary
        if (deallocate)
            this.scanSurroundingVariablesToDeallocate.add(actualName);

        // Return the allocated variable's name
        return actualName;
    }

    /**
     * Method for obtaining the list of scan-surrounding variable to allocate through the given
     * statements and the list of scan-surrounding variables to deallocate through the allocation
     * manager. The caller of the method assumes the responsibility for making sure the
     * variables are allocated and also discarded via the allocation manager.
     * The context is also cleaned again of the remaining global names.
     * @return The list of statements to allocate the scan surrounding variables and the list of the
     * variable names that need to be deallocated.
     */
    public Pair<List<Java.Statement>, List<String>> getScanSurroundingVariables() {
        // Obtain the allocation statements and deallocation list
        List<Java.Statement> allocationStatements = this.scanSurroundingVariableDeclarations;
        List<String> deallocationVariables = this.scanSurroundingVariablesToDeallocate;

        // Clean the context of parent "scopes"
        for (String nameToClean : this.scanSurroundingVariableNames) {
            for (Set<String> definedNamesAtStage : this.definedVariables) {
                definedNamesAtStage.remove(nameToClean);
            }
        }

        // Reset the scan surrounding variables variables
        this.scanSurroundingVariableNames = new ArrayList<>();
        this.scanSurroundingVariableDeclarations = new ArrayList<>();
        this.scanSurroundingVariablesToDeallocate = new ArrayList<>();

        // Return the allocation statements
        return new Pair<>(allocationStatements, deallocationVariables);
    }

    /**
     * Obtain the {@link RootAllocator} to be able to process Arrow files.
     * @return The {@link RootAllocator} belonging to this query.
     */
    public RootAllocator getArrowRootAllocator() {
        if (this.arrowRootAllocator == null)
            this.arrowRootAllocator = new RootAllocator();
        return this.arrowRootAllocator;
    }

    /**
     * Method for adding an {@link ArrowTableReader} to the context represented by {@code this}.
     * @param arrowReader The {@link ArrowTableReader} to add.
     * @return The index of the added {@link ArrowTableReader} in the context.
     */
    public int addArrowReader(ArrowTableReader arrowReader) {
        if (this.arrowTableReaders == null)
            this.arrowTableReaders = new ArrayList<>();

        this.arrowTableReaders.add(arrowReader);
        return this.arrowTableReaders.size() - 1;
    }

    /**
     * Method for obtaining all {@link ArrowTableReader}s belonging to the query.
     * @return The list of {@link ArrowTableReader}s belonging to the query.
     */
    public List<ArrowTableReader> getArrowReaders() {
        return this.arrowTableReaders;
    }

    /**
     * Method for obtaining a specific {@link ArrowTableReader} belonging to the query.
     * @param index The index of the {@link ArrowTableReader} to return.
     * @return The {@link ArrowTableReader} corresponding to the given index.
     */
    public ArrowTableReader getArrowReader(int index) {
        return this.arrowTableReaders.get(index);
    }

    /**
     * Method for obtaining the {@link AllocationManager} of this {@link CodeGenContext}.
     * @return The {@link AllocationManager} of this {@link CodeGenContext}.
     */
    public AllocationManager getAllocationManager() {
        if (this.allocationManager == null)
            this.allocationManager = new DirectAllocationManager();
        return this.allocationManager;
    }

    /**
     * Method for setting the {@link ResultConsumptionTarget} of this {@link CodeGenContext}.
     * @param resultConsumptionTarget The {@link ResultConsumptionTarget} to set.
     */
    public void setResultConsumptionTarget(ResultConsumptionTarget resultConsumptionTarget) {
        this.resultConsumptionTarget = resultConsumptionTarget;
    }

    /**
     * Method to obtain the {@link ResultConsumptionTarget} of this {@link CodeGenContext}.
      * @return The {@link ResultConsumptionTarget} of this {@link CodeGenContext}.
     */
    public ResultConsumptionTarget getResultConsumptionTarget() {
        if (this.resultConsumptionTarget == null)
            throw new IllegalStateException("The ResultConsumptionTarget has not been set on this CodeGenContext");
        return this.resultConsumptionTarget;
    }

    /**
     * Method to push the current ordinal to access path mapping so it remains stable until it is needed again.
     */
    public void pushOrdinalMapping() {
        // Push the current ordinal mapping
        this.ordinalMapping.push(this.currentOrdinalMapping);
        // And create a copy which can be updated
        this.currentOrdinalMapping = new ArrayList<>(this.currentOrdinalMapping);
    }

    /**
     * Method for popping the previous ordinal to access path mapping so we can use it again.
     */
    public void popOrdinalMapping() {
        this.currentOrdinalMapping = this.ordinalMapping.pop();
    }

    /**
     * Set the current ordinal to access path mapping.
     * @param accessPathPerOrdinal The new ordinal to access path mapping to use.
     */
    public void setCurrentOrdinalMapping(List<AccessPath> accessPathPerOrdinal) {
        this.currentOrdinalMapping = accessPathPerOrdinal;
    }

    /**
     * Method for obtaining the current ordinal to access path mapping.
     * @return The current ordinal to access path mapping.
     */
    public List<AccessPath> getCurrentOrdinalMapping() {
        return this.currentOrdinalMapping;
    }

    @Override
    public void close() throws Exception {
        if (this.arrowTableReaders != null)
            for (ArrowTableReader reader : this.arrowTableReaders)
                reader.close();

        if (this.arrowRootAllocator != null)
            this.arrowRootAllocator.close();
    }

}
