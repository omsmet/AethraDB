package evaluation.codegen.infrastructure.context;

import evaluation.codegen.infrastructure.context.access_path.AccessPath;
import evaluation.codegen.infrastructure.data.ArrowTableReader;
import org.apache.arrow.memory.RootAllocator;

import java.util.*;

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
     * Creates a new empty {@link CodeGenContext} instance.
     */
    public CodeGenContext() {
        this.definedVariables = new Stack<>();
        this.currentDefinedVariables = new HashSet<>();

        this.defineVariable("cCtx");
        this.defineVariable("oCtx");

        this.ordinalMapping = new Stack<>();
        this.currentOrdinalMapping = new ArrayList<>();

        this.arrowRootAllocator = null;
        this.arrowTableReaders = null;
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
     * Method for obtaining a specific {@link ArrowTableReader} belonging to the query.
     * @param index The index of the {@link ArrowTableReader} to return.
     * @return The {@link ArrowTableReader} corresponding to the given index.
     */
    public ArrowTableReader getArrowReader(int index) {
        return this.arrowTableReaders.get(index);
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
