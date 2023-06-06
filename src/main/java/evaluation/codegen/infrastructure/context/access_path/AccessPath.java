package evaluation.codegen.infrastructure.context.access_path;

import org.codehaus.janino.Java;

/**
 * Class which keeps track of information of how to access ordinals of the logical plan during
 * the code generation process.
 */
public abstract class AccessPath {

    /**
     * Method which performs code generation so that read access to the current ordinal is possible.
     * @return A piece of code to read this ordinal.
     */
    public abstract Java.Rvalue read();

}
