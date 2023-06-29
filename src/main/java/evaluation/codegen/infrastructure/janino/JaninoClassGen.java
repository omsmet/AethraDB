package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import javax.annotation.Nullable;

import static evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;

/**
 * Class containing helper methods for generating class-related code with Janino.
 */
public class JaninoClassGen {

    /**
     * Prevent the class from being instantiated.
     */
    private JaninoClassGen() {

    }

    /**
     * Add a class definition to a specific compilation unit.
     * Does not currently support modifiers like static, abstract, final etc.
     * @param location The location at which the class definition is requested for generation.
     * @param accessModifier The access modifier for the new class(e.g. public, protected, etc.)
     * @param targetUnit The {@link Java.CompilationUnit} in which the class should be created.
     * @param className The name of the class being generated.
     * @param extendsType An optional super-class of the class being generated.
     * @return a {@link Java.PackageMemberClassDeclaration} representing the generated class.
     */
    public static Java.PackageMemberClassDeclaration addPackageMemberClassDeclaration(
            Location location,
            Access accessModifier,
            Java.CompilationUnit targetUnit,
            String className,
            @Nullable Java.Type extendsType
    ) {
        // Separate the package and class name
        String cn  = className;
        int idx = cn.lastIndexOf('.');
        if (idx != -1) {
            targetUnit.setPackageDeclaration(new Java.PackageDeclaration(location, cn.substring(0, idx)));
            cn = cn.substring(idx + 1);
        }

        Java.PackageMemberClassDeclaration tlcd = new Java.PackageMemberClassDeclaration(
                location,                                                                           // location
                null,                                                                               // docComment currently not used
                new Java.Modifier[] {                                                               // modifiers
                        new Java.AccessModifier(accessModifier.toString(), location) },
                cn,                                                                                 // name
                null,                                                                               // typeParameters currently not used
                extendsType,                                                                        // extendedType
                new Java.Type[0]                                                                    // implementedTypes currently not used
        );

        targetUnit.addPackageMemberTypeDeclaration(tlcd);
        return tlcd;
    }

    /**
     * Method for creating a local class declaration.
     * @param location The location from which the class declaration is requested for generation.
     * @param modifiers The modifiers for the new class(e.g. public, protected, etc.)
     * @param className The name of the class being generated.
     * @return A {@link Java.LocalClassDeclaration} corresponding to the provided parameters.
     */
    public static Java.LocalClassDeclaration createLocalClassDeclaration(
            Location location,
            Java.Modifier[] modifiers,
            String className
    ) {
        return new Java.LocalClassDeclaration(
                location,
                null,
                modifiers,
                className,
                null,
                null,
                new Java.Type[0]
        );
    }

    /**
     * Method for creating a local class declaration statement from a local class declaration.
     * @param localClassDeclaration The {@link org.codehaus.janino.Java.LocalClassDeclaration} for
     *                              which the statement should be created.
     * @return A {@link org.codehaus.janino.Java.LocalClassDeclarationStatement} corresponding to
     * {@code localClassDeclaration}.
     */
    public static Java.LocalClassDeclarationStatement createLocalClassDeclarationStm(
            Java.LocalClassDeclaration localClassDeclaration
    ) {
        return new Java.LocalClassDeclarationStatement(localClassDeclaration);
    }

    /**
     * Method to create a "public final" field on a class.
     * @param location The location from which the field is requested for generation.
     * @param fieldType The type that the field will store.
     * @param fieldDeclarator The declaration of the field itself.
     * @return A {@link org.codehaus.janino.Java.FieldDeclaration} matching the provided parameters.
     */
    public static Java.FieldDeclaration createPublicFinalFieldDeclaration(
            Location location,
            Java.Type fieldType,
            Java.VariableDeclarator fieldDeclarator
    ) {
        return new Java.FieldDeclaration(
                location,
                null,
                new Java.Modifier[] {
                        new Java.AccessModifier(Access.PUBLIC.toString(), getLocation()),
                        new Java.AccessModifier("final", getLocation())
                },
                fieldType,
                new Java.VariableDeclarator[] { fieldDeclarator }
        );
    }

    /**
     * Method to add a new class instance.
     * @param location The location from which the class instance creation is requested for generation.
     * @param classType The type of the class for which an instance should be created.
     * @return A {@link Java.NewClassInstance} corresponding to the class instance creation.
     */
    public static Java.NewClassInstance createClassInstance(Location location, Java.Type classType) {
        return createClassInstance(location, classType, new Java.Rvalue[]{});
    }

    /**
     * Method to add a new class instance.
     * @param location The location from which the class instance creation is requested for generation.
     * @param classType The type of the class for which an instance should be created.
     * @param arguments The arguments required for the constructor invocation.
     * @return A {@link Java.NewClassInstance} corresponding to the class instance creation.
     */
    public static Java.NewClassInstance createClassInstance(
            Location location,
            Java.Type classType,
            Java.Rvalue[] arguments
    ) {
        return new Java.NewClassInstance(location, null, classType, arguments);
    }

}
