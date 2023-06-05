package evaluation.codegen.infrastructure.janino;

import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Access;
import org.codehaus.janino.Java;

import javax.annotation.Nullable;

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
    public static Java.PackageMemberClassDeclaration
    addPackageMemberClassDeclaration(
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

}
