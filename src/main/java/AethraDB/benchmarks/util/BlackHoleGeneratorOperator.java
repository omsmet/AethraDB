package AethraDB.benchmarks.util;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;
import AethraDB.evaluation.codegen.infrastructure.context.OptimisationContext;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.AccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrayVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithSelectionVectorAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ArrowVectorWithValidityMaskAccessPath;
import AethraDB.evaluation.codegen.infrastructure.context.access_path.ScalarVariableAccessPath;
import AethraDB.evaluation.codegen.operators.CodeGenOperator;
import org.apache.calcite.rel.RelNode;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoClassGen.createClassInstance;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createAmbiguousNameRef;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createReferenceType;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.createStringLiteral;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoGeneralGen.getLocation;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoMethodGen.createMethodInvocationStm;
import static AethraDB.evaluation.codegen.infrastructure.janino.JaninoVariableGen.createLocalVariable;

/**
 * {@link CodeGenOperator} which generates a {@link org.openjdk.jmh.infra.Blackhole} that consumes
 * the current ordinal mapping to ensure that the compiler does not optimise the query result away.
 */
public class BlackHoleGeneratorOperator extends CodeGenOperator<RelNode> {

    /**
     * The {@link CodeGenOperator} producing the query result that is to be transferred to a benchmark.
     */
    private final CodeGenOperator<?> child;

    /**
     * The name of the {@link org.openjdk.jmh.infra.Blackhole} variable to use.
     */
    private String blackholeName;

    /**
     * Initialises a new instance of a specific {@link BlackHoleGeneratorOperator}.
     * @param logicalSubplan The logical sub-plan that should be implemented by this operator.
     * @param child The {@link CodeGenOperator} producing the actual query result.
     */
    public BlackHoleGeneratorOperator(RelNode logicalSubplan, CodeGenOperator<?> child) {
        super(logicalSubplan, false);
        this.child = child;
        this.child.setParent(this);
    }

    @Override
    public boolean canProduceNonVectorised() {
        // Since the BlackHoleGeneratorOperator operator will never produce a result for its
        // "parent" (as it will not have a parent) but rather print its result to the standard output
        // we say that it can produce both the vectorised and non-vectorised type.
        return true;
    }

    @Override
    public boolean canProduceVectorised() {
        // Since the BlackHoleGeneratorOperator operator will never produce a result for its
        // "parent" (as it will not have a parent) but rather print its result to the standard output
        // we say that it can produce both the vectorised and non-vectorised type.
        return true;
    }

    @Override
    public List<Java.Statement> produceNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Create the blackhole
        this.blackholeName = cCtx.defineVariable("blackhole");
        Java.Type blackholeType = createReferenceType(getLocation(), "org.openjdk.jmh.infra.Blackhole");
        codegenResult.add(
                createLocalVariable(
                        getLocation(),
                        blackholeType,
                        this.blackholeName,
                        createClassInstance(
                                getLocation(),
                                blackholeType,
                                new Java.Rvalue[] {
                                        createStringLiteral(getLocation(), "\"Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.\"")
                                }
                        )
                )
        );

        // Then forward the call to the child operator to get the results of the query
        codegenResult.addAll(this.child.produceNonVec(cCtx, oCtx));

        return codegenResult;
    }

    @Override
    public List<Java.Statement> consumeNonVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Simply obtain the access path from the ordinal mapping and consume the value of each of them
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();

        // Generate the required consumption statements
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {
            Java.Rvalue consumptionValue = getRValueFromOrdinalAccessPathNonVec(cCtx, i, codegenResult);

            codegenResult.add(
                    createMethodInvocationStm(
                            getLocation(),
                            createAmbiguousNameRef(getLocation(), this.blackholeName),
                            "consume",
                            new Java.Rvalue[] { consumptionValue }
                    )
            );
        }

        return codegenResult;
    }

    @Override
    public List<Java.Statement> produceVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Create the blackhole
        this.blackholeName = cCtx.defineVariable("blackhole");
        Java.Type blackholeType = createReferenceType(getLocation(), "org.openjdk.jmh.infra.Blackhole");
        codegenResult.add(
                createLocalVariable(
                        getLocation(),
                        blackholeType,
                        this.blackholeName ,
                        createClassInstance(
                                getLocation(),
                                blackholeType,
                                new Java.Rvalue[] {
                                        createStringLiteral(getLocation(), "\"Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.\"")
                                }
                        )
                )
        );

        // Then forward the call to the child operator to get the results of the query
        codegenResult.addAll(this.child.produceVec(cCtx, oCtx));

        return codegenResult;
    }

    @Override
    public List<Java.Statement> consumeVec(CodeGenContext cCtx, OptimisationContext oCtx) {
        List<Java.Statement> codegenResult = new ArrayList<>();

        // Simply obtain the access path from the ordinal mapping and consume each column's vectors
        List<AccessPath> currentOrdinalMapping = cCtx.getCurrentOrdinalMapping();
        for (int i = 0; i < currentOrdinalMapping.size(); i++) {

            // Differentiate by type of result
            AccessPath ordinalAccessPath = currentOrdinalMapping.get(i);
            if (ordinalAccessPath instanceof ArrowVectorAccessPath avap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avap.read() }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrowVectorWithSelectionVectorAccessPath avwsvap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avwsvap.readArrowVector() }
                        )
                );

                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] {avwsvap.readSelectionVectorLength() }
                        )
                );

                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avwsvap.readSelectionVector() }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrowVectorWithValidityMaskAccessPath avwvmap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avwvmap.readArrowVector() }
                        )
                );

                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avwvmap.readValidityMaskLength() }
                        )
                );

                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avwvmap.readValidityMask() }
                        )
                );

            } else if (ordinalAccessPath instanceof ArrayVectorAccessPath avap) {
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avap.getVectorVariable().read() }
                        )
                );

                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { avap.getVectorLengthVariable().read() }
                        )
                );

            } else if (ordinalAccessPath instanceof ScalarVariableAccessPath svap) {
                // This should only occur for aggregation queries in the vectorised paradigm
                codegenResult.add(
                        createMethodInvocationStm(
                                getLocation(),
                                createAmbiguousNameRef(getLocation(), this.blackholeName),
                                "consume",
                                new Java.Rvalue[] { svap.read() }
                        )
                );

            } else {
                throw new UnsupportedOperationException("BlackHoleGeneratorOperator.consumeVec does not support this ordinal type");
            }
        }

        return codegenResult;
    }

}
