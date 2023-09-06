package calcite.rules;

import calcite.operators.LogicalArrowTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that converts a {@link LogicalProject} on top of a {@link LogicalFilter} that is on
 * top of a {@link LogicalArrowTableScan} without a projection into a scan which includes the
 * projection.
 */
@Value.Enclosing
public class ArrowTableScanFilterProjectRule extends RelRule<ArrowTableScanFilterProjectRule.Config> {

    /**
     * Creates an instance of the {@link ArrowTableScanFilterProjectRule}.
     */
    protected ArrowTableScanFilterProjectRule(Config config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        if (call.rels.length == 3) {
            // the ordinary variant
            final LogicalProject project = call.rel(0);
            final LogicalFilter filter = call.rel(1);
            final LogicalArrowTableScan scan = call.rel(2);
            apply(call, project, filter, scan);
        } else {
            throw new AssertionError();
        }
    }

    /**
     * Pushes down the projection, past the filter, into the {@link LogicalArrowTableScan} on a match.
     */
    protected void apply(
            RelOptRuleCall call, LogicalProject project, LogicalFilter filter, LogicalArrowTableScan scan) {
        // Check that the scan does not project anything yet, since we must prevent recursive application of the rule
        if (!scan.projects.equals(scan.identity()))
            return;

        // Collect the list of columns that are actually required by the project
        final List<Integer> projectionColumns = new ArrayList<>();

        final RexVisitorImpl<Void> projectionVisitor = new RexVisitorImpl<Void>(true) {
            @Override public Void visitInputRef(RexInputRef inputRef) {
                if (!projectionColumns.contains(inputRef.getIndex())) {
                    projectionColumns.add(inputRef.getIndex());
                }
                return null;
            }
        };
        projectionVisitor.visitEach(project.getProjects());

        // Collect the list of columns that are required by the filter
        final List<Integer> filterColumns = new ArrayList<>();

        final RexVisitorImpl<Void> filterVisitor = new RexVisitorImpl<Void>(true) {
            @Override public Void visitCall(RexCall call) {
                visitEach(call.operands);
                return null;
            }

            @Override public Void visitInputRef(RexInputRef inputRef) {
                if (!filterColumns.contains(inputRef.getIndex())) {
                    filterColumns.add(inputRef.getIndex());
                }
                return null;
            }
        };
        if (!(filter.getCondition() instanceof RexCall filterCall))
            throw new UnsupportedOperationException("ArrowTableScanFilterProjectRule expects filter conditions to be RexCall instances");
        filterVisitor.visitCall(filterCall);

        // Combine the column lists
        final List<Integer> columnsToProject = new ArrayList<>(projectionColumns);
        for (Integer filterColumn : filterColumns) {
            if (!columnsToProject.contains(filterColumn))
                columnsToProject.add(filterColumn);
        }
        columnsToProject.sort(Integer::compare);

        // Setup the replacement arrow scan
        LogicalArrowTableScan newScan = LogicalArrowTableScan.create(
                scan.getCluster(),
                scan.getTable(),
                scan.getHints(),
                columnsToProject
        );

        // Map the old projection/filter ordinals to the new column variables
        Mapping mapping = Mappings.target(columnsToProject, scan.getRowType().getFieldCount());
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());
        final RexNode newFilterRexNode = RexUtil.apply(mapping, filterCall);

        if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
            call.transformTo(
                    call.builder()
                            .push(newScan)
                            .filter(newFilterRexNode)
                            .build());
        } else {
            call.transformTo(
                    call.builder()
                            .push(newScan)
                            .filter(newFilterRexNode)
                            .project(newProjectRexNodes)
                            .build());
        }
    }

    /**
     * Config specification for the {@link ArrowTableScanFilterProjectRule}.
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {

        /**
         * Config that matches a LogicalProject on LogicalTableScan.
         */
        Config DEFAULT = ImmutableArrowTableScanFilterProjectRule.Config.builder()
                .operandSupplier(b0 ->
                        b0.operand(LogicalProject.class).oneInput(b1 ->
                                b1.operand(LogicalFilter.class).oneInput(b2 ->
                                        b2.operand(LogicalArrowTableScan.class).noInputs())))
                .build();

        @Override
        default ArrowTableScanFilterProjectRule toRule() {
            return new ArrowTableScanFilterProjectRule(this);
        }
    }

}
