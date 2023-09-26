package AethraDB.util.arrow;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

/**
 * Statistics provider class for an {@link ArrowTable}.
 */
public class ArrowTableStatistics implements Statistic {

    /**
     * Field storing the approximate row count of a table.
     */
    private final long approximateRowCount;

    /**
     * Constructs a statistics instance for a table.
     * @param approximateRowCount The approximate row count of the table.
     */
    public ArrowTableStatistics(long approximateRowCount) {
        this.approximateRowCount = approximateRowCount;
    }

    @Override
    public Double getRowCount() {
        return (double) this.approximateRowCount;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return Collections.emptyList();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return Collections.emptyList();
    }

    @Override
    public List<RelCollation> getCollations() {
        return Collections.emptyList();
    }

    @Override
    public RelDistribution getDistribution() {
        return RelDistributionTraitDef.INSTANCE.getDefault();
    }

}
