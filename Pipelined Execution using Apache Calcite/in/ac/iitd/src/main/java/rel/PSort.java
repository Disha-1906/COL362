package rel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{

    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    private List<Object[]> all_rows = new ArrayList<>();
    private int idx = 0;
    private int len = 0;


    private void get_all_rows(PRel node) {
        while (node.hasNext()) {
            Object[] row = node.next();
            if (row != null) all_rows.add(row);
        }
//        System.out.println("Got all rows");
        len = all_rows.size();
    }

    private Comparator<Object[]> createHierarchicalComparator() {
        return (row1, row2) -> {
            for (RelFieldCollation rfc : collation.getFieldCollations()) {
                int index = rfc.getFieldIndex();
                Comparable val1 = (Comparable) row1[index];
                Comparable val2 = (Comparable) row2[index];
                int result;
                if (rfc.getDirection().isDescending()) result = val2.compareTo(val1);
                else result = val1.compareTo(val2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };
    }
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        PRel child = (PRel) getInput();
//        System.out.println("Child of sort is "+child);
        boolean f =child.open();
        if(!f) return false;
        get_all_rows(child);
        Comparator<Object[]> hc = createHierarchicalComparator();
        all_rows.sort(hc);
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        if(idx >= len) return false;
        return true;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        if(idx >= len) return null;
        idx++;
        return all_rows.get(idx-1);
    }

}