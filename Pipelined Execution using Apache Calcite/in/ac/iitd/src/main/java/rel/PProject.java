package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                         List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        PRel child = (PRel) getInput();
//        System.out.println("Child of project is  "+child);
        boolean f = child.open();
        return f;
//        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        return;
    }


    private Object[] cached_row;
    private int flag = 0;
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        PRel child = (PRel) getInput();
        while(child.hasNext()){
            Object[] r = child.next();
            if(r==null) continue;
            cached_row = r;
            flag = 1;
            return true;
        }
        return false;
    }


    private Object compute_operand(RexNode op, Object[] r) {
        if (op instanceof RexInputRef) {
            int i = ((RexInputRef) op).getIndex();
            if (i >= 0 && i < r.length) return r[i];
            throw new IllegalArgumentException("Invalid column index: " + i);
        }
        return null;
    }
    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
        Object[] r;
        if(flag == 1){ r = cached_row; flag=0;}
        else{ r = ((PRel)getInput()).next(); }
        if(r == null) return null;
        List<RexNode> projects = getProjects();
        Object[] projectedRow = new Object[projects.size()];
        for (int i = 0; i < projects.size(); i++) {
            RexNode projectExpr = projects.get(i);
            Object value = compute_operand(projectExpr, r);
            projectedRow[i] = value;
        }
        return projectedRow;
    }
}