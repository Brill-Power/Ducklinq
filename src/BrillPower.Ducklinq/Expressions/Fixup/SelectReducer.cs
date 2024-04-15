using System.Collections.Generic;
using System.Linq;

namespace BrillPower.Ducklinq.Expressions.Fixup;

internal class SelectReducer : DuckDbExpressionVisitor
{
    protected override DuckDbExpression VisitSelect(SelectExpression node)
    {
        DuckDbExpression visited = base.VisitSelect(node);
        if (visited is SelectExpression visitedSelect && visitedSelect.Source is SelectExpression innerSelect)
        {
            // if the inner select has a projection to a new form, we cannot reduce
            if (innerSelect.Projections.Count > 0)
            {
                return visited;
            }

            if (innerSelect.Join is not null)
            {
                return visited;
            }

            // don't think we can simplify two groupings
            if (innerSelect.Groupings.Count > 0 && visitedSelect.Groupings.Count > 0)
            {
                return visited;
            }

            IReadOnlyCollection<DuckDbExpression> groupings = innerSelect.Groupings.Count > 0 ? innerSelect.Groupings : visitedSelect.Groupings;
            IReadOnlyCollection<AsExpression> windows = innerSelect.Windows.Count > 0 ? innerSelect.Windows : visitedSelect.Windows;
            IEnumerable<OrderExpression> orderings = innerSelect.Orderings.Concat(visitedSelect.Orderings);

            // think it's safe to AND two WHERE predicates together
            LogicalExpression? predicate = null;
            if (innerSelect.Predicate is not null && visitedSelect.Predicate is not null)
            {
                predicate = new LogicalExpression(innerSelect.Predicate, visitedSelect.Predicate, LogicalOperator.AndAlso);
            }
            else
            {
                predicate = innerSelect.Predicate ?? visitedSelect.Predicate;
            }

            return new SelectExpression(visitedSelect.ElementType, innerSelect.Alias ?? visitedSelect.Alias, innerSelect.Source, predicate, visitedSelect.Projections, groupings, windows, visitedSelect.Join, orderings, innerSelect.PartitionKeySelector);
        }
        return visited;
    }
}