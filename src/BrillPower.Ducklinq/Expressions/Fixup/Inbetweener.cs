namespace BrillPower.Ducklinq.Expressions.Fixup;

internal class Inbetweener : DuckDbExpressionVisitor
{
    protected override DuckDbExpression VisitLogical(LogicalExpression node)
    {
        DuckDbExpression gauche = Visit(node.Left);
        DuckDbExpression droite = Visit(node.Right);
        if (node.Operator == LogicalOperator.AndAlso && gauche is LogicalExpression left && droite is LogicalExpression right && left.Left.Equals(right.Left))
        {
            if (left.Operator == LogicalOperator.GreaterThanOrEqual && right.Operator == LogicalOperator.LessThanOrEqual)
            {
                return new LogicalExpression(left.Left, new LogicalExpression(left.Right, right.Right, LogicalOperator.AndAlso), LogicalOperator.Between);
            }
            else if (right.Operator == LogicalOperator.GreaterThanOrEqual && left.Operator == LogicalOperator.LessThanOrEqual)
            {
                return new LogicalExpression(right.Left, new LogicalExpression(right.Right, left.Right, LogicalOperator.AndAlso), LogicalOperator.Between);
            }
        }
        return new LogicalExpression(gauche, droite, node.Operator);
    }
}