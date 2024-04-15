using System;
using System.Linq;

namespace BrillPower.Ducklinq.Expressions;

public class DuckDbExpressionVisitor
{
    public DuckDbExpression Visit(DuckDbExpression node)
    {
        switch (node)
        {
            case ArithmeticExpression arithmetic:
                return VisitArithmetic(arithmetic);
            case LiteralExpression literal:
                return VisitLiteral(literal);
            case SelectExpression select:
                return VisitSelect(select);
            case ParameterReferenceExpression parameterReference:
                return VisitParameterReference(parameterReference);
            case ColumnReferenceExpression columnReference:
                return VisitColumnReference(columnReference);
            case LogicalExpression logical:
                return VisitLogical(logical);
            case AsExpression @as:
                return VisitAs(@as);
            case OverExpression over:
                return VisitOver(over);
            case CallExpression call:
                return VisitCall(call);
            case IntervalExpression interval:
                return VisitInterval(interval);
            case WindowExpression window:
                return VisitWindow(window);
            case OrderExpression order:
                return VisitOrder(order);
            case CaseExpression @case:
                return VisitCase(@case);
            case EmptyExpression empty:
                return VisitEmpty(empty);
            case UnionExpression ulster:
                return VisitUnion(ulster);
            case CoalesceExpression coalesce:
                return VisitCoalesce(coalesce);
            default:
                throw new NotSupportedException($"Nodes of type {node.GetType().Name} are not supported.");
        }
    }

    private DuckDbExpression VisitUnion(UnionExpression ulster)
    {
        SelectExpression left = (SelectExpression)Visit(ulster.Left);
        SelectExpression right = (SelectExpression)Visit(ulster.Right);
        return new UnionExpression(left, right, ulster.All);
    }

    protected virtual DuckDbExpression VisitOver(OverExpression over)
    {
        return new OverExpression(Visit(over.Operand), over.Alias);
    }

    protected virtual DuckDbExpression VisitOrder(OrderExpression node)
    {
        return new OrderExpression(node.ColumnReference, node.IsAscending);
    }

    protected virtual DuckDbExpression VisitWindow(WindowExpression node)
    {
        return new WindowExpression(node.Partitions.Select(Visit), node.Orderings.Select(Visit).Cast<OrderExpression>(), node.IsCumulative);
    }

    protected virtual DuckDbExpression VisitEmpty(EmptyExpression node)
    {
        return new EmptyExpression();
    }

    protected virtual DuckDbExpression VisitCase(CaseExpression node)
    {
        return new CaseExpression(node.Fallback, node.Blocks);
    }

    protected virtual DuckDbExpression VisitInterval(IntervalExpression node)
    {
        return new IntervalExpression(node.Interval);
    }

    protected virtual DuckDbExpression VisitCall(CallExpression node)
    {
        return new CallExpression(node.Function, node.Arguments.Select(Visit));
    }

    protected virtual DuckDbExpression VisitAs(AsExpression node)
    {
        DuckDbExpression operand = Visit(node.Expression);
        return new AsExpression(operand, node.Alias);
    }

    protected virtual DuckDbExpression VisitCoalesce(CoalesceExpression node)
    {
        DuckDbExpression left = Visit(node.Left);
        DuckDbExpression right = Visit(node.Right);
        return new CoalesceExpression(left, right);
    }

    protected virtual DuckDbExpression VisitLogical(LogicalExpression node)
    {
        DuckDbExpression left = Visit(node.Left);
        DuckDbExpression right = Visit(node.Right);
        return new LogicalExpression(left, right, node.Operator);
    }

    protected virtual DuckDbExpression VisitColumnReference(ColumnReferenceExpression node)
    {
        return node;
    }

    protected virtual DuckDbExpression VisitParameterReference(ParameterReferenceExpression node)
    {
        return node;
    }

    protected virtual DuckDbExpression VisitSelect(SelectExpression node)
    {
        DuckDbExpression? source = node.Source is not null ? Visit(node.Source) : null;
        LogicalExpression? predicate = node.Predicate is not null ? (LogicalExpression)Visit(node.Predicate) : null;
        DuckDbExpression[] projections = node.Projections.Select(Visit).ToArray();
        DuckDbExpression[] groupings = node.Groupings.Select(Visit).ToArray();
        AsExpression[] windows = node.Windows.Select(Visit).Cast<AsExpression>().ToArray();
        OrderExpression[] orderings = node.Orderings.Select(Visit).Cast<OrderExpression>().ToArray();
        return new SelectExpression(node.ElementType, node.Alias, source, predicate, projections, groupings, windows, node.Join, orderings, node.PartitionKeySelector);
    }

    protected virtual DuckDbExpression VisitLiteral(LiteralExpression node)
    {
        return node;
    }

    protected virtual DuckDbExpression VisitArithmetic(ArithmeticExpression node)
    {
        DuckDbExpression left = Visit(node.Left);
        DuckDbExpression right = Visit(node.Right);
        return new ArithmeticExpression(left, right, node.Operator);
    }
}