using System;

namespace BrillPower.Ducklinq.Expressions;

public class LogicalExpression : OperatorExpression<LogicalOperator>, IEquatable<LogicalExpression>
{
    public LogicalExpression(DuckDbExpression left, DuckDbExpression right, LogicalOperator @operator) : base(left, right, @operator)
    {
    }

    public override Type Type => typeof(bool);

    public bool Equals(LogicalExpression? other)
    {
        return other is not null && Operator == other.Operator && Left.Equals(other.Left) && Right.Equals(other.Right);
    }

    public override bool Equals(object? obj) => obj is LogicalExpression that && Equals(that);

    public override int GetHashCode() => HashCode.Combine(Operator.GetHashCode(), Left.GetHashCode(), Right.GetHashCode());

    public override string ToString()
    {
        return Operator switch
        {
            LogicalOperator.Equal => $"{Left}={Right}",
            LogicalOperator.NotEqual => $"{Left}<>{Right}",
            LogicalOperator.GreaterThan => $"{Left}>{Right}",
            LogicalOperator.GreaterThanOrEqual => $"{Left}>={Right}",
            LogicalOperator.LessThan => $"{Left}<{Right}",
            LogicalOperator.LessThanOrEqual => $"{Left}<={Right}",
            LogicalOperator.Like => $"{Left} LIKE {Right}",
            LogicalOperator.AndAlso => $"{Left} AND {Right}",
            LogicalOperator.And => $"({Left} AND {Right})",
            LogicalOperator.Or => $"({Left} OR {Right})",
            LogicalOperator.OrElse => $"{Left} OR {Right}",
            LogicalOperator.Contains => $"{Left} IN {Right}",
            LogicalOperator.Between => $"{Left} BETWEEN {Right}",
            _ => throw new NotSupportedException($"Operator {Operator} is not supported."),
        };
    }
}
