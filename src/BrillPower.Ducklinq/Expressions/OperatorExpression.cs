using System;

namespace BrillPower.Ducklinq.Expressions;

public abstract class OperatorExpression<T> : DuckDbExpression, IBinaryExpression
    where T : struct, Enum
{
    protected OperatorExpression(DuckDbExpression left, DuckDbExpression right, T @operator)
    {
        Left = left;
        Right = right;
        Operator = @operator;
    }

    public DuckDbExpression Left { get; }
    public DuckDbExpression Right { get; }

    public T Operator { get; }
}
