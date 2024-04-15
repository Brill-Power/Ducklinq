using System;

namespace BrillPower.Ducklinq.Expressions;

public class CoalesceExpression : DuckDbExpression, IBinaryExpression
{
    public CoalesceExpression(DuckDbExpression left, DuckDbExpression right)
    {
        Left = left;
        Right = right;
    }

    public DuckDbExpression Left { get; }
    public DuckDbExpression Right { get; }

    public override Type Type => Right.Type;

    public override string ToString() => $"COALESCE({Left}, {Right})";
}