using System;

namespace BrillPower.Ducklinq.Expressions;

public class UnionExpression : DuckDbExpression, IBinaryExpression
{
    public UnionExpression(SelectExpression left, SelectExpression right, bool all)
    {
        Left = left;
        Right = right;
        All = all;
    }

    public SelectExpression Left { get; }
    public SelectExpression Right { get; }
    public bool All { get; }

    public override Type Type => Left.Type;

    DuckDbExpression IBinaryExpression.Left => Left;

    DuckDbExpression IBinaryExpression.Right => Right;

    public override string ToString() => $"({Left}) UNION {(All ? "ALL " : String.Empty)}({Right})";
}