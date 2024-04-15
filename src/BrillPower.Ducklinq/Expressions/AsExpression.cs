using System;

namespace BrillPower.Ducklinq.Expressions;

public class AsExpression : DuckDbExpression
{
    public AsExpression(DuckDbExpression expression, string alias)
    {
        Expression = expression;
        Alias = alias;
    }

    public DuckDbExpression Expression { get; }
    public string Alias { get; }

    public override Type Type => Expression.Type;

    public override string ToString() => $"{Expression} AS {Alias}";
}