using System;

namespace BrillPower.Ducklinq.Expressions;

public class EmptyExpression : DuckDbExpression
{
    public override string ToString() => String.Empty;
}
