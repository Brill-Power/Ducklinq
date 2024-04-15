using System;

namespace BrillPower.Ducklinq.Expressions;

public class ParameterReferenceExpression : DuckDbExpression
{
    public ParameterReferenceExpression(string name, object? value, Type type)
    {
        Name = name;
        Value = value;
        Type = type;
    }

    public string Name { get; }

    public object? Value { get; }
    public override Type Type { get; }

    public override string ToString() => $"${Name}";
}
