using System;

namespace BrillPower.Ducklinq.Expressions;

public class LiteralExpression : DuckDbExpression, IEquatable<LiteralExpression>
{
    internal LiteralExpression(object? value, Type type)
    {
        Value = value;
        Type = type;
    }

    public object? Value { get; }

    public override Type Type { get; }

    public bool Equals(LiteralExpression? other)
    {
        return other is not null && Type == other.Type && Object.Equals(Value, other.Value);
    }

    public override bool Equals(object? obj) => obj is LiteralExpression that && Equals(that);

    public override int GetHashCode() => Value is not null ? Value.GetHashCode() : 0;

    public override string ToString()
    {
        return Value is null ? "NULL" : $"{Value}";
    }

    public LiteralExpression Update(object? value)
    {
        return new LiteralExpression(value, value!.GetType());
    }
}
