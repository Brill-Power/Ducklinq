using System;

namespace BrillPower.Ducklinq.Expressions;

public class ColumnReferenceExpression : DuckDbExpression, IEquatable<ColumnReferenceExpression>
{
    public ColumnReferenceExpression(string name, string? alias, Type type)
    {
        Name = name;
        Alias = alias;
        Type = type;
    }

    public string Name { get; }
    public string? Alias { get; }
    public override Type Type { get; }

    public bool Equals(ColumnReferenceExpression? other) => other is not null && Name == other.Name && Type == other.Type  && Alias == other.Alias;

    public override bool Equals(object? obj) => obj is ColumnReferenceExpression that && Equals(that);

    public override int GetHashCode() => Name.GetHashCode();

    public override string ToString() => Alias is not null ? $"{Alias}.{Name}" : Name;
}
