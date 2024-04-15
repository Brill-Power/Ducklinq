namespace BrillPower.Ducklinq.Expressions;

public class OrderExpression : DuckDbExpression
{
    public OrderExpression(ColumnReferenceExpression columnReference, bool ascending)
    {
        ColumnReference = columnReference;
        IsAscending = ascending;
    }

    public ColumnReferenceExpression ColumnReference { get; }
    public bool IsAscending { get; }

    public override string ToString() => $"{ColumnReference} {(IsAscending ? "ASC" : "DESC")}";
}