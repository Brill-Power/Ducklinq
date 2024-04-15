namespace BrillPower.Ducklinq.Expressions;

public class OverExpression : DuckDbExpression
{
    public OverExpression(DuckDbExpression operand, string alias)
    {
        Operand = operand;
        Alias = alias;
    }

    public DuckDbExpression Operand { get; }
    public string Alias { get; }

    public override string ToString() => $"{Operand} OVER {Alias}";
}