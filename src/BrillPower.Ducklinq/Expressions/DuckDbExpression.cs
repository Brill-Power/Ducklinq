
using System.Linq.Expressions;

namespace BrillPower.Ducklinq.Expressions;

public abstract class DuckDbExpression : Expression
{
    public sealed override ExpressionType NodeType => ExpressionType.Extension;
}
