using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using BrillPower.Ducklinq.Extensions;

namespace BrillPower.Ducklinq.Expressions.Fixup;

internal class PartitionedWhere : DuckDbExpressionVisitor
{
    protected override DuckDbExpression VisitSelect(SelectExpression node)
    {
        DuckDbExpression visited = base.VisitSelect(node);
        if (visited is SelectExpression visitedSelect && visitedSelect.PartitionKeySelector is not null && visitedSelect.Predicate is not null)
        {
            LogicalExpression predicate = visitedSelect.Predicate;

            // find column references in predicate
            ColumnReferencePredicateFinder predicateMemberFinder = new ColumnReferencePredicateFinder();
            predicateMemberFinder.Visit(predicate);

            DuckDbExpression translated = (DuckDbExpression)new QueryTranslator().Visit(visitedSelect.PartitionKeySelector);
            PredicateRewriter rewriter = new PredicateRewriter(predicateMemberFinder.PredicatesByColumnReference);
            rewriter.Visit(translated);

            predicate = rewriter.Predicates.Aggregate(predicate, (x, y) => new LogicalExpression(x, y, LogicalOperator.AndAlso));

            return new SelectExpression(visitedSelect.ElementType, visitedSelect.Alias, visitedSelect.Source, predicate, visitedSelect.Projections, visitedSelect.Groupings, visitedSelect.Windows, visitedSelect.Join, visitedSelect.Orderings);
        }
        return visited;
    }

    private class PredicateRewriter : DuckDbExpressionVisitor
    {
        private readonly IReadOnlyDictionary<ColumnReferenceExpression, List<LogicalExpression>> _predicatesByColumnReference;
        private readonly List<LogicalExpression> _predicates = new List<LogicalExpression>();
        private readonly Stack<List<LogicalExpression>> _latestPredicates = new Stack<List<LogicalExpression>>();

        public PredicateRewriter(IReadOnlyDictionary<ColumnReferenceExpression, List<LogicalExpression>> predicatesByColumnReference)
        {
            _predicatesByColumnReference = predicatesByColumnReference;
        }

        public IReadOnlyCollection<LogicalExpression> Predicates => _predicates;

        protected override DuckDbExpression VisitAs(AsExpression node)
        {
            DuckDbExpression visited = base.VisitAs(node);
            List<LogicalExpression> predicates = new List<LogicalExpression>();
            List<DateTime> predicateValues = new List<DateTime>();
            if (_latestPredicates.TryPop(out List<LogicalExpression>? expressions))
            {
                foreach (LogicalExpression logical in expressions)
                {
                    // replace column reference with comparand
                    ParameterFinder parameterFinder = new ParameterFinder();
                    parameterFinder.Visit(logical);
                    foreach (ParameterReferenceExpression parameterReference in parameterFinder.Parameters)
                    {
                        DuckDbExpression right = new ColumnReferenceSubstitutor(parameterReference).Visit(node.Expression);
                        string? alias = ((ColumnReferenceExpression)logical.Left).Alias;
                        predicates.Add(new LogicalExpression(new ColumnReferenceExpression(node.Alias.ToCamelCase(), alias, logical.Right.Type), right, logical.Operator));
                        predicateValues.Add((DateTime)parameterReference.Value!);
                    }
                }
            }
            if (predicates.Count == 1)
            {
                _predicates.Add(predicates[0]);
            }
            else if (predicates.Count == 2)
            {
                if(IsDateNode(node.Alias))
                {
                    DateTime from = predicateValues[0];
                    DateTime to = predicateValues[1];
                    string? alias = ((ColumnReferenceExpression)predicates[0].Left).Alias;
                    LogicalExpression partitionPredicate = GetPartitionPredicate(alias, from, to);
                    if (!_predicates.Contains(partitionPredicate))
                    {
                        _predicates.Add(partitionPredicate);
                    }
                }
                else
                {
                    _predicates.Add(new LogicalExpression(predicates[0], predicates[1], LogicalOperator.AndAlso));
                }
            }
            else if (predicates.Count > 0 && predicates.Count % 2 == 0)//in case of filtering 2 or more tables
            {
                for (int pairIndex = 0; pairIndex < predicates.Count / 2; pairIndex++)
                {
                    int firstIndex = 2 * pairIndex;
                    int secondIndex = firstIndex + 1;
                    if (IsDateNode(node.Alias))
                    {
                        DateTime from = predicateValues[firstIndex];
                        DateTime to = predicateValues[secondIndex];
                        string? alias = ((ColumnReferenceExpression)predicates[firstIndex].Left).Alias;
                        LogicalExpression partitionPredicate = GetPartitionPredicate(alias, from, to);
                        if (!_predicates.Contains(partitionPredicate))
                        {
                            _predicates.Add(partitionPredicate);
                        }
                    }
                    else
                    {
                        _predicates.Add(new LogicalExpression(predicates[firstIndex], predicates[secondIndex], LogicalOperator.AndAlso));
                    }
                }
            }
            else
            {
                throw new NotSupportedException($"Don't yet know how we'd end up here.");
            }
            return node;
        }

        private bool IsDateNode(string name)
        {
            return name.Equals(nameof(DateTime.Year)) ||
                   name.Equals(nameof(DateTime.Month)) ||
                   name.Equals(nameof(DateTime.Day));
        }

        private ColumnReferenceExpression YearColumn(string? alias) => new(nameof(DateTime.Year).ToCamelCase(), alias, typeof(int));
        private ColumnReferenceExpression MonthColumn(string? alias) => new (nameof(DateTime.Month).ToCamelCase(), alias, typeof(int));
        private ColumnReferenceExpression DayColumn(string? alias) => new (nameof(DateTime.Day).ToCamelCase(), alias, typeof(int));

        private LogicalExpression GetPartitionPredicate(string? alias, DateTime from, DateTime to)
        {
            LiteralExpression fromDay = new(from.Day, typeof(int));
            LiteralExpression fromMonth = new(from.Month, typeof(int));
            LiteralExpression fromYear = new(from.Year, typeof(int));
            LiteralExpression toDay = new(to.Day, typeof(int));
            LiteralExpression toYear = new(to.Year, typeof(int));

            LogicalExpression yearEqualFrom = new(YearColumn(alias), fromYear, LogicalOperator.Equal);

            if (from.Year == to.Year)
            {
                if (from.Month == to.Month)
                {
                    LogicalExpression monthEqualFrom = new(MonthColumn(alias), fromMonth, LogicalOperator.Equal);
                    if (from.Day == to.Day)
                    {
                        LogicalExpression dayEqual = new(DayColumn(alias), fromDay, LogicalOperator.Equal);
                        return yearEqualFrom.AndOthers(monthEqualFrom, dayEqual);
                    }

                    LogicalExpression dayBetween = new(DayColumn(alias), new LogicalExpression(fromDay, toDay, LogicalOperator.AndAlso), LogicalOperator.Between);
                    return yearEqualFrom.AndOthers(monthEqualFrom, dayBetween);
                }

                LogicalExpression fromOrMiddleOrTo = GetMonthInTheSameYear(alias, from, to);
                return new LogicalExpression(yearEqualFrom, fromOrMiddleOrTo, LogicalOperator.And);
            }

            LogicalExpression monthAndDayFrom = GetMonthAndDayFrom(alias, from);
            LogicalExpression? middleMonthsFrom = GetMonthsAfter(alias, from);
            if (middleMonthsFrom is not null)
            {
                monthAndDayFrom = new LogicalExpression(monthAndDayFrom, middleMonthsFrom, LogicalOperator.Or);
            }

            LogicalExpression fromExpression =
                new LogicalExpression(yearEqualFrom, monthAndDayFrom, LogicalOperator.And);


            LogicalExpression? middleYears = GetMiddleYears(alias, from, to);
            if (middleYears is not null)
            {
                fromExpression = fromExpression.OrOthers(middleYears);
            }

            LogicalExpression yearEqualTo = new(YearColumn(alias), toYear, LogicalOperator.Equal);
            LogicalExpression monthAndDayTo = GetMonthAndDayTo(alias, to);
            LogicalExpression? middleMonthsTo = GetMonthsBefore(alias, to);
            if (middleMonthsTo is not null)
            {
                monthAndDayTo = new LogicalExpression(middleMonthsTo, monthAndDayTo, LogicalOperator.Or);
            }

            LogicalExpression toExpression = new LogicalExpression(yearEqualTo, monthAndDayTo, LogicalOperator.And);
            return fromExpression.OrOthers(toExpression);
        }

        private LogicalExpression GetMonthAndDayFrom(string? alias, DateTime from)
        {
            LiteralExpression fromDay = new(from.Day, typeof(int));
            LiteralExpression fromMonth = new(from.Month, typeof(int));
            LogicalExpression monthEqualFrom = new (MonthColumn(alias), fromMonth, LogicalOperator.Equal);
            LogicalExpression dayGraterThanFrom = new (DayColumn(alias), fromDay, DateTime.DaysInMonth(from.Year, from.Month) == from.Day?  LogicalOperator.Equal : LogicalOperator.GreaterThanOrEqual);
            return new (monthEqualFrom, dayGraterThanFrom, LogicalOperator.And);
        }

        private LogicalExpression GetMonthAndDayTo(string? alias, DateTime to)
        {
            LiteralExpression toMonth = new(to.Month, typeof(int));
            LiteralExpression toDay = new(to.Day, typeof(int));
            LogicalExpression monthEqualTo = new (MonthColumn(alias), toMonth, LogicalOperator.Equal);
            LogicalExpression dayLessThanTo = new (DayColumn(alias), toDay, to.Day == 1 ? LogicalOperator.Equal : LogicalOperator.LessThanOrEqual);
            return new (monthEqualTo, dayLessThanTo, LogicalOperator.And);
        }

        private LogicalExpression? GetMiddleMonths(string? alias, DateTime from, DateTime to)
        {
            LogicalExpression? middleMonths;
            int monthDiff = to.Month - from.Month;
            if (monthDiff <= 1)
            {
                return null;
            }
            LiteralExpression secondMonth = new(from.Month + 1, typeof(int));

            if (monthDiff == 2)
            {
                LogicalExpression secondMonthEqual = new (MonthColumn(alias), secondMonth, LogicalOperator.Equal);
                middleMonths = secondMonthEqual;
            }
            else
            {
                LiteralExpression penultimateMonth = new( to.Month - 1, typeof(int));
                LogicalExpression monthBetween = new (MonthColumn(alias), new LogicalExpression(secondMonth, penultimateMonth, LogicalOperator.AndAlso), LogicalOperator.Between);
                middleMonths = monthBetween;
            }
            return middleMonths;
        }

        private LogicalExpression? GetMonthsBefore(string? alias, DateTime date)
        {
            LiteralExpression firstMonth = new(1, typeof(int));

            if (date.Month == 1)
            {
                return null;
            }
            if (date.Month == 2)
            {
                return new(MonthColumn(alias), firstMonth, LogicalOperator.Equal);
            }

            LiteralExpression penultimateMonth = new(date.Month - 1, typeof(int));
            return new (MonthColumn(alias), new LogicalExpression(firstMonth, penultimateMonth, LogicalOperator.AndAlso), LogicalOperator.Between);
        }

        private LogicalExpression? GetMonthsAfter(string? alias, DateTime date)
        {
            LiteralExpression lastMonth = new(12, typeof(int));

            if (date.Month == 12)
            {
                return null;
            }
            if (date.Month == 11)
            {
                return new(MonthColumn(alias), lastMonth, LogicalOperator.Equal);
            }

            LiteralExpression nextMonth = new(date.Month + 1, typeof(int));
            return new (MonthColumn(alias), new LogicalExpression(nextMonth, lastMonth, LogicalOperator.AndAlso), LogicalOperator.Between);
        }

        private LogicalExpression GetMonthInTheSameYear(string? alias, DateTime from, DateTime to)
        {
            LogicalExpression monthAndDayFrom = GetMonthAndDayFrom(alias, from);
            DuckDbExpression monthAndDayTo = GetMonthAndDayTo(alias,to);

            LogicalExpression? middleMonths = GetMiddleMonths(alias,from, to);
            LogicalExpression monthAndDayFromOrMiddle = monthAndDayFrom;
            if(middleMonths is not null)
            {
                monthAndDayFromOrMiddle = new (monthAndDayFrom, middleMonths, LogicalOperator.OrElse);
            }

            LogicalExpression fromOrMiddleOrTo = new (monthAndDayFromOrMiddle, monthAndDayTo, LogicalOperator.Or);
            return fromOrMiddleOrTo;
        }

        private LogicalExpression? GetMiddleYears(string? alias, DateTime from, DateTime to)
        {
            LogicalExpression? middleYears;
            int yearDiff = to.Year - from.Year;
            if (yearDiff <= 1)
            {
                return null;
            }
            LiteralExpression secondYear= new(from.Year + 1, typeof(int));

            if (yearDiff == 2)
            {
                LogicalExpression secondMonthEqual = new (YearColumn(alias), secondYear, LogicalOperator.Equal);
                middleYears = secondMonthEqual;
            }
            else
            {
                LiteralExpression penultimateYear = new(to.Year - 1, typeof(int));
                LogicalExpression yearBetween = new (YearColumn(alias), new LogicalExpression(secondYear, penultimateYear, LogicalOperator.AndAlso), LogicalOperator.Between);
                middleYears = yearBetween;
            }
            return middleYears;
        }

        protected override DuckDbExpression VisitColumnReference(ColumnReferenceExpression node)
        {
            if (_predicatesByColumnReference.TryGetValue(node, out List<LogicalExpression>? expressions))
            {
                _latestPredicates.Push(expressions);
            }
            return base.VisitColumnReference(node);
        }
    }

    private class ParameterFinder : DuckDbExpressionVisitor
    {
        private readonly List<ParameterReferenceExpression> _parameters = new List<ParameterReferenceExpression>();

        public IReadOnlyList<ParameterReferenceExpression> Parameters => _parameters;

        protected override DuckDbExpression VisitParameterReference(ParameterReferenceExpression node)
        {
            _parameters.Add(node);
            return base.VisitParameterReference(node);
        }
    }

    private class ColumnReferenceSubstitutor : DuckDbExpressionVisitor
    {
        private readonly DuckDbExpression _replacement;

        public ColumnReferenceSubstitutor(DuckDbExpression replacement)
        {
            _replacement = replacement;
        }

        protected override DuckDbExpression VisitColumnReference(ColumnReferenceExpression node)
        {
            return _replacement;
        }
    }

    private class ColumnReferencePredicateFinder : DuckDbExpressionVisitor
    {
        private ConcurrentDictionary<ColumnReferenceExpression, List<LogicalExpression>> _predicatesByColumnReference = new ConcurrentDictionary<ColumnReferenceExpression, List<LogicalExpression>>(new ColumnReferenceComparer());

        public IReadOnlyDictionary<ColumnReferenceExpression, List<LogicalExpression>> PredicatesByColumnReference => _predicatesByColumnReference;

        protected override DuckDbExpression VisitLogical(LogicalExpression node)
        {
            DuckDbExpression left = Visit(node.Left);
            DuckDbExpression right = Visit(node.Right);
            if (left is ColumnReferenceExpression leftColumn)
            {
                _predicatesByColumnReference.GetOrAdd(leftColumn, _ => new List<LogicalExpression>()).Add(node);
            }
            else if (right is ColumnReferenceExpression rightColumn)
            {
                _predicatesByColumnReference.GetOrAdd(rightColumn, _ => new List<LogicalExpression>()).Add(node);
            }
            return node;
        }
    }

    internal class ColumnReferenceComparer : IEqualityComparer<ColumnReferenceExpression>
    {
        public bool Equals(ColumnReferenceExpression? x, ColumnReferenceExpression? y)
        {
            return x is not null && y is not null && x.Name.Equals(y.Name);
        }

        public int GetHashCode([DisallowNull] ColumnReferenceExpression obj)
        {
            return obj.Name.GetHashCode();
        }
    }
}
