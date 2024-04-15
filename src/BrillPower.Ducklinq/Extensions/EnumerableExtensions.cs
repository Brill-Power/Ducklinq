using System;
using System.Collections.Generic;

namespace BrillPower.Ducklinq.Extensions;

public static class EnumerableExtensions
{
    public static IEnumerable<T> Cumulative<T>(this IEnumerable<T> self)
    {
        throw new InvalidOperationException();
    }
}