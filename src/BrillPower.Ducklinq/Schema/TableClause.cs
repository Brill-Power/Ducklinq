using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using BrillPower.Ducklinq.Extensions;

namespace BrillPower.Ducklinq.Schema;

public class Table
{
    public static string GetTableName(Type elementType)
    {
        if (elementType.GetCustomAttribute<TableAttribute>() is TableAttribute ta)
        {
            return ta.Name;
        }
        else
        {
            return elementType.Name.Pluralise().ToCamelCase();
        }
    }
}
