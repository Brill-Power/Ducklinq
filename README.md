# Ducklinq

LINQ-to-DuckDb. A LINQ wrapper around [DuckDB.NET](https://github.com/Giorgi/DuckDB.NET) using Dapper for materialisation.

## Getting Started

The unit tests should give an idea of what is possible, but in brief:

```csharp
using BrillPower.Ducklinq;

DuckDbContext dbContext = new DuckDbContext("DataSource=file.db");
IQueryable<Integer> query = dbContext.Get<Integer>();
List<Integer> integers = query.Where(i => i.Foo == 3).ToList();

public class Integer
{
    public int Foo { get; set; }
    public int Bar { get; set; }
}
```

## Documentation

...is a little sparse at the moment.

## Support

If you find a bug, please file an issue or submit a PR.
