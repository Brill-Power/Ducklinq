using System;
using System.Linq;
using Xunit;
using BrillPower.Ducklinq.Extensions;
using System.IO;
using DuckDB.NET.Data;
using System.Collections.Generic;
using BrillPower.Ducklinq.Expressions;

namespace BrillPower.Ducklinq.Test;

public class DuckDbQueryableTest : IDisposable
{
    public DuckDbQueryableTest()
    {
        if (File.Exists("batteryData.parquet"))
        {
            File.Delete("batteryData.parquet");
        }

        using (DuckDBConnection connection = new DuckDBConnection("DataSource=:memory:"))
        {
            connection.Open();

            using (DuckDBCommand command = connection.CreateCommand())
            {
                command.CommandText = "CREATE TABLE batteryData (batteryId INT NOT NULL, " +
                    "timestamp TIMESTAMP NOT NULL, " +
                    "voltage DOUBLE NOT NULL, " +
                    "current DOUBLE NOT NULL, " +
                    "state INT NULL, " +
                    "error INT NULL, " +
                    "maxChargeCurrent DOUBLE NULL, " +
                    "maxDischargeCurrent DOUBLE NULL " +
                    ")";
                command.ExecuteNonQuery();
            }

            using (DuckDBCommand command = connection.CreateCommand())
            {
                command.CommandText = "CREATE TABLE moduleData (moduleId INT NOT NULL, " +
                    "timestamp TIMESTAMP NOT NULL, " +
                    "voltage DOUBLE NOT NULL, " +
                    "current DOUBLE NOT NULL, " +
                    "state INT NULL, " +
                    "error INT NULL " +
                    ")";
                command.ExecuteNonQuery();
            }

            DateTime timestamp = DateTime.UtcNow;
            using (DuckDBAppender appender = connection.CreateAppender("batteryData"))
            {
                IDuckDBAppenderRow row = appender.CreateRow();
                row.AppendValue(1)
                    .AppendValue(timestamp)
                    .AppendValue(48.1)
                    .AppendValue(31.5)
                    .AppendNullValue()
                    .AppendNullValue()
                    .AppendValue(62.5)
                    .AppendValue(62.5)
                    .EndRow();
            }

            using (DuckDBAppender appender = connection.CreateAppender("moduleData"))
            {
                IDuckDBAppenderRow row = appender.CreateRow();
                row.AppendValue(10)
                    .AppendValue(timestamp)
                    .AppendValue(3.25)
                    .AppendValue(10.1)
                    .AppendNullValue()
                    .AppendNullValue()
                    .EndRow();
            }

            using (DuckDBCommand command = connection.CreateCommand())
            {
                command.CommandText = "COPY batteryData TO 'batteryData.parquet' (FORMAT PARQUET)";
                command.ExecuteNonQuery();
            }

            using (DuckDBCommand command = connection.CreateCommand())
            {
                command.CommandText = "COPY moduleData TO 'moduleData.parquet' (FORMAT PARQUET)";
                command.ExecuteNonQuery();
            }

            Assert.True(File.Exists("batteryData.parquet"));
            Assert.True(File.Exists("moduleData.parquet"));
        }
    }

    public void Dispose()
    {
        File.Delete("batteryData.parquet");
        File.Delete("moduleData.parquet");
    }

    [Fact]
    public void TestWhere()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> query = context.Get<BatteryDatum>().AsParquet();
        query = query.Where(bd => bd.Voltage > 12);
        List<BatteryDatum> batteryData = query.ToList();

        Assert.Equal("SELECT * FROM 'batteryData.parquet' AS t0 WHERE t0.Voltage>$p0", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(batteryData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
                Assert.Equal(62.5, x.MaxChargeCurrent);
                Assert.Null(x.State);
            });
    }

    [Fact]
    public void TestWhereContains()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> query = context.Get<BatteryDatum>().AsParquet();
        int[] ids = [1, 2];
        query = query.Where(bd => ids.Contains(bd.BatteryID));
        List<BatteryDatum> batteryData = query.ToList();

        Assert.Equal("SELECT * FROM 'batteryData.parquet' AS t0 WHERE t0.BatteryID IN $p0", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(batteryData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
                Assert.Equal(62.5, x.MaxChargeCurrent);
                Assert.Null(x.State);
            });
    }

    [Fact]
    public void TestWhereBetween()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> query = context.Get<BatteryDatum>().AsParquet();
        DateTime start = DateTime.UtcNow.AddDays(-1);
        DateTime end = DateTime.UtcNow.AddDays(1);
        query = query.Where(bd => bd.Timestamp >= start && bd.Timestamp <= end);
        List<BatteryDatum> batteryData = query.ToList();

        Assert.Equal("SELECT * FROM 'batteryData.parquet' AS t0 WHERE t0.Timestamp BETWEEN $p0 AND $p1", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(batteryData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
                Assert.Equal(62.5, x.MaxChargeCurrent);
                Assert.Null(x.State);
            });
    }

    [Fact]
    public void TestWherePartitioned()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> query = context.Get<BatteryDatum>().AsPartitioned(bd => new
        {
            bd.Timestamp.Year,
            bd.Timestamp.Month,
            bd.Timestamp.Day,
        });
        DateTime date = DateTime.UtcNow.AddHours(-1);
        query = query.Where(bd => bd.Timestamp >= date);

        Assert.Equal("SELECT * FROM read_parquet('batteryData/*/*/*/*.parquet', hive_partitioning=1) AS t0 " +
                     "WHERE t0.Timestamp>=$p0 AND t0.year>=date_part('year', $p0) AND t0.month>=date_part('month', $p0) AND t0.day>=date_part('day', $p0)",
            new QueryTranslator().Translate(query.Expression));
    }

    [Fact]
    public void TestWhereProjection()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        var query = context.Get<BatteryDatum>().Select(b => new
        {
            b.BatteryID,
            b.Timestamp,
            Power = b.Voltage * b.Current,
            b.Current,
            b.Error,
            b.State
        })
        .Where(a => a.Power > 100)
        .Select(a => new
        {
            a.BatteryID,
            a.Timestamp
        });
        string actualSql = new QueryTranslator().Translate(query.Expression);
        Assert.Equal("SELECT t1.BatteryID, t1.Timestamp FROM " +
                     "(SELECT t0.BatteryID, t0.Timestamp, t0.Voltage*t0.Current AS Power, t0.Current, t0.Error, t0.State " +
                     "FROM batteryData AS t0) AS t1 " +
                     "WHERE t1.Power>$p0", actualSql);
    }

    [Fact]
    public void TestWhereGroupByTimeBucketAndBatteryId()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>();
        DateTime start = new DateTime(2023, 12, 31);
        DateTime end = new DateTime(2024, 1, 1);
        var query = data.Where(bd => bd.Timestamp >= start && bd.Timestamp <= end)
            .GroupBy(bd => new { Timestamp = bd.Timestamp.TimeBucket(TimeSpan.FromMinutes(10)), bd.BatteryID })
            .Select(g => new
            {
                g.Key.BatteryID,
                g.Key.Timestamp,
                Voltage = g.Average(bd => bd.Voltage),
                Current = g.Average(bd => bd.Current),
                State = g.Max(bd => bd.State),
                Error = g.Max(bd => bd.Error),
            });

        string actualSql = new QueryTranslator().Translate(query.Expression);
        Assert.Equal("SELECT t0.BatteryID, time_bucket('00:10:00'::INTERVAL, t0.Timestamp) AS Timestamp, AVG(t0.Voltage) AS Voltage, AVG(t0.Current) AS Current, MAX(t0.State) AS State, MAX(t0.Error) AS Error " +
                     "FROM batteryData AS t0 WHERE t0.Timestamp BETWEEN $p0 AND $p1 " +
                     "GROUP BY time_bucket('00:10:00'::INTERVAL, t0.Timestamp), t0.BatteryID", actualSql);
    }

    [Fact]
    public void TestWherePartitionedGroupByTimeBucketAndBatteryId()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsPartitioned(bd => new
        {
            bd.Timestamp.Year,
            bd.Timestamp.Month,
            bd.Timestamp.Day,
        });
        DateTime start = new DateTime(2023, 12, 31);
        DateTime end = new DateTime(2024, 1, 1);
        var query = data.Where(bd => bd.Timestamp >= start && bd.Timestamp <= end)
            .GroupBy(bd => new { Timestamp = bd.Timestamp.TimeBucket(TimeSpan.FromMinutes(10)), bd.BatteryID })
            .Select(g => new
            {
                g.Key.BatteryID,
                g.Key.Timestamp,
                Voltage = g.Average(bd => bd.Voltage),
                Current = g.Average(bd => bd.Current),
                State = g.Max(bd => bd.State),
                Error = g.Max(bd => bd.Error),
            });

        string actualSql = new QueryTranslator().Translate(query.Expression);
        Assert.Equal("SELECT t0.BatteryID, time_bucket('00:10:00'::INTERVAL, t0.Timestamp) AS Timestamp, AVG(t0.Voltage) AS Voltage, AVG(t0.Current) AS Current, MAX(t0.State) AS State, MAX(t0.Error) AS Error " +
                     "FROM read_parquet('batteryData/*/*/*/*.parquet', hive_partitioning=1) AS t0 WHERE t0.Timestamp BETWEEN $p0 AND $p1 AND ((t0.year=2023 AND (t0.month=12 AND t0.day=31)) OR (t0.year=2024 AND (t0.month=1 AND t0.day=1))) " +
                     "GROUP BY time_bucket('00:10:00'::INTERVAL, t0.Timestamp), t0.BatteryID", actualSql);
    }

    [Fact]
    public void TestWhereBetweenPartitioned()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> query = context.Get<BatteryDatum>().AsPartitioned(bd => new
        {
            bd.Timestamp.Year,
            bd.Timestamp.Month,
            bd.Timestamp.Day,
        });
        DateTime start = new DateTime(2024, 2, 19);
        DateTime end = new DateTime(2024, 2, 21);
        query = query.Where(bd => bd.Timestamp >= start && bd.Timestamp <= end);

        Assert.Equal("SELECT * " +
                     "FROM read_parquet('batteryData/*/*/*/*.parquet', hive_partitioning=1) AS t0 " +
                     "WHERE t0.Timestamp BETWEEN $p0 AND $p1 AND t0.year=2024 AND t0.month=2 AND t0.day BETWEEN 19 AND 21",
            new QueryTranslator().Translate(query.Expression));
    }

    [Theory]
    [ClassData(typeof(PartitionedEdgeCasesTestData))]
    public void TestWhereBetweenPartitionedEdgeCase(DateTime start, DateTime end, string expectedSql)
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> query = context.Get<BatteryDatum>().AsPartitioned(bd => new
        {
            bd.Timestamp.Year,
            bd.Timestamp.Month,
            bd.Timestamp.Day,
        });
        query = query.Where(bd => bd.Timestamp >= start && bd.Timestamp <= end);

        Assert.Equal(expectedSql, new QueryTranslator().Translate(query.Expression));
    }

    [Fact]
    public void TestSelect()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        var query = context.Get<BatteryDatum>().AsParquet().Select(bd => new
        {
            bd.BatteryID,
            bd.Timestamp,
            bd.Voltage
        });
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID, t0.Timestamp, t0.Voltage FROM 'batteryData.parquet' AS t0", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
            });
    }

    [Fact]
    public void TestSelectCoalesce()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        var query = context.Get<BatteryDatum>().AsParquet().Select(bd => new
        {
            bd.BatteryID,
            bd.Timestamp,
            State = bd.State ?? 1,
        });
        var stateData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID, t0.Timestamp, COALESCE(t0.State, $p0) AS State FROM 'batteryData.parquet' AS t0", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(stateData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(1, x.State);
            });
    }

    [Fact]
    public void TestSelectSelect()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        var query = context.Get<BatteryDatum>().Select(b => new
        {
            b.BatteryID,
            b.Timestamp,
            b.Voltage,
            b.Current,
            b.Error,
            b.State
        }).Select(a => new
        {
            a.BatteryID,
            a.Timestamp
        });
        string actualSql = new QueryTranslator().Translate(query.Expression);
        Assert.Equal("SELECT t1.BatteryID, t1.Timestamp FROM " +
                     "(SELECT t0.BatteryID, t0.Timestamp, t0.Voltage, t0.Current, t0.Error, t0.State FROM batteryData AS t0) AS t1", actualSql);
    }

    [Fact]
    public void TestGroupBySelectSelect()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        var query = context.Get<BatteryDatum>()
            .GroupBy(bd => new { Timestamp = bd.Timestamp.TimeBucket(TimeSpan.FromMinutes(10)), bd.BatteryID })
            .Select(g => new
            {
                g.Key.BatteryID,
                g.Key.Timestamp,
                Voltage = g.Average(bd => bd.Voltage),
                Current = g.Average(bd => bd.Current),
                State = g.Max(bd => bd.State),
                Error = g.Max(bd => bd.Error),
            })
            .Select(b => new
            {
                b.BatteryID,
                b.Timestamp,
                b.Voltage,
                b.Current,
            });

        string actualSql = new QueryTranslator().Translate(query.Expression);
        Assert.Equal("SELECT t2.BatteryID, t2.Timestamp, t2.Voltage, t2.Current FROM " +
            "(" +
            "SELECT t0.BatteryID, time_bucket('00:10:00'::INTERVAL, t0.Timestamp) AS Timestamp, AVG(t0.Voltage) AS Voltage, AVG(t0.Current) AS Current, MAX(t0.State) AS State, MAX(t0.Error) AS Error " +
            "FROM batteryData AS t0 " +
            "GROUP BY time_bucket('00:10:00'::INTERVAL, t0.Timestamp), t0.BatteryID" +
            ") AS t2", actualSql);
    }

    [Fact]
    public void TestUnion()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.Where(bd => bd.BatteryID == 2).Select(bd => new
        {
            bd.BatteryID,
            bd.Timestamp,
            bd.Voltage
        }).Union(data.Where(bd => bd.BatteryID == 1).Select(bd => new
        {
            bd.BatteryID,
            bd.Timestamp,
            bd.Voltage,
        }));
        var voltageData = query.ToList();

        Assert.Equal("(SELECT t0.BatteryID, t0.Timestamp, t0.Voltage FROM 'batteryData.parquet' AS t0 WHERE t0.BatteryID=$p0) UNION (SELECT t2.BatteryID, t2.Timestamp, t2.Voltage FROM 'batteryData.parquet' AS t2 WHERE t2.BatteryID=$p1)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
            });
    }

    [Fact]
    public void TestConcatUnionAll()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.Where(bd => bd.BatteryID == 1).Select(bd => new
        {
            bd.BatteryID,
            bd.Timestamp,
            bd.Voltage
        }).Concat(data.Where(bd => bd.BatteryID == 1).Select(bd => new
        {
            bd.BatteryID,
            bd.Timestamp,
            bd.Voltage,
        }));
        var voltageData = query.ToList();

        Assert.Equal("(SELECT t0.BatteryID, t0.Timestamp, t0.Voltage FROM 'batteryData.parquet' AS t0 WHERE t0.BatteryID=$p0) " +
                     "UNION ALL " +
                     "(SELECT t2.BatteryID, t2.Timestamp, t2.Voltage FROM 'batteryData.parquet' AS t2 WHERE t2.BatteryID=$p1)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
            },
            x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
            });
    }

    [Fact]
    public void TestGroupByThenMax()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.GroupBy(bd => bd.BatteryID).Select(g => new
        {
            g.Key,
            MaxVoltage = g.Max(bd => bd.Voltage)
        });
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID AS Key, MAX(t0.Voltage) AS MaxVoltage " +
                     "FROM 'batteryData.parquet' AS t0 GROUP BY t0.BatteryID", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.Key);
                Assert.Equal(48.1, x.MaxVoltage);
            });
    }

    [Fact]
    public void TestGroupByThenConditional()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.GroupBy(bd => bd.BatteryID).Select(g => new
        {
            g.Key,
            AverageVoltage = g.Average(bd => bd.Voltage) == 0 ? 1 : g.Average(bd => bd.Voltage)
        });
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID AS Key, CASE WHEN AVG(t0.Voltage)=$p0 THEN $p1 ELSE AVG(t0.Voltage) END AS AverageVoltage " +
                     "FROM 'batteryData.parquet' AS t0 GROUP BY t0.BatteryID", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.Key);
                Assert.Equal(48.1, x.AverageVoltage);
            });
    }

    [Fact]
    public void TestGroupByTimeBucket()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.GroupBy(bd => bd.Timestamp.TimeBucket(TimeSpan.FromMinutes(5))).Select(g => new
        {
            g.Key,
            MaxVoltage = g.Max(bd => bd.Voltage)
        });
        var voltageData = query.ToList();

        Assert.Equal("SELECT time_bucket('00:05:00'::INTERVAL, t0.Timestamp) AS Key, MAX(t0.Voltage) AS MaxVoltage " +
                     "FROM 'batteryData.parquet' AS t0 GROUP BY time_bucket('00:05:00'::INTERVAL, t0.Timestamp)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                //Assert.Equal(1, x.Key);
                Assert.Equal(48.1, x.MaxVoltage);
            });
    }

    [Fact]
    public void TestGroupBySelectMany()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.GroupBy(bd => bd.BatteryID).SelectMany(g => g.Select(bd => new
        {
            g.Key,
            VoltageRatio = bd.Voltage / g.Sum(bc => bc.Voltage)
        }));
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID AS Key, t0.Voltage/SUM(t0.Voltage) OVER w0 AS VoltageRatio " +
                     "FROM 'batteryData.parquet' AS t0 WINDOW w0 AS (PARTITION BY t0.BatteryID)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.Key);
                Assert.Equal(1.0, x.VoltageRatio);
            });
    }

    [Fact]
    public void TestOrderByGroupBySelectMany()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.OrderBy(bd => bd.Timestamp).GroupBy(bd => bd.BatteryID).SelectMany(g => g.Select(bd => new
        {
            g.Key,
            VoltageRatio = bd.Voltage / g.Sum(bc => bc.Voltage)
        }));
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID AS Key, t0.Voltage/SUM(t0.Voltage) OVER w0 AS VoltageRatio " +
                     "FROM 'batteryData.parquet' AS t0 WINDOW w0 AS (PARTITION BY t0.BatteryID ORDER BY t0.Timestamp ASC)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.Key);
                Assert.Equal(1.0, x.VoltageRatio);
            });
    }

    [Fact]
    public void TestOrderByGroupByCumulativeSelectManySum()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.OrderBy(bd => bd.Timestamp).GroupBy(bd => bd.BatteryID).Cumulative().SelectMany(g => g.Select(bd => new
        {
            g.Key,
            EnergyThroughput = g.Sum(bd => bd.Voltage * bd.Current),
        }));
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID AS Key, SUM(t0.Voltage*t0.Current) OVER w0 AS EnergyThroughput " +
                     "FROM 'batteryData.parquet' AS t0 WINDOW w0 AS (PARTITION BY t0.BatteryID ORDER BY t0.Timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.Key);
                Assert.Equal(1515.15, x.EnergyThroughput);
            });
    }

    [Fact]
    public void TestGroupBySelectManyOrderByCumulativeSum()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.GroupBy(bd => bd.BatteryID).SelectMany(g => g.Select(bd => new
        {
            g.Key,
            EnergyThroughput = g.OrderBy(bd => bd.Timestamp).Cumulative().Sum(bd => bd.Voltage * bd.Current),
        }));
        var voltageData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID AS Key, SUM(t0.Voltage*t0.Current) OVER w1 AS EnergyThroughput " +
                     "FROM 'batteryData.parquet' AS t0 WINDOW w1 AS (PARTITION BY t0.BatteryID ORDER BY t0.Timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w0 AS (PARTITION BY t0.BatteryID)", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.Key);
                Assert.Equal(1515.15, x.EnergyThroughput);
            });
    }

    private class TestGroupByWithMultipleKeysOutput
    {
        public int BatteryID { get; set; }
        public double Voltage { get; set; }
        public double EnergyThroughput { get; set; }
    }

    [Fact]
    public void TestGroupByWithMultipleKeys()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        var query = data.GroupBy(bd => new { bd.BatteryID, bd.Voltage }).Select(g => new TestGroupByWithMultipleKeysOutput()
        {
            BatteryID = g.Key.BatteryID,
            Voltage = g.Key.Voltage,
            EnergyThroughput = g.Sum(bd => bd.Voltage * bd.Current)
        });

        Assert.Equal("SELECT t0.BatteryID, t0.Voltage, SUM(t0.Voltage*t0.Current) AS EnergyThroughput " +
                     "FROM 'batteryData.parquet' AS t0 GROUP BY t0.BatteryID, t0.Voltage", new QueryTranslator().Translate(query.Expression));
        var voltageData = query.ToList();

        Assert.Collection(voltageData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(48.1, x.Voltage);
                Assert.Equal(1515.15, x.EnergyThroughput);
            });
    }

    [Fact]
    public void TestJoin()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        IQueryable<ModuleDatum> moduleData = context.Get<ModuleDatum>().AsParquet();
        var query = data.Join(moduleData, bd => bd.Timestamp, md => md.Timestamp, (bd, md) => new
        {
            bd.BatteryID,
            md.ModuleID,
            bd.Timestamp,
            BatteryVoltage = bd.Voltage,
            ModuleVoltage = md.Voltage
        });
        var joinedData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID, t1.ModuleID, t0.Timestamp, t0.Voltage AS BatteryVoltage, t1.Voltage AS ModuleVoltage " +
                     "FROM 'batteryData.parquet' AS t0 " +
                     "JOIN 'moduleData.parquet' AS t1 ON t0.Timestamp=t1.Timestamp", new QueryTranslator().Translate(query.Expression));
        Assert.Collection(joinedData, x =>
            {
                Assert.Equal(1, x.BatteryID);
                Assert.Equal(10, x.ModuleID);
                Assert.Equal(48.1, x.BatteryVoltage);
                Assert.Equal(3.25, x.ModuleVoltage);
            });
    }

    [Fact]
    public void TestJoinByTwoColumns()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<ModuleDatum> data = context.Get<ModuleDatum>().AsParquet();
        IQueryable<AggregatedModuleDatum> aggregatedModule = context.Get<AggregatedModuleDatum>().AsParquet();
        var query = data.Join(aggregatedModule, md => new { md.Timestamp, md.ModuleID },
            amd => new { amd.Timestamp, amd.ModuleID },
            (md, amd) => new
            {
                md.ModuleID,
                md.Timestamp,
                md.Voltage,
                amd.CumulativeEnergyThroughput
            });

        Assert.Equal("SELECT t0.ModuleID, t0.Timestamp, t0.Voltage, t1.CumulativeEnergyThroughput " +
                     "FROM 'moduleData.parquet' AS t0 " +
                     "JOIN 'aggregatedModuleData.parquet' AS t1 " +
                     "ON t0.Timestamp=t1.Timestamp " +
                     "AND t0.ModuleID=t1.ModuleID", new QueryTranslator().Translate(query.Expression));
    }

    [Fact]
    public void TestAsOfJoin()
    {
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet();
        IQueryable<ModuleDatum> moduleData = context.Get<ModuleDatum>().AsParquet();
        var query = data.AsOfJoin(moduleData, bd => bd.Timestamp, md => md.Timestamp, bd => bd.BatteryID, md => md.ModuleID, (bd, md) => new
        {
            bd.BatteryID,
            md.ModuleID,
            bd.Timestamp,
            BatteryVoltage = bd.Voltage,
            ModuleVoltage = md.Voltage
        });
        var joinedData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID, t1.ModuleID, t0.Timestamp, t0.Voltage AS BatteryVoltage, t1.Voltage AS ModuleVoltage " +
            "FROM 'batteryData.parquet' AS t0 " +
            "ASOF JOIN 'moduleData.parquet' AS t1 ON t0.BatteryID=t1.ModuleID AND t0.Timestamp>=t1.Timestamp", new QueryTranslator().Translate(query.Expression));
        Assert.Empty(joinedData);
    }

    [Fact]
    public void TestAsOfJoinWithWherePartitioned()
    {
        DateTime from = new DateTime(2024, 1, 1);
        DateTime to = new DateTime(2024, 2, 1);
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>()
            .AsPartitioned(bd => new
            {
                bd.Timestamp.Year,
                bd.Timestamp.Month,
                bd.Timestamp.Day,
            })
            .Where(bd => bd.Timestamp >= from && bd.Timestamp <= to);
        IQueryable<ModuleDatum> moduleData = context.Get<ModuleDatum>()
            .AsPartitioned(bd => new
            {
                bd.Timestamp.Year,
                bd.Timestamp.Month,
                bd.Timestamp.Day,
            })
            .Where(md => md.Timestamp >= from && md.Timestamp <= to);
        var query = data.AsOfJoin(moduleData, bd => bd.Timestamp, md => md.Timestamp, bd => bd.BatteryID, md => md.ModuleID, (bd, md) => new
        {
            bd.BatteryID,
            md.ModuleID,
            bd.Timestamp
        });

        Assert.Equal(
            "SELECT t0.BatteryID, t1.ModuleID, t0.Timestamp FROM read_parquet('batteryData/*/*/*/*.parquet', hive_partitioning=1) AS t0 " +
            "ASOF JOIN read_parquet('moduleData/*/*/*/*.parquet', hive_partitioning=1) AS t1 " +
            "ON t0.BatteryID=t1.ModuleID AND t0.Timestamp>=t1.Timestamp " +
            "WHERE t0.Timestamp BETWEEN $p0 AND $p1 " +
            "AND t1.Timestamp BETWEEN $p2 AND $p3 " +
            "AND (t0.year=2024 AND ((t0.month=1 AND t0.day>=1) OR (t0.month=2 AND t0.day=1))) " +
            "AND (t1.year=2024 AND ((t1.month=1 AND t1.day>=1) OR (t1.month=2 AND t1.day=1)))",
            new QueryTranslator().Translate(query.Expression));
    }

    [Fact]
    public void TestAsOfJoinWithWhere()
    {
        DateTime from = new DateTime(2024, 1, 1);
        DateTime to = new DateTime(2024, 2, 1);
        DuckDbContext context = new DuckDbContext("DataSource=:memory:");
        IQueryable<BatteryDatum> data = context.Get<BatteryDatum>().AsParquet()
            .Where(bd => bd.Timestamp >= from && bd.Timestamp <= to);
        IQueryable<ModuleDatum> moduleData = context.Get<ModuleDatum>().AsParquet()
            .Where(md => md.Timestamp >= from && md.Timestamp <= to);
        var query = data.AsOfJoin(moduleData, bd => bd.Timestamp, md => md.Timestamp, bd => bd.BatteryID, md => md.ModuleID, (bd, md) => new
        {
            bd.BatteryID,
            md.ModuleID,
            bd.Timestamp
        });
        var joinedData = query.ToList();

        Assert.Equal("SELECT t0.BatteryID, t1.ModuleID, t0.Timestamp " +
                     "FROM 'batteryData.parquet' AS t0 " +
                     "ASOF JOIN 'moduleData.parquet' AS t1 ON t0.BatteryID=t1.ModuleID AND t0.Timestamp>=t1.Timestamp " +
                     "WHERE t0.Timestamp BETWEEN $p0 AND $p1 AND t1.Timestamp BETWEEN $p2 AND $p3", new QueryTranslator().Translate(query.Expression));
        Assert.Empty(joinedData);
    }

    private class BatteryDatum
    {
        public int BatteryID { get; set; }
        public DateTime Timestamp { get; set; }
        public double Voltage { get; set; }
        public double Current { get; set; }
        public int? State { get; set; }
        public int? Error { get; set; }
        public double? MaxChargeCurrent { get; set; }
        public double? MaxDischargeCurrent { get; set; }
    }

    private class ModuleDatum
    {
        public int ModuleID { get; set; }
        public DateTime Timestamp { get; set; }
        public double Voltage { get; set; }
        public int? State { get; set; }
        public int? Error { get; set; }
    }

    private class AggregatedModuleDatum
    {
        public int ModuleID { get; set; }
        public DateTime Timestamp { get; set; }
        public double? CumulativeEnergyThroughput { get; set; }
    }
}