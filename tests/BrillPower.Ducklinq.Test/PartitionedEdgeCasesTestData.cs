using System;
using Xunit;

namespace BrillPower.Ducklinq.Test;

public class PartitionedEdgeCasesTestData : TheoryData<DateTime, DateTime, string>
{
    public PartitionedEdgeCasesTestData()
    {
        string commonSqlPrefix = "SELECT * FROM read_parquet('batteryData/*/*/*/*.parquet', hive_partitioning=1) AS t0 WHERE t0.Timestamp BETWEEN $p0 AND $p1 AND";
        Add(new DateTime(2023, 12, 31), new DateTime(2024, 1, 1),
            $"{commonSqlPrefix} ((t0.year=2023 AND (t0.month=12 AND t0.day=31)) OR (t0.year=2024 AND (t0.month=1 AND t0.day=1)))");
        Add(new DateTime(2023, 12, 1), new DateTime(2024, 1, 31),
            $"{commonSqlPrefix} ((t0.year=2023 AND (t0.month=12 AND t0.day>=1)) OR (t0.year=2024 AND (t0.month=1 AND t0.day<=31)))");
        Add(new DateTime(2023, 1, 1), new DateTime(2024, 12, 31),
            $"{commonSqlPrefix} ((t0.year=2023 AND ((t0.month=1 AND t0.day>=1) OR t0.month BETWEEN 2 AND 12)) OR (t0.year=2024 AND (t0.month BETWEEN 1 AND 11 OR (t0.month=12 AND t0.day<=31))))");
        Add(new DateTime(2022, 1, 1), new DateTime(2024, 12, 31),
            $"{commonSqlPrefix} (((t0.year=2022 AND ((t0.month=1 AND t0.day>=1) OR t0.month BETWEEN 2 AND 12)) OR t0.year=2023) OR (t0.year=2024 AND (t0.month BETWEEN 1 AND 11 OR (t0.month=12 AND t0.day<=31))))");
        Add(new DateTime(2020, 12, 31), new DateTime(2024, 1, 1),
            $"{commonSqlPrefix} (((t0.year=2020 AND (t0.month=12 AND t0.day=31)) OR t0.year BETWEEN 2021 AND 2023) OR (t0.year=2024 AND (t0.month=1 AND t0.day=1)))");
        Add(new DateTime(2019, 9, 10), new DateTime(2029, 5, 18),
            $"{commonSqlPrefix} (((t0.year=2019 AND ((t0.month=9 AND t0.day>=10) OR t0.month BETWEEN 10 AND 12)) OR t0.year BETWEEN 2020 AND 2028) OR (t0.year=2029 AND (t0.month BETWEEN 1 AND 4 OR (t0.month=5 AND t0.day<=18))))");

    }
}