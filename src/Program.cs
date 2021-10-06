using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using static Dapper.SqlMapper;

namespace BE4371
{
    static class Program
    {
        ////const string _connectionString = "Server=dm5;Initial Catalog=epicuro;Integrated Security=True;";
        const string _connectionString = "Server=dm5;Initial Catalog=epicurobrooksprod;Integrated Security=True;";
        static async Task Main(string[] args)
        {
            List<string> inventoryIds = new string[]
            {
"76b0a8b3cd4143669078510e877e4fa2",
"INV_NA_120_AO",
"INV_US_V_120_AO",
"INV_NA_120_FUT",
"INV_NA_120_TEMPO_FUT",
"INV_US_V_120_FUT",
"76b0a8b3cd4143669078510e877e4fa2",
"accountinventory-108340",
"INV_US_FF_120_AO",
"INV_US_V_120_AO",
"da6e4b71b7654593944f831f26802b52",
"INV_NA_120_TEMPO_FUT",
"INV_US_FF_120_FUT",
"INV_US_V_120_FUT"
            }.Distinct().OrderBy(x => x).ToList();
            List<string> productIds = new string[]
            {
"120329",
"120329013",
"120329016",
"120329020",
"120329033",
"120329042",
"120329050",
"120329054",
"120329061",
"120329083",
"120329123",
"120329134",
"120329135",
"120329137",
"120329147",
"120329149",
"120329153",
"120329162",
"120329166",
"120329171",
"120329177",
"120329182",
"120329193",
"120329199",
"120329276",
"120329339",
"120329353",
"120329380",
"120329408",
"120329432",
"120329486",
"120329528",
"120329529",
"120329586",
"120329592",
"120329664",
"120329675",
"120329SMU",
"1203291B013.050",
"1203291B013.055",
"1203291B013.060",
"1203291B013.065",
"1203291B013.070",
"1203291B013.075",
"1203291B013.080",
"1203291B013.085",
"1203291B013.090",
"1203291B013.095",
"1203291B013.100",
"1203291B013.105",
"1203291B013.110",
"1203291B013.115",
"1203291B013.120",
"1203291B013.130",
"1203291B016.050",
"1203291B016.055",
"1203291B016.060",
"1203291B016.065",
"1203291B016.070",
"1203291B016.075",
"1203291B016.080",
"1203291B016.085",
"1203291B016.090",
"1203291B016.095",
"1203291B016.100",
"1203291B016.105",
"1203291B016.110",
"1203291B016.115",
"1203291B016.120",
"1203291B016.130",
"1203291B020.050",
"1203291B020.055",
"1203291B020.060",
"1203291B020.065",
"1203291B020.070",
"1203291B020.075",
"1203291B020.080",
"1203291B020.085",
"1203291B020.090",
"1203291B020.095",
"1203291B020.100",
"1203291B020.105",
"1203291B020.110",
"1203291B020.115",
"1203291B020.120",
"1203291B020.130",
"1203291D020.050",
"1203291D020.055",
"1203291D020.060",
"1203291D020.065",
"1203291D020.070",
"1203291D020.075",
"1203291D020.080",
"1203291D020.085",
"1203291D020.090",
"1203291D020.095",
"1203291D020.100",
"1203291D020.105",
"1203291D020.110",
"1203291D020.115",
"1203291D020.120",
"1203291D020.130",
"1203291B033.050",
"1203291B033.055",
"1203291B033.060",
"1203291B033.065",
"1203291B033.070",
"1203291B033.075",
"1203291B033.080",
"1203291B033.085",
"1203291B033.090",
"1203291B033.095",
"1203291B033.100",
"1203291B033.105",
"1203291B033.110",
"1203291B033.115",
"1203291B033.120",
"1203291B033.130",
"1203291D033.050",
"1203291D033.055",
"1203291D033.060",
"1203291D033.065",
"1203291D033.070",
"1203291D033.075",
"1203291D033.080",
"1203291D033.085",
"1203291D033.090",
"1203291D033.095",
"1203291D033.100",
"1203291D033.105",
"1203291D033.110",
"1203291D033.115",
"1203291D033.120",
"1203291D033.130",
"1203291B042.050",
"1203291B042.055",
"1203291B042.060",
"1203291B042.065",
"1203291B042.070",
"1203291B042.075",
"1203291B042.080",
"1203291B042.085",
"1203291B042.090",
"1203291B042.095",
"1203291B042.100",
"1203291B042.105",
"1203291B042.110",
"1203291B042.115",
"1203291B042.120",
"1203291B042.130",
"1203291B050.050",
"1203291B050.055",
"1203291B050.060",
"1203291B050.065",
"1203291B050.070",
"1203291B050.075",
"1203291B050.080",
"1203291B050.085",
"1203291B050.090",
"1203291B050.095",
"1203291B050.100",
"1203291B050.105",
"1203291B050.110",
"1203291B050.115",
"1203291B050.120",
"1203291B050.130",
"1203291D050.050",
"1203291D050.055",
"1203291D050.060",
"1203291D050.065",
"1203291D050.070",
"1203291D050.075",
"1203291D050.080",
"1203291D050.085",
"1203291D050.090",
"1203291D050.095",
"1203291D050.100",
"1203291D050.105",
"1203291D050.110",
"1203291D050.115",
"1203291D050.120",
"1203291D050.130",
"1203291B054.050",
"1203291B054.055",
"1203291B054.060",
"1203291B054.065",
"1203291B054.070",
"1203291B054.075",
"1203291B054.080",
"1203291B054.085",
"1203291B054.090",
"1203291B054.095",
"1203291B054.100",
"1203291B054.105",
"1203291B054.110",
"1203291B054.115",
"1203291B054.120",
"1203291B054.130",
"1203291D054.050",
"1203291D054.055",
"1203291D054.060",
"1203291D054.065",
"1203291D054.070",
"1203291D054.075",
"1203291D054.080",
"1203291D054.085",
"1203291D054.090",
"1203291D054.095",
"1203291D054.100",
"1203291D054.105",
"1203291D054.110",
"1203291D054.115",
"1203291D054.120",
"1203291D054.130",
"1203292A054.060",
"1203292A054.065",
"1203292A054.070",
"1203292A054.075",
"1203292A054.080",
"1203292A054.085",
"1203292A054.090",
"1203292A054.095",
"1203292A054.100",
"1203292A054.105",
"1203292A054.110",
"1203292A054.115",
"1203292A054.120",
"1203292A054.130",
"1203291B061.050",
"1203291B061.055",
"1203291B061.060",
"1203291B061.065",
"1203291B061.070",
"1203291B061.075",
"1203291B061.080",
"1203291B061.085",
"1203291B061.090",
"1203291B061.095",
"1203291B061.100",
"1203291B061.105",
"1203291B061.110",
"1203291B061.115",
"1203291B061.120",
"1203291B061.130",
"1203291D061.050",
"1203291D061.055",
"1203291D061.060",
"1203291D061.065",
"1203291D061.070",
"1203291D061.075",
"1203291D061.080",
"1203291D061.085",
"1203291D061.090",
"1203291D061.095",
"1203291D061.100",
"1203291D061.105",
"1203291D061.110",
"1203291D061.115",
"1203291D061.120",
"1203291D061.130",
"1203292A061.060",
"1203292A061.065",
"1203292A061.070",
"1203292A061.075",
"1203292A061.080",
"1203292A061.085",
"1203292A061.090",
"1203292A061.095",
"1203292A061.100",
"1203292A061.105",
"1203292A061.110",
"1203292A061.115",
"1203292A061.120",
"1203292A061.130",
"1203292E061.050",
"1203292E061.055",
"1203292E061.060",
"1203291BSMU.100"
            }.OrderBy(x => x).ToList();
            List<string> productIdsCopy = productIds.ToList(); // to keep original
            string brandId = "brooks";
            int runCount = 500, repeatCount = 1, rowCount = 0;

            // Method 1 (reference)
            var sw1 = Stopwatch.StartNew();
            for (int i = 0; i < runCount; i++)
            {
                productIdsCopy.Add(Guid.NewGuid().ToString()); // increase params count
                for (int y = 0; y < repeatCount; y++)
                {
                    List<InventoryItemRow> results = await Get1Async(inventoryIds, brandId, productIdsCopy);
                    rowCount += results.Count;
                }
            }
            sw1.Stop();
            Console.WriteLine($"Method #1\tRun count: {runCount} Repeats: {repeatCount} Total rows: {rowCount} Total time: {sw1.Elapsed}.");

            // Method 2
            productIdsCopy = productIds.ToList(); //reset
            rowCount = 0;
            var sw2 = Stopwatch.StartNew();
            for (int i = 0; i < runCount; i++)
            {
                productIdsCopy.Add(Guid.NewGuid().ToString()); // increase params count
                for (int y = 0; y < repeatCount; y++)
                {
                    List<InventoryItemRow> results = await Get2Async(inventoryIds, brandId, productIdsCopy);
                    rowCount += results.Count;
                }
            }
            sw2.Stop();
            double factor = Math.Round(sw1.Elapsed.TotalMilliseconds / sw2.Elapsed.TotalMilliseconds, 2);
            Console.WriteLine($"Method #2\tRun count: {runCount} Repeats: {repeatCount} Total rows: {rowCount} Total time: {sw2.Elapsed}. Time factor: {factor}.");

            // Method 3
            productIdsCopy = productIds.ToList(); //reset
            rowCount = 0;
            var sw3 = Stopwatch.StartNew();
            for (int i = 0; i < runCount; i++)
            {
                productIdsCopy.Add(Guid.NewGuid().ToString()); // increase params count
                for (int y = 0; y < repeatCount; y++)
                {
                    List<InventoryItemRow> results = await Get3Async(inventoryIds, brandId, productIdsCopy);
                    rowCount += results.Count;
                }
            }
            sw3.Stop();
            factor = Math.Round(sw1.Elapsed.TotalMilliseconds / sw3.Elapsed.TotalMilliseconds, 2);
            Console.WriteLine($"Method #3\tRun count: {runCount} Repeats: {repeatCount} Total rows: {rowCount} Total time: {sw3.Elapsed}. Time factor: {factor}.");

            Console.WriteLine("Done.");
            Console.ReadLine();
        }

        public static async Task<List<InventoryItemRow>> Get1Async(List<string> inventoryIds, string brandId, List<string> productIds)
        {
            using (IDbConnection connection = new SqlConnection(_connectionString))
            {
                string query = @"/*1*/
SELECT [dbo].[InventoryItem].* 
FROM [dbo].[Inventory]
INNER JOIN [dbo].[InventoryItem] ON [dbo].[Inventory].[Id] = [dbo].[InventoryItem].[InventoryId]
WHERE [dbo].[Inventory].[Id] IN @InventoryIds AND [BrandId] = @BrandId AND [ProductId] IN @ProductIds;";

                return (await connection.QueryAsync<InventoryItemRow>(query, new
                {
                    InventoryIds = inventoryIds.Select(e => new DbString { Value = e, IsAnsi = false, Length = 50 }),
                    BrandId = new DbString { Value = brandId, IsAnsi = true, Length = 50 },
                    ProductIds = productIds.Select(e => new DbString { Value = e, IsAnsi = true, Length = 50 })
                })).ToList();
            }
        }

        public static async Task<List<InventoryItemRow>> Get2Async(List<string> inventoryIds, string brandId, List<string> productIds)
        {
            using (IDbConnection connection = new SqlConnection(_connectionString))
            {
                string query = @"/*2*/
SELECT [dbo].[InventoryItem].*
FROM [dbo].[Inventory]
INNER JOIN [dbo].[InventoryItem] ON [dbo].[Inventory].[Id] = [dbo].[InventoryItem].[InventoryId]
WHERE [dbo].[Inventory].[Id] IN (select Id from @InventoryIds) AND [BrandId] = @BrandId AND [ProductId] IN (select Id from @ProductIds);";

                return (await connection.QueryAsync<InventoryItemRow>(query, new
                {
                    BrandId = brandId,
                    InventoryIds = AsTableValuedParameter(inventoryIds),
                    ProductIds = AsTableValuedParameter(productIds)
                })).ToList();
            }
        }

public static async Task<List<InventoryItemRow>> Get3Async(List<string> inventoryIds, string brandId, List<string> productIds)
{
    const string tempTableQuery = "create table {0} (Id nvarchar(200) collate DATABASE_DEFAULT not null primary key)";
    string InventoryIdsTempTableName = $"#{nameof(Get3Async)}InventoryIds";
    string ProductIdsTempTableName = $"#{nameof(Get3Async)}ProductsIds";

    using (var connection = new SqlConnection(_connectionString))
    {
        connection.Open();
        connection.Execute(string.Format(tempTableQuery, InventoryIdsTempTableName));
        connection.Execute(string.Format(tempTableQuery, ProductIdsTempTableName));
        ExecuteBulkInsert(InventoryIdsTempTableName, AsIdDataTable(inventoryIds), connection);
        ExecuteBulkInsert(ProductIdsTempTableName, AsIdDataTable(productIds), connection);

        string query = $@"/*3*/
SELECT [dbo].[InventoryItem].*
FROM [dbo].[Inventory]
INNER JOIN [dbo].[InventoryItem] ON [dbo].[Inventory].[Id] = [dbo].[InventoryItem].[InventoryId]
WHERE [dbo].[Inventory].[Id] IN (select Id from {InventoryIdsTempTableName}) AND [BrandId] = @BrandId AND [ProductId] IN (select Id from {ProductIdsTempTableName});";

        return (await connection.QueryAsync<InventoryItemRow>(query, new { BrandId = brandId})).ToList();
    }
}

        public static void ExecuteBulkInsert(string tableName, DataTable source, SqlConnection connection, SqlTransaction transaction = null)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                foreach (DataColumn col in source.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                bulkCopy.BatchSize = 5000;
                bulkCopy.BulkCopyTimeout = 120;
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.WriteToServer(source);
            }
        }

        private static DataTable AsIdDataTable(IEnumerable<string> ids)
        {
            var table = new DataTable();
            table.Columns.Add("Id", typeof(string)).MaxLength = 200;
            foreach (var id in ids) { table.Rows.Add(id); }
            return table;
        }

        private static ICustomQueryParameter AsTableValuedParameter(IEnumerable<string> ids)
        {
            var table = AsIdDataTable(ids);
            return table.AsTableValuedParameter("DFType");
        }
    }

    public class InventoryItemRow
    {
        public Guid Id { get; set; }
        public string InventoryId { get; set; }
        public string BrandId { get; set; }
        public string ProductId { get; set; }
        public int? OriginalQuantity { get; set; }
        public int? QuantityDecrement { get; set; }
        public DateTime? ValidityDateFrom { get; set; }
        public DateTime? ValidityDateTo { get; set; }
        public DateTime? AvailabilityDateFrom { get; set; }
        public DateTime? AvailabilityDateTo { get; set; }
        public DateTime? DisplayDateFrom { get; set; }
        public DateTime? DisplayDateTo { get; set; }
        public string ExternalInventoryItemId { get; set; }
        public DateTime? LastUpdatedUtc { get; set; }
        public DateTime? LastDecrementedUtc { get; set; }
        public bool? IsEnabled { get; set; }
    }
}
