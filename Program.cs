using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace ConsoleApplication
{
    public class Program
    {
        private const int _id = 1;
        private const int _threads = 64;
        private static readonly TimeSpan _duration = TimeSpan.FromSeconds(10);

        private static string _connectionString;
        
        private static int _stopwatchStarted;
        private static Stopwatch _stopwatch = new Stopwatch();

        private static long _requests;
        private static long _connections;

        public static void Main(string[] args)
        {
            var config = new ConfigurationBuilder().
                SetBasePath(Directory.GetCurrentDirectory()).
                AddJsonFile("appsettings.json").
                Build();

            _connectionString = config.GetConnectionString("DefaultConnection");

            Init();

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            WriteResults();
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            var showHelp = true;
            if (args.Any(a => a.Equals("SqlDataReader", StringComparison.OrdinalIgnoreCase))) {
                Test(TestSqlDataReader);
                showHelp = false;
            }

            if (args.Any(a => a.Equals("EF", StringComparison.OrdinalIgnoreCase))) {
                Test(TestEf);
                showHelp = false;
            }

            if (args.Any(a => a.Equals("EFSingleTable", StringComparison.OrdinalIgnoreCase))) {
                Test(TestEfSingleTable);
                showHelp = false;
            }

            if (showHelp) {
                Console.WriteLine("sql.exe [SqlDataReader|EF|EFSingleTable]");
            }
        }

        private static void Test(Action action) {
            Log($"Running test for {_duration}...");

            var threadObjects = new Thread[_threads];
            for (var i = 0; i < _threads; i++)
            {
                var thread = new Thread(() =>
                {
                    action();
                });
                threadObjects[i] = thread;                
            }

            for (var i = 0; i < _threads; i++)
            {
                threadObjects[i].Start();
            }

            for (var i = 0; i < _threads; i++)
            {
                threadObjects[i].Join();
            }                        
        }

        private static void EnsureWatchStarted() {
            if (Interlocked.Exchange(ref _stopwatchStarted, 1) == 0) {
                _stopwatch.Start();
            }
        }

        private static void TestEf() {           
            using (var context = new SqlContext(new DbContextOptionsBuilder().UseSqlServer(_connectionString).Options)) {
                Interlocked.Increment(ref _connections);

                while (true) {
                    var pet = context.Pets.AsNoTracking().Include(p => p.Images).Include(p => p.Tags).FirstOrDefault(p => p.Id == _id);
                    Interlocked.Increment(ref _requests);
                    EnsureWatchStarted();
                }
            } 
        }

        private static void TestEfSingleTable() {           
            using (var context = new SqlContext(new DbContextOptionsBuilder().UseSqlServer(_connectionString).Options)) {
                Interlocked.Increment(ref _connections);

                while (true) {
                    var pet = context.Pets.FirstOrDefault(p => p.Id == _id);
                    Interlocked.Increment(ref _requests);
                    EnsureWatchStarted();
                }
            } 
        }

        private static void TestSqlDataReader() {
            const string selectCmd =
                @"
                SELECT Id, Name       
                FROM Pets
                WHERE Pets.Id = @id;

                SELECT Id, Url, PetId
                FROM Images
                WHERE PetId = @id;

                SELECT Id, Name, PetId
                FROM Tags
                WHERE PetId = @id;                
                ";
            
            using (var connection = new SqlConnection(_connectionString)) {
                using (var command = new SqlCommand(selectCmd, connection)) {
                    command.Parameters.Add("@id", SqlDbType.Int);
                    command.Parameters["@id"].Value = _id;
                    
                    connection.Open();
                    Interlocked.Increment(ref _connections);

                    while (true) {
                        using (var reader = command.ExecuteReader()) {
                            var pet = ReadPet(reader); 
                            Interlocked.Increment(ref _requests);
                            EnsureWatchStarted();                            
                        }
                    }
                }
            }  
        }

        private static Pet ReadPet(SqlDataReader reader)
        {
            reader.Read();

            var pet = new Pet();
            pet.Id = (int)reader["Id"];
            pet.Name = (string)reader["Name"];

            reader.NextResult();
            pet.Images = new List<Image>();
            while (reader.Read())
            {
                var image = new Image();
                image.Id = (int)reader["Id"];
                image.Url = (string)reader["Url"];
                pet.Images.Add(image);
            }

            reader.NextResult();
            pet.Tags = new List<Tag>();
            while (reader.Read())
            {
                var tag = new Tag();
                tag.Id = (int)reader["Id"];
                tag.Name = (string)reader["Name"];
                pet.Tags.Add(tag);
            }

            return pet;
        }

        private static void Init() {
            Log("Initializing DB...");

            const string createCmd =
                @"
                IF OBJECT_ID(N'dbo.Pets', N'U') IS NULL
                BEGIN
                    CREATE TABLE [dbo].[Pets](
                        [Id] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        [Name] [nvarchar](max) NOT NULL,
                    )

                    INSERT INTO [dbo].[Pets] ([Name]) VALUES ('Fluffy')
                END

                IF OBJECT_ID(N'dbo.Images', N'U') IS NULL
                BEGIN
                    CREATE TABLE [dbo].[Images](
                        [Id] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,                        
                        [Url] [nvarchar](max) NOT NULL,
                        [PetId] [int] FOREIGN KEY REFERENCES [dbo].[Pets](Id)                        
                    )

                    INSERT INTO [dbo].[Images] ([Url], [PetId]) VALUES ('http://example.com/1.jpg', 1)
                    INSERT INTO [dbo].[Images] ([Url], [PetId]) VALUES ('http://example.com/2.jpg', 1)                    
                END

                IF OBJECT_ID(N'dbo.Tags', N'U') IS NULL
                BEGIN
                    CREATE TABLE [dbo].[Tags](
                        [Id] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,                        
                        [Name] [nvarchar](max) NOT NULL,
                        [PetId] [int] FOREIGN KEY REFERENCES [dbo].[Pets](Id)                        
                    )

                    INSERT INTO [dbo].[Tags] ([Name], [PetId]) VALUES ('Dog', 1)
                    INSERT INTO [dbo].[Tags] ([Name], [PetId]) VALUES ('Small', 1)                    
                END
                ";
            
            using (var connection = new SqlConnection(_connectionString)) {
                connection.Open();
                
                using (var command = new SqlCommand(createCmd, connection)) {
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void Log(string message)
        {
            Console.WriteLine($"[{DateTime.Now.ToString("HH:mm:ss.fff")}] {message}");
        }

        private static async Task WriteResults()
        {
            var lastRequests = (long)0;
            var lastElapsed = TimeSpan.Zero;

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                var currentRequests = _requests - lastRequests;
                lastRequests = _requests;

                var elapsed = _stopwatch.Elapsed;
                var currentElapsed = elapsed - lastElapsed;
                lastElapsed = elapsed;

                WriteResult(_requests, currentRequests, currentElapsed);

                if (elapsed > _duration) {
                    Console.WriteLine();
                    Console.WriteLine($"Average RPS: {Math.Round(_requests / elapsed.TotalSeconds)}");
                    Environment.Exit(0);
                }
            }
        }

        private static void WriteResult(long totalRequests, long currentRequests, TimeSpan elapsed)
        {
            Log($"Connections: {_connections}, Requests: {totalRequests}, RPS: {Math.Round(currentRequests / elapsed.TotalSeconds)}");
        }                
    }

    public class Pet
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public List<Image> Images { get; set; }
        public List<Tag> Tags { get; set; } 
    }

    public class Image
    {
        public int Id { get; set; }
        public string Url { get; set; }
    }

    public class Tag
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class SqlContext : DbContext {
        public SqlContext(DbContextOptions options) : base(options) { }

        public DbSet<Pet> Pets { get; set; }
        public DbSet<Image> Images { get; set; }
        public DbSet<Tag> Tags { get; set; }
    }
}
