using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Numerics;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Aggregator
{
    class Program
    {
        public static volatile int ProcessedSchedulesCount = 0;
        public static volatile int ValidSchedulesCount = 0;

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine(
                    "First argument should be amount of teams, second should be matches against same team, third should be amount of threads");
                return;
            }

            int amountOfTeams = int.Parse(args[0]);
            var matchesAgainstSameTeam = int.Parse(args[1]);
            
            var connectionString =
                @"Data Source=localhost;Initial Catalog=testing;User ID=sa;Password=yourStrong(!)Password";

            var scheduleTable = $"schedule_{amountOfTeams}_teams_{matchesAgainstSameTeam}_matches";


            Console.WriteLine(
                $"Generating schedules for {amountOfTeams} teams that play {matchesAgainstSameTeam} matches. MultiThreaded");


            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                SetupPersistence(connection, scheduleTable);

                connection.Close();
            }


            var worker = new ScheduleWorker(Guid.NewGuid().ToString(), connectionString, scheduleTable,
                amountOfTeams, matchesAgainstSameTeam);

            worker.Work();
        }

        private static void SetupPersistence(SqlConnection connection, string tableName)
        {
            var sqlInsert =
                $"DROP TABLE IF EXISTS {tableName}; create table {tableName} (schedule_id uniqueidentifier, rounds varchar(250));";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }
    }
}