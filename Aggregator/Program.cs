using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Aggregator
{
    class Program
    {
        static void Main(string[] args)
        {
            new Program().Start();
        }

        void Start()
        {
            var connectionString =
                @"Data Source=localhost;Initial Catalog=testing;User ID=sa;Password=yourStrong(!)Password";


            int amountOfTeams = 4;
            var matchesAgainstSameTeam = 2;
            int amountOfWeeks = (amountOfTeams - 1) * matchesAgainstSameTeam;

            var matchesTable = $"matches_{amountOfTeams}_teams";
            var scheduleTable = $"schedule_{amountOfTeams}_teams_{matchesAgainstSameTeam}_matches";
            
            var selects = string.Join(",",
                Enumerable.Range(1, amountOfWeeks).Select(weekNr =>
                    $"week_{weekNr}.combination as week_{weekNr}_combination"));

            var query = BuildSelect(selects, matchesTable, amountOfWeeks);
            var countQuery = BuildSelect("COUNT(*) as combination_count", matchesTable, amountOfWeeks);

            var possibleCombinations = amountOfWeeks < 5 ? int.MaxValue : GetPossibleCombinations(connectionString, countQuery);
            
            int counter = 0;
            int validSchemaCounter = 0;
            using (SqlConnection writeConnection = new SqlConnection(connectionString))
            {
                writeConnection.Open();
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    SqlCommand command = new SqlCommand(query, connection);
                    connection.Open();

                    SetupPersistence(connection, scheduleTable);

                    SqlDataReader reader = command.ExecuteReader();

                    var validator = new SchemaValidator();
                    try
                    {
                        while (reader.Read())
                        {
                            var schedule = Enumerable.Range(1, amountOfWeeks)
                                .Select(weekNr => reader[$"week_{weekNr}_combination"].ToString()).ToList();


                            if (validator.IsValid(schedule))
                            {
                                PersistSchedule(writeConnection, scheduleTable, validator.GetMatchesByRound(schedule));
                                validSchemaCounter++;
                            }

                            counter++;

                            if (counter % 1000000 == 0)
                            {
                                var progressPercentage = Math.Round((double) counter / possibleCombinations * 100, 4);
                                var validPercentage = Math.Round((double) validSchemaCounter / counter * 100,4);
                                
                                Console.WriteLine(
                                    $"Processed {counter}/{possibleCombinations} ({progressPercentage}%) of the items! {validSchemaCounter}/{counter} {validPercentage}% were valid schemas!");
                            }
                        }
                    }
                    finally
                    {
                        Console.WriteLine(
                            $"Final: Processed {counter} items! {validSchemaCounter}/{counter} were valid schemas!");
                        reader.Close();
                    }
                }
                
                writeConnection.Close();

            }
        }

        private int GetPossibleCombinations(string connectionString, string countQuery)
        {
            using (SqlConnection writeConnection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(countQuery, writeConnection);
                writeConnection.Open();
                SqlDataReader reader = command.ExecuteReader();

                reader.Read();

                var result = int.Parse(reader["combination_count"].ToString());
                

                writeConnection.Close();

                return result;
            }
        }

        private string BuildSelect(string select, string matchesTable, int amountOfWeeks)
        {
            var joins = string.Join(" ",
                Enumerable.Range(2, amountOfWeeks - 1).Select(weekNr => $"CROSS JOIN {matchesTable} week_" + weekNr));
            
            return $"SELECT {select} FROM {matchesTable} week_1 " + joins;
        }
        
        private static void PersistSchedule(SqlConnection connection, string tableName,
            List<IGrouping<int, Match>> schedule)
        {
            string scheduleId = Guid.NewGuid().ToString();
            var matchesSql = "";

            bool isFirstRow = true;
            foreach (var round in schedule)
            {
                foreach (var match in round)
                {
                    var prefix = isFirstRow ? "" : ",";
                    matchesSql += $"{prefix}('{scheduleId}','{round.Key}','{match.Placement}','{match.HomeTeam}', '{match.AwayTeam}')";
                    isFirstRow = false;
                }
            }
            
            

            var sqlInsert = $"INSERT INTO {tableName}(schedule_id, round_id, placement, home_team, away_team) VALUES {matchesSql}";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }

        private static void SetupPersistence(SqlConnection connection, string tableName)
        {
            var sqlInsert =
                $"DROP TABLE IF EXISTS {tableName}; create table {tableName} (schedule_id uniqueidentifier, round_id int, placement int, home_team char,away_team char);";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }
    }
}