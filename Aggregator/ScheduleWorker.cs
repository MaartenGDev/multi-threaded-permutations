using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Aggregator
{
    public class ScheduleWorker
    {
        private readonly string _connectionString;
        private readonly string _scheduleTable;
        private readonly int _amountOfTeams;
        private readonly int _matchesAgainstSameTeam;

        public ScheduleWorker(string threadId, string connectionString, string scheduleTable, int amountOfTeams,
            int matchesAgainstSameTeam)
        {
            _connectionString = connectionString;
            _scheduleTable = scheduleTable;
            _amountOfTeams = amountOfTeams;
            _matchesAgainstSameTeam = matchesAgainstSameTeam;
        }

        public void Work()
        {
            int amountOfWeeks = (_amountOfTeams - 1) * _matchesAgainstSameTeam;

            var matchesTable = $"matches_{_amountOfTeams}_teams";
            

            var query = BuildSelect(matchesTable, _amountOfTeams, amountOfWeeks);

            using (SqlConnection writeConnection = new SqlConnection(_connectionString))
            {
                writeConnection.Open();
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    SqlCommand command = new SqlCommand(query, connection) {CommandTimeout = 0};
                    connection.Open();


                    SqlDataReader reader = command.ExecuteReader();

                    var validator = new SchemaValidator();
                    
                    while (reader.Read())
                    {
                        Program.ProcessedSchedulesCount++;

                        var schedule = Enumerable.Range(1, amountOfWeeks)
                            .Select(weekNr => reader[$"week_{weekNr}_combination"].ToString()).ToList();

                        if (validator.IsValid(schedule))
                        {
                            Program.ValidSchedulesCount++;
                            
                            PersistSchedule(writeConnection, _scheduleTable, validator.GetMatchesByRound(schedule));
                        }
                        
                        
                        if (Program.ProcessedSchedulesCount % 1000000 == 0)
                        {
                            PrintProgress();
                        }
                    }


                    reader.Close();
                }


               
                writeConnection.Close();
            }
        }

        private void PrintProgress()
        {
            var validPercentage =
                Math.Round((double) Program.ValidSchedulesCount / Program.ProcessedSchedulesCount * 100, 4);

            Console.WriteLine($"{Program.ValidSchedulesCount}/{Program.ProcessedSchedulesCount} {validPercentage}% were valid schemas!");
        }

        private static string BuildSelect(string matchesTable, int amountOfTeams, int amountOfWeeks)
        {
            var select = string.Join(",",
                Enumerable.Range(1, amountOfWeeks).Select(weekNr =>
                {
                    var parts = string.Join("+",Enumerable.Range(1, amountOfTeams).Select(teamId => $"week_{weekNr}.part_{teamId}"));
                    
                    return $"{parts} as week_{weekNr}_combination";
                }));
            
            var joins = string.Join(" ",
                Enumerable.Range(2, amountOfWeeks - 1).Select(weekNr => $"CROSS JOIN {matchesTable} week_" + weekNr));

            var where = "WHERE ";
            
            
            for (int weekIndex = 0; weekIndex < amountOfWeeks; weekIndex += 2)
            {
                int weekNr = weekIndex + 1;

                bool isNotLastRound = (weekNr + 2) < amountOfWeeks;
                bool isLastRound = (weekNr + 2) >= amountOfWeeks;

                for (int partIndex = 0; partIndex < amountOfTeams; partIndex += 2)
                {
                    var partNr = partIndex + 1;
                 
                    bool isLastStatement = isLastRound && (partIndex + 2) >= amountOfTeams;
                
                    var andStatement = isLastStatement ? "" : "AND";
                    
                    where +=
                        $"NOT (week_{weekNr}.part_{partNr} = week_{weekNr + 1}.part_{partNr} AND week_{weekNr}.part_{partNr + 1} = week_{weekNr + 1}.part_{partNr + 1}) AND ";
                
                    where +=
                        $"NOT (week_{weekNr}.part_{partNr} = week_{weekNr + 1}.part_{partNr +1} AND week_{weekNr}.part_{partNr + 1} = week_{weekNr + 1}.part_{partNr}) {andStatement} ";

                    if (isNotLastRound)
                    {
                        where +=
                            $"NOT (week_{weekNr + 1}.part_{partNr} = week_{weekNr + 2}.part_{partNr} AND week_{weekNr + 1}.part_{partNr + 1} = week_{weekNr + 2}.part_{partNr + 1}) AND ";

                        where +=
                            $"NOT (week_{weekNr + 1}.part_{partNr} = week_{weekNr + 2}.part_{partNr + 1} AND week_{weekNr + 1}.part_{partNr + 1} = week_{weekNr + 2}.part_{partNr}) AND ";   
                    }
                }
            }
            
            var query =$"SELECT {select} FROM {matchesTable} week_1 {joins} {where}";

            return query;
        }

        private static void PersistSchedule(SqlConnection connection, string tableName,
            List<IGrouping<int, Match>> schedule)
        {
            var scheduleId = Guid.NewGuid().ToString();
            var matchesSql = "";

            var firstRound = true;
            foreach (var round in schedule)
            {
                matchesSql += firstRound ? "" : "|";
                firstRound = false;

                foreach (var match in round)
                {
                    matchesSql += $"{match.HomeTeam}{match.AwayTeam}";
                }

            }


            var sqlInsert =
                $"INSERT INTO {tableName}(schedule_id, rounds) VALUES ('{scheduleId}', '{matchesSql}')";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }
    }
}