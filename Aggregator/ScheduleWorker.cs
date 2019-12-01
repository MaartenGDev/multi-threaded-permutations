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

        private static readonly int _maxMatchesOnSameSide = 3;

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

            var scheduleBuffer = new List<List<IGrouping<int, Match>>>();
            using (SqlConnection writeConnection = new SqlConnection(_connectionString))
            {
                writeConnection.Open();

                using (SqlTransaction transaction = writeConnection.BeginTransaction())
                {
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
                                if (scheduleBuffer.Count > 25)
                                {
                                    PersistSchedules(writeConnection, _scheduleTable, transaction, scheduleBuffer);
                                    scheduleBuffer.Clear();
                                }

                                scheduleBuffer.Add(validator.GetMatchesByRound(schedule));
                            }


                            if (Program.ProcessedSchedulesCount % 10000 == 0)
                            {
                                PrintProgress();
                            }
                        }

                        PersistSchedules(writeConnection, _scheduleTable, transaction, scheduleBuffer);

                        reader.Close();
                    }

                    transaction.Commit();

                    PrintProgress();
                }

                writeConnection.Close();
            }
        }

        private void PrintProgress()
        {
            var validPercentage =
                Math.Round((double) Program.ValidSchedulesCount / Program.ProcessedSchedulesCount * 100, 4);

            Console.WriteLine(
                $"{Program.ValidSchedulesCount}/{Program.ProcessedSchedulesCount} {validPercentage}% were valid schemas!");
        }

        private static string BuildSelect(string matchesTable, int amountOfTeams, int amountOfWeeks)
        {
            var select = string.Join(",",
                Enumerable.Range(1, amountOfWeeks).Select(weekNr =>
                {
                    var parts = string.Join("+",
                        Enumerable.Range(1, amountOfTeams).Select(teamId => $"week_{weekNr}.part_{teamId}"));

                    return $"{parts} as week_{weekNr}_combination";
                }));

            var joins = string.Join(" ",
                Enumerable.Range(2, amountOfWeeks - 1).Select(weekNr => $"CROSS JOIN {matchesTable} week_" + weekNr));

            var where = "WHERE ";


            for (int weekIndex = 0; weekIndex < amountOfWeeks; weekIndex++)
            {
                int weekNr = weekIndex + 1;

                for (int partIndex = 0; partIndex < amountOfTeams; partIndex += 2)
                {
                    var partNr = partIndex + 1;


                    for (int impossibleWeekIndex = 0; impossibleWeekIndex < amountOfTeams - 2; impossibleWeekIndex++)
                    {
                        int impossibleWeekNr = weekNr + impossibleWeekIndex + 1;

                        if (impossibleWeekNr > amountOfWeeks) break;

                        for (int nextPartIndex = 0; nextPartIndex < amountOfTeams; nextPartIndex += 2)
                        {
                            var nextPartNr = nextPartIndex + 1;

                            if (nextPartNr > amountOfTeams) break;


                            where +=
                                $"NOT /* NEXT_WEEKS_SHOULD_NOT_PLAY_SAME_SIDES */ (week_{weekNr}.part_{partNr} = week_{impossibleWeekNr}.part_{nextPartNr} AND week_{weekNr}.part_{partNr + 1} = week_{impossibleWeekNr}.part_{nextPartNr + 1}) AND ";

                            where +=
                                $"NOT /* NEXT_WEEKS_SHOULD_NOT_PLAY_SWITCHED_SIDES */ (week_{weekNr}.part_{partNr} = week_{impossibleWeekNr}.part_{nextPartNr + 1} AND week_{weekNr}.part_{partNr + 1} = week_{impossibleWeekNr}.part_{nextPartNr}) AND ";
                        }
                    }
                }
            }

            for (int weekIndex = 0; weekIndex < amountOfWeeks; weekIndex++)
            {
                var weekNr = weekIndex + 1;

                var possibleNextPlayMoment = weekNr + (amountOfTeams - 1);

                bool isLastWeek = possibleNextPlayMoment >= amountOfWeeks;

                if (possibleNextPlayMoment > amountOfWeeks) break;

                for (int partIndex = 0; partIndex < amountOfTeams; partIndex += 2)
                {
                    var partNr = partIndex + 1;

                    if (partNr > amountOfTeams) break;


                    for (int nextPartIndex = 0; nextPartIndex < amountOfTeams; nextPartIndex += 2)
                    {
                        var nextPartNr = nextPartIndex + 1;

                        if (nextPartNr > amountOfTeams) break;

                        var isLastStatement = isLastWeek && partNr + 1 == amountOfTeams &&
                                              nextPartIndex + 2 >= amountOfTeams;

                        var andStatement = isLastStatement ? "" : "AND";

                        where +=
                            $"NOT /* POSSIBLE_PLAY_MOMENT_NOT_SAME_SIDES */ (week_{weekNr}.part_{partNr} = week_{possibleNextPlayMoment}.part_{nextPartNr} AND week_{weekNr}.part_{partNr + 1} = week_{possibleNextPlayMoment}.part_{nextPartNr + 1}) {andStatement} ";
                    }
                }
            }

            var isFirstWeek = true;
            for (int weekIndex = 0; weekIndex < amountOfWeeks; weekIndex++)
            {
                var weekNr = weekIndex + 1;
                var prefix = isFirstWeek ? "" : "AND";
                isFirstWeek = false;

                var isCompleteCheck = false;

                for (int partIndex = 0; partIndex < amountOfTeams; partIndex++)
                {
                    var playingSameSideCheck = "AND NOT /* SAME_SIDE_CHECK */(";
                    
                    var partNr = partIndex + 1;

                    for (int followingWeekIndex = weekNr; followingWeekIndex < weekNr + _maxMatchesOnSameSide; followingWeekIndex++)
                    {
                        var followingWeekNr = followingWeekIndex + 1;

                        
                        if (followingWeekNr ==  weekNr + _maxMatchesOnSameSide)
                        {
                            isCompleteCheck = true;
                        }
                        
                        if (followingWeekNr > amountOfWeeks)
                        {
                            isCompleteCheck = false;
                            break;
                        }


                        var isLastStatement = followingWeekNr == weekNr + _maxMatchesOnSameSide &&
                                              partNr + 3 >= amountOfTeams;
                        var andStatement = isLastStatement ? "" : "AND";

                        var hasNextGroup = partNr + 2 <= amountOfTeams;
                        
                        var orClause = hasNextGroup
                            ? $"OR week_{weekNr}.part_{partNr} = week_{followingWeekNr}.part_{partNr + 2}) {andStatement} "
                            : ")" + andStatement;
                        
                        playingSameSideCheck +=
                            $"(week_{weekNr}.part_{partNr} = week_{followingWeekNr}.part_{partNr} {orClause} ";
                    }
                    
                    playingSameSideCheck += " )";


                    where += isCompleteCheck ? playingSameSideCheck : "";
                    
                }

            
            }

            var query = $"SELECT {select} FROM {matchesTable} week_1 {joins} {where}";

            return query;
        }

        private static void PersistSchedules(SqlConnection connection, string tableName, SqlTransaction transaction,
            List<List<IGrouping<int, Match>>> schedules)
        {
            var scheduleInserts = "";
            var isFirstSchedule = true;
            foreach (var schedule in schedules)
            {
                var prefix = isFirstSchedule ? "" : ",";
                isFirstSchedule = false;
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

                scheduleInserts += $"{prefix}('{scheduleId}', '{matchesSql}')";
            }


            var sqlInsert =
                $"INSERT INTO {tableName}(schedule_id, rounds) VALUES {scheduleInserts}";
            var command = new SqlCommand(sqlInsert, connection, transaction);

            command.ExecuteNonQuery();
        }
    }
}