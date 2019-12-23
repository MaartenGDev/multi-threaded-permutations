using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace ScheduleShifter
{
    class Program
    {
        static void Main(string[] args)
        {
            var alphabet = "abcdefghijklmnopqrstuvw";
            var amountOfTeams = 4;

            string options = alphabet.Substring(0,amountOfTeams);

            var sourceTeams = options.ToCharArray();
            var teams = options.ToCharArray();

            var possibleCombinations = GetPossibleCombinations(options);
            
            
            var sourceTables = new Dictionary<int, string>()
            {
                {4, "acdb|bacd|cbda"},
                {6, "aebdfc|cbdaef|edacfb|bacedf|dcebfa"},
                {8, "agbfcehd|dcebfagh|gfaebdhc|cbdaegfh|fegdachb|bacgdfeh|edfcgbha"},
                {10, "aibhcgdfje|edfcgbhaij|ihagbfcejd|dcebfagihj|hgifaebdjc|cbdaeifhgj|gfheidacjb|bacidhegfj|fegdhcibja"},
                {12, "akbjcidheglf|fegdhcibjakl|kjaibhcgdfle|edfcgbhaikjl|jikhagbfceld|dcebfagkhjil|ihjgkfaebdlc|cbdaekfjgihl|hgifjekdaclb|backdjeifhgl|gfheidjckbla"},
                {14, "amblckdjeifhng|gfheidjckblamn|mlakbjcidhegnf|fegdhcibjakmln|lkmjaibhcgdfne|edfcgbhaimjlkn|kjlimhagbfcend|dcebfagmhlikjn|jikhlgmfaebdnc|cbdaemflgkhjin|ihjgkflemdacnb|bacmdlekfjgihn|hgifjekdlcmbna"},
            };
            
            // 123456789 10 11 12 13 14
            // abcdefghi j   k   l  m  n

            var combination = sourceTables.GetValueOrDefault(amountOfTeams);

            var table = new DataTable();
            
            table.Columns.Add(new DataColumn
            {
                ColumnName = "sequence",
                DataType = typeof(string),
                MaxLength = combination.Length,
            });

            
            var connectionString = @"Data Source=localhost;Initial Catalog=testing;User ID=sa;Password=yourStrong(!)Password";

            var cnn = new SqlConnection(connectionString);

            var tableName = $"sequences_{amountOfTeams}_teams";
            cnn.Open();

            ClearPersistence(cnn, tableName, combination.Length);
            
            var bulk = new SqlBulkCopy(cnn)
                {DestinationTableName = tableName, BulkCopyTimeout = 0};
            
            var generatedSchedules = 0;
            var lastInsert = 0;
            
            var targetPermutations = 40000000;
            // 2x 6325
            // 3x = 350
            
            var requiredMatches = 350;
            
            ForAllPermutation(teams, (permutation, isFinished) =>
            {
                lastInsert++;

                if (!isFinished)
                {
                    generatedSchedules++;

                    var localCombination = combination;

                    for (var teamIndex = 0; teamIndex < sourceTeams.Length; teamIndex++)
                    {
                        localCombination = localCombination.Replace(sourceTeams[teamIndex],
                            char.ToUpper(permutation[teamIndex]));
                    }

                    table.Rows.Add(localCombination.ToLower());
                }
                
                if (lastInsert > 1000000 || isFinished | generatedSchedules >= requiredMatches)
                {
                    bulk.WriteToServer(table);
                    table.Clear();
                    lastInsert = 0;
                }

                return generatedSchedules >= requiredMatches;
            });

            Console.WriteLine($"Generated {generatedSchedules} schedules!");
            cnn.Close();
        }

        private static int CalculateRequiredRecordsForCoverage(int maxPermutations, int amountOfTimesAgainstSameTeam)
        {
            var requiredMatches = maxPermutations;
            
            for (int i = 0; i < amountOfTimesAgainstSameTeam -1; i++)
            {
                requiredMatches = (int) Math.Ceiling(Math.Sqrt(requiredMatches));
            }

            return requiredMatches;
        }

        private static void ClearPersistence(SqlConnection connection, string tableName, int columnSize)
        {
            var sqlInsert =
                $"DROP TABLE IF EXISTS {tableName}; CREATE table {tableName} (sequence varchar({columnSize}), CONSTRAINT pk_{tableName} PRIMARY KEY (sequence));";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }
        

        public static bool ForAllPermutation<T>(T[] items, Func<T[], bool, bool> funcExecuteAndTellIfShouldStop)
        {
            int countOfItem = items.Length;

            if (countOfItem <= 1)
            {
                return funcExecuteAndTellIfShouldStop(items, false);
            }

            var indexes = new int[countOfItem];
            for (int i = 0; i < countOfItem; i++)
            {
                indexes[i] = 0;
            }

            if (funcExecuteAndTellIfShouldStop(items, false))
            {
                return true;
            }

            for (int i = 1; i < countOfItem;)
            {
                if (indexes[i] < i)
                {
                    // On the web there is an implementation with a multiplication which should be less efficient.
                    if ((i & 1) == 1) // if (i % 2 == 1)  ... more efficient ??? At least the same.
                    {
                        Swap(ref items[i], ref items[indexes[i]]);
                    }
                    else
                    {
                        Swap(ref items[i], ref items[0]);
                    }

                    if (funcExecuteAndTellIfShouldStop(items, false))
                    {
                        return true;
                    }

                    indexes[i]++;
                    i = 1;
                }
                else
                {
                    indexes[i++] = 0;
                }
            }

            funcExecuteAndTellIfShouldStop(new T[] { }, true);

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void Swap<T>(ref T a, ref T b)
        {
            T temp = a;
            a = b;
            b = temp;
        }
        
        private static int GetPossibleCombinations(string dataset)
        {
            var input = dataset.Length;
            var result = 1;

            for (var i = input; i > 0; i--)
            {
                result *= i;
            }

            return result;
        }

    }
}

public static class StringExtensions
{
    public static string Repeat(this string s, int n)
        => new StringBuilder(s.Length * n).Insert(0, s, n).ToString();
}