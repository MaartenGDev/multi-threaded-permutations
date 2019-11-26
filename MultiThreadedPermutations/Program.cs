using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;

namespace MultiThreadedPermutations
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.Out.WriteLine("First argument should be the input set");
                return;
            }

            var options = args[0];

            var connectionString = @"Data Source=localhost;Initial Catalog=testing;User ID=sa;Password=yourStrong(!)Password";
            string tableName = $"matches_{options.Length}_teams";
            var cnn = new SqlConnection(connectionString);
            cnn.Open();

            ClearPersistence(cnn, tableName, options.Length);

            Console.Out.WriteLine("Cleared persistence!");


            var buffer = new List<string>();

            int maxPermutations = GetPossibleCombinations(options);
            int permutationCount = 0;

            int lastInsert = 0;

            Console.Out.WriteLine($"Generating {maxPermutations} permutations for: " + options);

            ForAllPermutation(options.ToCharArray(), (permutation, isFinished) =>
            {
                permutationCount++;
                lastInsert++;

                PrintProgress(permutationCount, maxPermutations);

                if (!isFinished)
                {
                    buffer.Add(string.Join("", permutation));
                }


                if (lastInsert > 80 || isFinished)
                {
                    PersistItems(cnn, tableName, options.Length, buffer);
                    buffer.Clear();
                    lastInsert = 0;
                }

                if (isFinished)
                {
                    Console.Out.WriteLine($"Finished from producer!, Created {permutationCount - 1} permutations");
                }

                return false;
            });
            
            cnn.Close();

            Console.Out.WriteLine("Finished all!");
        }

        private static void PersistItems(SqlConnection connection, string tableName, int amountOfTeams, List<string> buffer)
        {
            if (buffer.Count == 0) return;
            var parts = string.Join(",",Enumerable.Range(1, amountOfTeams).Select(teamId => $"part_{teamId}"));

            string values = string.Join(", ", buffer.Select(x => $"({string.Join(",", x.ToCharArray().Select(c => $"'{c}'"))})"));

            var sqlInsert = $"INSERT INTO {tableName}({parts}) VALUES {values}";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }
        
        private static void ClearPersistence(SqlConnection connection, string tableName, int amountOfTeams)
        {
            var columnDeclaration = string.Join(",",Enumerable.Range(1, amountOfTeams).Select(teamId => $"part_{teamId} char NOT NULL"));
            var columns = string.Join(",",Enumerable.Range(1, amountOfTeams).Select(teamId => $"part_{teamId}"));

            
            var sqlInsert = $"DROP TABLE IF EXISTS {tableName}; CREATE table {tableName} ({columnDeclaration}, CONSTRAINT pk_{tableName} PRIMARY KEY ({columns}));";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }

        private static void PrintProgress(int permutationCount, int maxPermutations)
        {
            var progress = permutationCount % (Math.Max(maxPermutations / 1000, 1));
            if (progress != 0) return;

            var percentage = (double) permutationCount / maxPermutations;
            Console.Out.WriteLine($"{Math.Round(percentage * 100)}% done ({permutationCount}/{maxPermutations})");
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
    }
}