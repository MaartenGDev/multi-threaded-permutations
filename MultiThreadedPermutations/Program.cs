using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;

namespace MultiThreadedPermutations
{
    class Program
    {
        private int generatedCombinations = 0;
        private int permutationCountLastProgress = 0;
        private DateTime lastProgressUpdate = DateTime.Now;

        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.Out.WriteLine("First argument should be the input set");
                return;
            }

            new Program().Work(args[0]);
        }

        private void Work(string options)
        {
            var amountOfTeams = options.Length;

            var connectionString = @"Data Source=localhost;Initial Catalog=testing;User ID=sa;Password=yourStrong(!)Password";
            
            string tableName = $"matches_{options.Length}_teams";
            var cnn = new SqlConnection(connectionString);
            cnn.Open();

            ClearPersistence(cnn, tableName, options.Length);

            Console.Out.WriteLine("Cleared persistence!");


            var table = new DataTable();

            foreach (var index in Enumerable.Range(1, amountOfTeams))
            {
                table.Columns.Add("part_" + index, typeof(char));
            }


            var maxPermutations = GetPossibleCombinations(options);

            int lastInsert = 0;

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            Console.Out.WriteLine($"Generating {maxPermutations} permutations for: " + options);


            stopWatch.Start();
            var bulk = new SqlBulkCopy(cnn)
                {DestinationTableName = $"matches_{amountOfTeams}_teams", BulkCopyTimeout = 0};

            ForAllPermutation(options.ToCharArray(), (permutation, isFinished) =>
            {
                generatedCombinations++;
                lastInsert++;

                PrintProgress(generatedCombinations, maxPermutations);

                if (!isFinished)
                {
                    DataRow row = table.NewRow();

                    foreach (var index in Enumerable.Range(1, amountOfTeams))
                    {
                        row["part_" + index] = permutation[index - 1];
                    }

                    table.Rows.Add(row);
                }


                if (lastInsert > 1000000 || isFinished)
                {
                    bulk.WriteToServer(table);
                    table.Clear();
                    lastInsert = 0;
                }

                if (isFinished)
                {
                    Console.Out.WriteLine($"Finished from producer!, Created {generatedCombinations - 1} permutations");
                }

                return false;
            });


            cnn.Close();

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = $"{ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}.{ts.Milliseconds / 10:00}";

            Console.Out.WriteLine($"Finished all in {elapsedTime}!");
        }

        private void ClearPersistence(SqlConnection connection, string tableName, int amountOfTeams)
        {
            var columnDeclaration = string.Join(",",
                Enumerable.Range(1, amountOfTeams).Select(teamId => $"part_{teamId} char NOT NULL"));
            var columns = string.Join(",", Enumerable.Range(1, amountOfTeams).Select(teamId => $"part_{teamId}"));


            var sqlInsert =
                $"DROP TABLE IF EXISTS {tableName}; CREATE table {tableName} ({columnDeclaration}, CONSTRAINT pk_{tableName} PRIMARY KEY ({columns}));";
            var command = new SqlCommand(sqlInsert, connection);

            command.ExecuteNonQuery();
        }

        private void PrintProgress(int permutationCount, int maxPermutations)
        {
            var progress = permutationCount % (Math.Max(maxPermutations / 1000, 1));
            if (progress != 0) return;

            var processedItems = permutationCount - permutationCountLastProgress;
            var elapsedTime = DateTime.Now - lastProgressUpdate;

            var timeLeft = ((maxPermutations - (double) permutationCount) / processedItems) * elapsedTime;

            var relativeTimeLeft = timeLeft.TotalMinutes < 1 ? $"{timeLeft.TotalSeconds:0}s" :
                timeLeft.TotalMinutes > 59 ? $"{timeLeft.Hours:0}h" : $"{timeLeft.TotalMinutes:0}m";


            var percentage = (double) permutationCount / maxPermutations;
            Console.Out.WriteLine(
                $"{DateTime.Now} {Math.Round(percentage * 100)}% done ({permutationCount}/{maxPermutations}) {relativeTimeLeft} left");

            permutationCountLastProgress = permutationCount;
            lastProgressUpdate = DateTime.Now;
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