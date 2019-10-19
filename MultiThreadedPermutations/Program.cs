using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MultiThreadedPermutations
{
    class Program
    {
        static void Main(string[] args)
        {
            ClearPersistence();

            if (args.Length == 0)
            {
                Console.Out.WriteLine("First argument should be the input set");
                return;
            }
            var options = args[0];
            
            Console.Out.WriteLine("Calculating permutations for: " + options);

            var client = new MongoClient();
            var database = client.GetDatabase("planner");
            var collection = database.GetCollection<BsonDocument>(options +"_permutations");
            
            var buffer = new List<BsonDocument>();

            var startTime = DateTime.UtcNow;
            var breakDuration = TimeSpan.FromMilliseconds(200);

            int permutationCount = 0;
            
            ForAllPermutation(options.ToCharArray(), (permutation, isFinished) =>
            {
                permutationCount++;
                
                if (!isFinished)
                {
                    buffer.Add(new BsonDocument {{"teamCombination", string.Join("", permutation)}});
                }
                
                if (DateTime.UtcNow - startTime > breakDuration || isFinished)
                {
                    if (buffer.Count > 0)
                    {
                        collection.InsertMany(buffer);
                        buffer.Clear();   
                    }
                    startTime = DateTime.UtcNow;
                }

                if (isFinished)
                {
                    Console.Out.WriteLine($"Finished from producer!, Created {permutationCount -1} permutations");
                }

                return false;
            });

            Console.Out.WriteLine("Finished all!");
        }

        private static void ClearPersistence()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("planner");
            database.DropCollection("schedules");
        }

        public static bool ForAllPermutation<T>(T[] items, Func<T[],bool, bool> funcExecuteAndTellIfShouldStop)
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

            funcExecuteAndTellIfShouldStop(new T[]{}, true);

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