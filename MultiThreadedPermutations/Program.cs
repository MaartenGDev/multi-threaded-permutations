using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MultiThreadedPermutations
{
    class Program
    {
        public static volatile bool HasFinished = false;
        
        static void Main(string[] args)
        {
            ClearPersistence();

            var workerPool = new List<BufferPersistence>
            {
                new BufferPersistence()
            };
            
            var resultBuffer = new ResultBuffer();

            var buffer = "";
            var delimiter = "-";

            var startTime = DateTime.UtcNow;
            var breakDuration = TimeSpan.FromMilliseconds(1);

            int permutationCount = 0;
            ForAllPermutation("abcdefghij".ToCharArray(), (permutation, isFinished) =>
            {
                permutationCount++;
                buffer += (buffer.Length == 0 ? "" : delimiter) + string.Join(",", permutation);
                
                if (DateTime.UtcNow - startTime > breakDuration || isFinished)
                {
                    resultBuffer.PublishResult(isFinished ? "/" : buffer);
                    buffer = "";
                    startTime = DateTime.UtcNow;
                }

                if (isFinished)
                {
                    Console.Out.WriteLine($"Finished from producer!, Created {permutationCount -1} permutations");
                    resultBuffer.Dispose();
                }

                return false;
            });

            while (!HasFinished)
            {
            }


            foreach (var worker in workerPool)
            {
                worker.Dispose();
            }
            Console.Out.WriteLine("Finished all!" + HasFinished);
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