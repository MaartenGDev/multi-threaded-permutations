using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MultiThreadedPermutations
{
    public class BufferPersistence : IDisposable
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private string _queueName = "schedules";

        public BufferPersistence()
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            _connection = factory.CreateConnection("Consumer");
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(_queueName, false, false, false, null);
            
            Start();
        }

        public void Start()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("planner");
            var collection = database.GetCollection<BsonDocument>("schedules");

            var consumer = new EventingBasicConsumer(_channel);

            bool hasReachedEnd = false;
            
            consumer.Received += (ch, ea) =>
            {
            
                
                var body = ea.Body;

                var teamCombinations = Encoding.UTF8.GetString(body);
                
                if (hasReachedEnd)
                {
                    Console.Out.WriteLine($"got message: {teamCombinations} while has processed last item!");
                };
                
                var items = new List<BsonDocument>(); 
                    
                foreach (var teamCombination in teamCombinations.Split("-"))
                {
                    if (teamCombination == "/")
                    {
                        hasReachedEnd = true;
                    }
                    else
                    {
                        items.Add(new BsonDocument {{"teamCombination", teamCombination}});
                    }
                }

                if (items.Count > 0)
                {
                    collection.InsertMany(items);
                }

                if (hasReachedEnd)
                {
                    Program.HasFinished = true;
                    Console.Out.WriteLine("Finished! from consumer");
                }
            };

            _channel.BasicConsume(_queueName, true, consumer);
        }

        
        public void Dispose()
        {
            _channel.Close();
            _connection.Close();
        }
    }
}