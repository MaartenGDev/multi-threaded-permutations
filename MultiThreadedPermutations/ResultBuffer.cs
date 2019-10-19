using System;
using System.Text;
using RabbitMQ.Client;

namespace MultiThreadedPermutations
{
    public class ResultBuffer : IDisposable
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private string _queueName = "schedules";

        public ResultBuffer()
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            _connection = factory.CreateConnection("Producer");
            _channel = _connection.CreateModel();
            
            _channel.QueueDeclare(_queueName,
                false,
                false,
                false,
                null);

            _channel.QueuePurge(_queueName);
        }
        public void PublishResult(string result)
        {
            
                var body = Encoding.UTF8.GetBytes(result);

                _channel.BasicPublish("",
                    _queueName,
                    null,
                    body);
                
        }
        
        public void Dispose()
        {
            _channel.Close();
            _connection.Close();
        }
    }
}