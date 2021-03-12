using System;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SqsPublisher
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Initializing Amazon SQS Client and getting queue URL...");
            var sqsClient = new AmazonSQSClient();
            var getQueueUrlRequest = new GetQueueUrlRequest
            {
                QueueName = "DemoMasterWorkerPattern",
                QueueOwnerAWSAccountId = "545983628851"
            };
            var getQueueUrlResponse = sqsClient.GetQueueUrlAsync(getQueueUrlRequest).Result;
            var queueUrl = getQueueUrlResponse.QueueUrl;
            Console.WriteLine($"Received Queue URL: {queueUrl}");

            while (true)
            {
                Console.Write("Type a message to be sent to the queue or type 'exit' to quit: ");
                var message = Console.ReadLine();
                if (string.Equals(message, "exit", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }

                Console.WriteLine($"Sending message with content '{message}' to the queue...");
                var sendMessageRequest = new SendMessageRequest();
                sendMessageRequest.QueueUrl = queueUrl;
                sendMessageRequest.MessageBody = message;
                var sendMessageResponse = sqsClient.SendMessageAsync(sendMessageRequest).Result;
                Console.WriteLine("Message has been sent.");
                Console.WriteLine($"HttpStatusCode: {sendMessageResponse.HttpStatusCode}");
            }
        }
    }
}
