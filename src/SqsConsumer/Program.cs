using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SqsConsumer
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

            var numberOfParallelTasks = 10;
            Console.WriteLine($"Spawning {numberOfParallelTasks} to receive and process the messages");
            var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;
            // var task = Task.Run(() => {
                Parallel.For(1, numberOfParallelTasks + 1, i => {
                    Console.WriteLine($"Task {i}: Reading messages from queue.");
                    while (true)
                    {
                        var receiveMessageRequest = new ReceiveMessageRequest
                        {
                            QueueUrl = queueUrl,
                            MaxNumberOfMessages = 1,
                            WaitTimeSeconds = 20
                        };
                        var receiveMessageResponse = sqsClient.ReceiveMessageAsync(receiveMessageRequest).Result;
                        Console.WriteLine($"Task {i}: Received {receiveMessageResponse.Messages.Count} messages.");

                        if (receiveMessageResponse.Messages.Count != 0)
                        {
                            var message = receiveMessageResponse.Messages[0];
                            Console.WriteLine($"Task {i}: Processing messages...");
                            Console.WriteLine($"Task {i}: Publisher said: '{message.Body}'");
                            Thread.Sleep(2000);

                            Console.WriteLine($"Task {i}: Deleting message from the queue...");
                            sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle);
                        }

                        if (cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                });
            // }, cancellationToken);

            // Console.WriteLine("Running... Press any key to exit");
            // Console.ReadLine();

            // Console.WriteLine("Canceling gracefully...");
            // cts.Cancel();
            // while (!task.IsCompleted)
            // {
            //     Thread.Sleep(1000);
            // }
        }
    }
}
