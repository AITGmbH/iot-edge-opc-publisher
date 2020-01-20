using FluentAssertions;
using Microsoft.Azure.Devices.Client;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OpcPublisher;
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace OpcPublisher.Tests.AIT
{
    public class IoTCEventsProcessorTests
    {
        [Fact]
        public async Task MonitoredIoTCEventsProcessorAsync_ShouldSendSingleMessageForEvent()
        {
            Message receivedHubMessage = null;
            using (var cancellationTokenSource = Debugger.IsAttached ? new CancellationTokenSource() : new CancellationTokenSource(2000))
            {
                var hubClient = new Mock<IHubClient>();
                hubClient
                    .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                    .Returns(Task.CompletedTask)
                    .Callback((Message hubMessage) =>
                    {
                        receivedHubMessage = hubMessage;
                        cancellationTokenSource.Cancel();
                    });

                var eventProcessor = new IoTCEventsProcessor(Serilog.Log.Logger, hubClient.Object, 0, 512, 0, cancellationTokenSource.Token);

                _ = Task.Run(() => eventProcessor.MonitoredIoTCEventsProcessorAsync());

                MessageData message = new MessageData
                {
                    EventMessageData = new EventMessageData
                    {
                        Key = "Test"
                    }
                };
                eventProcessor.EnqueueEvent(message);

                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    await Task.Delay(1000);
                }
            }

            receivedHubMessage.BodyStream.Position = 0;
            var receivedMessage = Deserialize<JObject>(receivedHubMessage.BodyStream);
            receivedMessage.Properties().Should().ContainSingle(x => x.Name == "Test");
            receivedMessage.Properties().Should().ContainSingle(x => x.Name == "messageType");
            receivedMessage.Properties().Single(x => x.Name == "messageType").Value.Value<string>().Should().Be("event");
        }

        public static T Deserialize<T>(Stream s)
        {
            using (StreamReader reader = new StreamReader(s))
            using (JsonTextReader jsonReader = new JsonTextReader(reader))
            {
                JsonSerializer ser = new JsonSerializer();
                return ser.Deserialize<T>(jsonReader);
            }
        }

        [Fact]
        public void MonitoredIoTCEventsProcessorAsync_ShouldWaitForTimeout()
        {
            int timeoutSeconds = 10;
            double timeoutAcceptanceThresholdSeconds = 0.1;
            AutoResetEvent messageReceivedEvent = new AutoResetEvent(false);

            DateTime lastMessageSent = DateTime.UtcNow;
            TimeSpan timeBetweenMessages = new TimeSpan();
            long streamLength = new long();

            using (var cancellationTokenSource = new CancellationTokenSource())
            {
                var hubClient = new Mock<IHubClient>();
                hubClient
                    .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                    .Returns(Task.CompletedTask)
                    .Callback((Message hubMessage) =>
                    {
                        timeBetweenMessages = DateTime.UtcNow.Subtract(lastMessageSent);
                        streamLength = hubMessage.BodyStream.Length;
                        cancellationTokenSource.Cancel();
                        messageReceivedEvent.Set();
                    });

                MessageData message = new MessageData
                {
                    EventMessageData = new EventMessageData
                    {
                        Key = "Test"
                    }
                };
                int messageLength = 33;
                var eventProcessor = new IoTCEventsProcessor(Serilog.Log.Logger, hubClient.Object, 0, 1000000, timeoutSeconds, cancellationTokenSource.Token);

                lastMessageSent = DateTime.UtcNow;
                _ = Task.Run(() => eventProcessor.MonitoredIoTCEventsProcessorAsync());

                eventProcessor.EnqueueEvent(message);
                eventProcessor.EnqueueEvent(message);
                messageReceivedEvent.WaitOne((timeoutSeconds + 1) * 1000);

                timeBetweenMessages.TotalMilliseconds.Should().BeApproximately(timeoutSeconds * 1000, timeoutAcceptanceThresholdSeconds * 1000);
                streamLength.Should().Be((messageLength + 1) * 2 + 1);
            }
        }

        [Theory]
        [InlineData(999)]
        [InlineData(1000)]
        [InlineData(400)]
        public async Task MonitoredIoTCEventsProcessorAsync_ShouldWaitForFilledBuffer(uint buffersize)
        {
            await Task.Delay(1000); //TODO: Why does the test need to wait? Seems to have depency on other tests

            using (var cancellationTokenSource = new CancellationTokenSource())
            {
                long streamLength = 0;
                var hubClient = new Mock<IHubClient>();
                ManualResetEvent messageReceivedEvent = new ManualResetEvent(false);

                hubClient
                    .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                    .Returns(Task.CompletedTask)
                    .Callback((Message hubMessage) =>
                    {
                        streamLength = hubMessage.BodyStream.Length;
                        cancellationTokenSource.Cancel();
                        messageReceivedEvent.Set();
                    });

                MessageData message = new MessageData
                {
                    EventMessageData = new EventMessageData
                    {
                        Key = "Test"
                    }
                };

                // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
                Message tempMsg = new Message();
                int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString(CultureInfo.InvariantCulture).Length;
                int applicationPropertyLength = Encoding.UTF8.GetByteCount($"iothub-content-type=application/opcua+uajson") + Encoding.UTF8.GetByteCount($"iothub-content-encoding=UTF-8");
                int jsonSquareBracketLength = 2;
                int messageLength = 33;
                int bufferSizeWithoutOverhead = (int)buffersize - systemPropertyLength - applicationPropertyLength - jsonSquareBracketLength;

                //need to reduce buffersize by one for the '[' at the beginning of the message
                //need to add 1 to the message length for the ',' that is added after every enqueued Event
                //messagesToSend should be 1 higher then the messages received because the last one sent is needed to trigger the buffer overflow
                int messagesToSend = (int)((bufferSizeWithoutOverhead - 1) / (messageLength + 1)) + 1;

                var eventProcessor = new IoTCEventsProcessor(Serilog.Log.Logger, hubClient.Object, buffersize, buffersize, 0, cancellationTokenSource.Token);
                _ = Task.Run(() => eventProcessor.MonitoredIoTCEventsProcessorAsync());

                for (int messagesSent = 0; messagesSent < messagesToSend; messagesSent++)
                {
                    //Check if no message was received before sending the last item.
                    messageReceivedEvent.WaitOne(0).Should().BeFalse();
                    eventProcessor.EnqueueEvent(message);
                }

                //Wait a maximum of 2s for the message to arrive.
                messageReceivedEvent.WaitOne(2000).Should().BeTrue();

                //streamLength Calculation:
                //for every enqueued event, except the last one, there is one "," added -> messageLength+1
                //and the received messages begins with '[' and ends with ']' -> +2 but -1 for the missing ',' of the last Event so a total of +1
                streamLength.Should().Be((messageLength + 1) * (messagesToSend - 1) + 1);
            }
        }
    }
}
