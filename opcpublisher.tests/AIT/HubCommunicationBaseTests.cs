using FluentAssertions;
using Microsoft.Azure.Devices.Client;
using Moq;
using Newtonsoft.Json;
using OpcPublisher.AIT;
using Serilog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static OpcPublisher.Program;


namespace OpcPublisher.Tests.AIT
{
    [Collection("Need PLC and publisher config")]
    public class HubCommunicationBaseTests
    {
        private readonly ITestOutputHelper _output;

        public HubCommunicationBaseTests(ITestOutputHelper output)
        {
            _output = output;
            Logger = new LoggerConfiguration().CreateLogger();
        }

        [Fact]
        public async Task MonitoredItemsProcessorAsync_ShouldWaitForTimeout()
        {
            int timeoutSeconds = 10;
            double timeoutAcceptanceThresholdSeconds = 0.1;
            AutoResetEvent messageReceivedEvent = new AutoResetEvent(false);

            var itemProcessor = new HubCommunicationBase();

            itemProcessor.SendIntervalSeconds = timeoutSeconds;
            itemProcessor.HubMessageSize = 100000;

            TelemetryConfiguration = new PublisherTelemetryConfiguration();

            DateTime lastMessageSent = DateTime.UtcNow;
            TimeSpan timeBetweenMessages = new TimeSpan();
            long streamLength = 0;
            int messagesReceived = 0;

            var hubClient = new Mock<IHubClient>();
            hubClient
                .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                .Returns(Task.CompletedTask)
                .Callback((Message hubMessage) =>
                {
                    timeBetweenMessages = DateTime.UtcNow.Subtract(lastMessageSent);
                    lastMessageSent = DateTime.UtcNow;
                    streamLength = hubMessage.BodyStream.Length;
                    messageReceivedEvent.Set();
                    messagesReceived++;

                });

            MessageData message = new MessageData
            {
                DataChangeMessageData = new DataChangeMessageData
                {
                    DisplayName = "Test",
                    EndpointUrl = "abc123",
                    EndpointId = "qwe"
                }
            };

            int messageLength = 50;

            _ = await Task.Factory.StartNew(() => itemProcessor.InitHubCommunicationAsync(hubClient.Object, false, true), TaskCreationOptions.LongRunning);

            itemProcessor.Enqueue(message);
            itemProcessor.Enqueue(message);
            messageReceivedEvent.WaitOne((timeoutSeconds + 5) * 1000).Should().BeTrue();

            itemProcessor.Enqueue(message);
            itemProcessor.Enqueue(message);
            messageReceivedEvent.WaitOne((timeoutSeconds + 5) * 1000).Should().BeTrue();

            timeBetweenMessages.TotalMilliseconds.Should().BeApproximately(timeoutSeconds * 1000, timeoutAcceptanceThresholdSeconds * 1000);

            //2 messages are sent
            //for every message, except the last one, there is one "," added
            //and the received messages begins with '[' and ends with ']'W
            streamLength.Should().Be((messageLength + 1) * 2 + 1);
        }

        [Fact]
        public async Task MonitoredItemsProcessorAsync_ShouldWaitFilledBuffer()
        {
            uint buffersize = 1000;
            ManualResetEvent messageReceivedEvent = new ManualResetEvent(false);

            var itemProcessor = new HubCommunicationBase();
            TelemetryConfiguration = new PublisherTelemetryConfiguration();
            itemProcessor.SendIntervalSeconds = 0;
            itemProcessor.HubMessageSize = buffersize;

            long streamLength = 0;

            var hubClient = new Mock<IHubClient>();
            hubClient
                .Setup(client => client.SendEventAsync(It.IsAny<Message>()))
                .Returns(Task.CompletedTask)
                .Callback((Message hubMessage) =>
                {
                    streamLength = hubMessage.BodyStream.Length;
                    messageReceivedEvent.Set();
                });

            MessageData message = new MessageData
            {
                DataChangeMessageData = new DataChangeMessageData
                {
                    DisplayName = "Test",
                    EndpointUrl = "abc123",
                    EndpointId = "qwe"
                }
            };

            int messageLength = 50;

            // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
            Message tempMsg = new Message();
            int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString(CultureInfo.InvariantCulture).Length;
            int applicationPropertyLength = Encoding.UTF8.GetByteCount($"iothub-content-type=application/opcua+uajson") + Encoding.UTF8.GetByteCount($"iothub-content-encoding=UTF-8");
            int jsonSquareBracketLength = 2;

            int bufferSizeWithoutOverhead = (int)buffersize - systemPropertyLength - applicationPropertyLength - jsonSquareBracketLength;

            //need to reduce buffersize by one for the '[' at the beginning of the message
            //need to add 1 to the message length for the ',' that is added after every enqueued Item
            //messagesToSend should be 1 higher then the messages received because the last one sent is needed to trigger the buffer overflow
            int messagesToSend = (int)((bufferSizeWithoutOverhead - 1) / (messageLength + 1)) + 1;

            _ = await Task.Run(() => itemProcessor.InitHubCommunicationAsync(hubClient.Object, false, true));

            for (int messagesSent = 0; messagesSent < messagesToSend; messagesSent++)
            {
                //Check if no message was received before sending the last item.
                messageReceivedEvent.WaitOne(0).Should().BeFalse();
                itemProcessor.Enqueue(message);
            }

            //Wait a maximum of 2s for the message to arrive.
            messageReceivedEvent.WaitOne(2000).Should().BeTrue();

            //for every message, except the last one, there is one "," added
            //and the received messages begins with '[' and ends with ']'
            streamLength.Should().Be((messageLength + 1) * (messagesToSend - 1) + 1);

        }

        [Theory]
        [MemberData(nameof(PnPlcSimpleWithKeyAndModifications))]
        public async Task HandleSaveOpcPublishedConfigurationAsJson_FormatIsRight_ShouldReinitializeNodeConfiguration(
            string testFilename,
            string testFileNameWithModifiedKey,
            int configuredSessions,
            int configuredSubscriptions,
            int configuredMonitoredItems,
            int configuredMonitoredEvents)
        {
            OpcNodeOnEndpointModel GetFirstNode()
            {
                var endpointId = NodeConfiguration.OpcSessions[0].EndpointId;
                var nodes = NodeConfiguration.GetPublisherConfigurationFileEntries(endpointId, true, out uint version);
                return nodes[0].OpcNodes[0];
            }

            using (new ExecutionContext(testFilename))
            {
                var hubCommunicationBase = new HubCommunicationBase();
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems, configuredMonitoredEvents);

                // Get node before saving new file.
                var opcNodeBeforeSave = GetFirstNode();

                // ------- Act -----

                // load different file with key change.
                var modifiedFilePath = CopyFileToTempData(testFileNameWithModifiedKey);
                // Read the file data as bytes
                var text = await File.ReadAllTextAsync(modifiedFilePath);
                var methodRequestModel = new HandleSaveOpcPublishedConfigurationMethodRequestModel(text);
                var json = JsonConvert.SerializeObject(methodRequestModel);
                var bytes = Encoding.UTF8.GetBytes(json);
                var request = new MethodRequest("SaveOpcPublishedConfigurationAsJson", bytes);
                // Feed the json string to the communication base
                var success = await hubCommunicationBase.HandleSaveOpcPublishedConfigurationAsJson(request, new object());

                // ------- Assert ------

                // The preconditions should not change, since only the key of the node is changed.
                AssertPreconditions(configuredSessions, configuredSubscriptions, configuredMonitoredItems, configuredMonitoredEvents);
                Assert.True(success.Status == (int)HttpStatusCode.OK);

                var opcNodeAfterSave = GetFirstNode();
                Assert.Equal(opcNodeBeforeSave.Id, opcNodeBeforeSave.Id);
                Assert.Equal("i=2267", opcNodeBeforeSave.Key);
                Assert.Equal("i=2267-test", opcNodeAfterSave.Key);
            }
        }

        // name of published nodes configuration file
        // name of configuration file with modified keys
        // # of configured sessions
        // # of configured subscriptions
        // # of configured monitored items
        // # of configured monitored events
        public static IEnumerable<object[]> PnPlcSimpleWithKeyAndModifications =>
            new List<object[]>
            {
                new object[]
                {
                    "pn_plc_simple_with_key.json", "pn_plc_simple_with_key_2.json", 1, 2, 1, 1
                },
            };

        private void AssertPreconditions(int configuredSessions, int configuredSubscriptions, int configuredMonitoredItems, int configuredMonitoredEvents)
        {
            _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
            _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
            _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
            _output.WriteLine($"events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");

            Assert.True(NodeConfiguration.OpcSessions.Count == configuredSessions, "wrong # of sessions");
            Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == configuredSessions, "wrong # of sessions");
            Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == configuredSubscriptions, "wrong # of subscriptions");
            Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == configuredMonitoredItems, "wrong # of monitored items");
            Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == configuredMonitoredEvents, "wrong # of monitored events");
        }

        private static string CopyFileToTempData(string testFilename)
        {
            var methodName = UnitTestHelper.GetMethodName();
            var fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/ait/{testFilename}";
            var fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
            if (File.Exists(fqTempFilename))
            {
                File.Delete(fqTempFilename);
            }
            File.Copy(fqTestFilename, fqTempFilename);
            return fqTempFilename;
        }

        private class ExecutionContext : IDisposable
        {
            public ExecutionContext(string testFilename)
            {
                string fqTempFilename = CopyFileToTempData(testFilename);
                PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
                Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                NodeConfiguration = new PublisherNodeConfiguration(); // PublisherNodeConfiguration.Instance;
            }

            public void Dispose()
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                IotHubCommunication.IotHubClient = null;
                Hub?.Dispose();
                Hub = null;
            }
        }
    }
}
