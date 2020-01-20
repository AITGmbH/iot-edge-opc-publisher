using Newtonsoft.Json;
using OpcPublisher;
using OpcPublisher.AIT;
using static OpcPublisher.Program;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using Xunit.Abstractions;

namespace OpcPublisher.Tests.AIT
{
    [Collection("Need PLC and publisher config")]
    public sealed class ConfigurationViaFileUnitTestExtended
    {
        public ConfigurationViaFileUnitTestExtended(ITestOutputHelper output, PlcOpcUaServerFixture server)
        {
            // xunit output
            _output = output;
            _server = server;

            // init configuration objects
            TelemetryConfiguration = PublisherTelemetryConfiguration.Instance;
        }

        /// <summary>
        /// Test reading different configuration files and creating the correct internal data structures.
        /// </summary>
        [Theory]
        [Trait("Configuration", "File")]
        [Trait("ConfigurationSetting", "SimpleExtendedConfiguration")]
        [MemberData(nameof(PnPlcEventSimple))]
        public async void CreateSimpleExtendedConfiguration(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems, int configuredMonitoredEvents)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/ait/{testFilename}";
            fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
            if (File.Exists(fqTempFilename))
            {
                File.Delete(fqTempFilename);
            }
            File.Copy(fqTestFilename, fqTempFilename);
            PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
            _output.WriteLine($"now testing: {PublisherNodeConfiguration.PublisherNodeConfigurationFilename}");
            Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

            UnitTestHelper.SetPublisherDefaults();

            try
            {
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == configuredSessions, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == configuredSessions, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == configuredSubscriptions, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == configuredMonitoredEvents, "wrong # of events");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == configuredMonitoredItems, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, " +
                                  $"toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}, events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, " +
                                  $"monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}");

                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries[0].OpcNodes[0].IotCentralItemPublishMode == IotCentralItemPublishMode.Setting);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].EventNotifierId != null);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].Key != null);

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].BrowsePaths[0] == "EventId");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].BrowsePaths[0] == "Message");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].BrowsePaths[0] == "/2:CycleId");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].BrowsePaths[0] == "/2:CurrentStep");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operator == "OfType");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands[0].Literal == "ns=2;i=235");
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
            }
        }

        [Theory]
        [Trait("Configuration", "File")]
        [Trait("ConfigurationSetting", "SimpleExtendedConfiguration")]
        [MemberData(nameof(PnPlcEventSimple_IoTCEvent))]
        public async void CreateSimpleExtendedConfiguration_IoTCEvent(string testFilename, int configuredSessions,
            int configuredSubscriptions, int configuredMonitoredItems, int configuredMonitoredEvents)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/event/{testFilename}";
            fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
            if (File.Exists(fqTempFilename))
            {
                File.Delete(fqTempFilename);
            }
            File.Copy(fqTestFilename, fqTempFilename);
            PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
            _output.WriteLine($"now testing: {PublisherNodeConfiguration.PublisherNodeConfigurationFilename}");
            Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

            UnitTestHelper.SetPublisherDefaults();

            try
            {
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == configuredSessions, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == configuredSessions, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == configuredSubscriptions, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == configuredMonitoredEvents, "wrong # of events");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == configuredMonitoredItems, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, " +
                                  $"toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}, events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, " +
                                  $"monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}");

                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries[0].OpcNodes[0].IotCentralItemPublishMode == IotCentralItemPublishMode.Setting);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].EventNotifierId != null);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].Key != null);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].IotCentralEventPublishMode == IotCentralEventPublishMode.Event);

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].BrowsePaths[0] == "EventId");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].BrowsePaths[0] == "Message");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].BrowsePaths[0] == "/2:CycleId");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].BrowsePaths[0] == "/2:CurrentStep");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operator == "OfType");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands[0].Literal == "ns=2;i=235");
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
            }
        }

        public static IEnumerable<object[]> PnPlcEventSimple =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple.json"),
                    // # of configured sessions
                    1,
                    // # of configured subscriptions
                    2,
                    // # of configured monitored items
                    1,
                    // # of configured event items
                    1
                }
            };

        public static IEnumerable<object[]> PnPlcEventSimple_IoTCEvent =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple_iotcevent.json"),
                    // # of configured sessions
                    1,
                    // # of configured subscriptions
                    2,
                    // # of configured monitored items
                    1,
                    // # of configured event items
                    1
                }
            };


        private readonly ITestOutputHelper _output;
        private readonly PlcOpcUaServerFixture _server;
        private static List<PublisherConfigurationFileEntryLegacyModel> _configurationFileEntries;
    }
}
