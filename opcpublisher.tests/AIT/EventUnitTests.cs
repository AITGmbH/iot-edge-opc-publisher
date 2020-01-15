using Newtonsoft.Json;
using OpcPublisher;
using OpcPublisher.AIT;
using System.Collections.Generic;
using Xunit;

namespace OpcPublisher.Tests.AIT
{
    using static OpcPublisher.Program;
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Xunit.Abstractions;

    [Collection("Need PLC and publisher config")]
    public sealed class EventConfigurationViaFileUnitTests : IDisposable
    {
        public EventConfigurationViaFileUnitTests(ITestOutputHelper output)
        {
            // xunit output
            _output = output;

            // init configuration objects
            TelemetryConfiguration = PublisherTelemetryConfiguration.Instance;
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                // dispose managed resources
            }
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            // do cleanup
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Test reading different configuration files and creating the correct internal data structures.
        /// </summary>
        [Theory]
        [Trait("Configuration", "File")]
        [Trait("ConfigurationSetting", "SimpleEventConfiguration")]
        [MemberData(nameof(PnPlcEventSimple))]
        public async Task CreateOpcEventPublishingData(string testFilename, int configuredSessions,
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

                Assert.True(_configurationFileEntries[0].OpcEvents.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].EventNotifierId == "i=2253");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].Key == "SimpleEventServerEvents");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses.Count == 4);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].TypeId == "i=2041");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Property);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].BrowsePaths[0] == "EventId");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].TypeId == "i=2041");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].BrowsePaths[0] == "Message");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].TypeId == "nsu=http://opcfoundation.org/Quickstarts/SimpleEvents;i=235");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].BrowsePaths[0] == "/2:CycleId");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].TypeId == "nsu=http://opcfoundation.org/Quickstarts/SimpleEvents;i=235");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].BrowsePaths[0] == "/2:CurrentStep");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operator == "OfType");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands[0].Literal == "ns=2;i=235");
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
            }
        }

        /// <summary>
        /// Test reading different configuration files and creating the correct internal data structures.
        /// </summary>
        [Theory]
        [Trait("Configuration", "File")]
        [Trait("ConfigurationSetting", "SimpleEventConfiguration")]
        [MemberData(nameof(PnPlcEventSimple_EventAsEvent))]
        public async void CreateOpcEventPublishingData_EventAsIotCentralEvent(string testFilename, int configuredSessions,
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

                Assert.True(_configurationFileEntries[0].OpcNodes.Count == 3);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcSamplingInterval == 2000);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcPublishingInterval == 5000);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].IotCentralItemPublishMode == IotCentralItemPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcNodes[1].IotCentralItemPublishMode == IotCentralItemPublishMode.Property);
                Assert.True(_configurationFileEntries[0].OpcNodes[2].IotCentralItemPublishMode == IotCentralItemPublishMode.Setting);


                Assert.True(_configurationFileEntries[0].OpcEvents.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].EventNotifierId == "i=2253");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].Key == "SimpleEventServerEvents");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].IotCentralEventPublishMode == IotCentralEventPublishMode.Event);

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses.Count == 4);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].TypeId == "i=2041");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].BrowsePaths[0] == "EventId");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].TypeId == "i=2041");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].BrowsePaths[0] == "Message");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].TypeId == "nsu=http://opcfoundation.org/Quickstarts/SimpleEvents;i=235");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].BrowsePaths[0] == "/2:CycleId");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].TypeId == "nsu=http://opcfoundation.org/Quickstarts/SimpleEvents;i=235");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].BrowsePaths[0] == "/2:CurrentStep");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operator == "OfType");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands[0].Literal == "ns=2;i=235");
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
            }
        }

        /// <summary>
        /// Test reading different configuration files and creating the correct internal data structures.
        /// </summary>
        [Theory]
        [Trait("Configuration", "File")]
        [Trait("ConfigurationSetting", "SimpleEventConfiguration")]
        [MemberData(nameof(PnPlcEventSimple_EventAsEvent_Invalid))]
        public async void CreateOpcEventPublishingData_EventAsIotCentralEvent_Invalid(string testFilename, int configuredSessions,
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

                Assert.True(_configurationFileEntries[0].OpcNodes.Count == 3);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcSamplingInterval == 2000);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcPublishingInterval == 5000);
                Assert.True(_configurationFileEntries[0].OpcNodes[0].IotCentralItemPublishMode == IotCentralItemPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcNodes[1].IotCentralItemPublishMode == IotCentralItemPublishMode.Property);
                Assert.True(_configurationFileEntries[0].OpcNodes[2].IotCentralItemPublishMode == IotCentralItemPublishMode.Setting);


                Assert.True(_configurationFileEntries[0].OpcEvents.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].EventNotifierId == "i=2253");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].Key == "SimpleEventServerEvents");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].IotCentralEventPublishMode == IotCentralEventPublishMode.Event);

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses.Count == 4);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].TypeId == "i=2041");
                Assert.False(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[0].BrowsePaths[0] == "EventId");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].TypeId == "i=2041");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[1].BrowsePaths[0] == "Message");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].TypeId == "nsu=http://opcfoundation.org/Quickstarts/SimpleEvents;i=235");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[2].BrowsePaths[0] == "/2:CycleId");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].TypeId == "nsu=http://opcfoundation.org/Quickstarts/SimpleEvents;i=235");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].IotCentralEventPublishMode ==
                            IotCentralEventPublishMode.Default);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses[3].BrowsePaths[0] == "/2:CurrentStep");

                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operator == "OfType");
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause[0].Operands[0].Literal == "ns=2;i=235");
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
            }
        }

        /// <summary>
        /// Filename, 
        /// # of configured sessions,
        /// # of configured subscriptions
        /// # of configured monitored item
        /// # of configured event items
        /// </summary>
        public static IEnumerable<object[]> PnPlcEventSimple =>
            new List<object[]>
            {
                new object[]
                {
                    "pn_plc_simple.json", 1, 2, 3, 1
                },
                new object[]
                {
                    "pn_plc_simple_eventOnly.json", 1, 1, 0, 1
                }
            };

        public static IEnumerable<object[]> PnPlcEventSimple_EventAsEvent =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple_EventAsEvent.json"),
                    // # of configured sessions
                    1,
                    // # of configured subscriptions
                    2,
                    // # of configured monitored items
                    3,
                    // # of configured event items
                    1
                }
            };

        public static IEnumerable<object[]> PnPlcEventSimple_EventAsEvent_Invalid =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple_EventAsEventInvalid.json"),
                    // # of configured sessions
                    1,
                    // # of configured subscriptions
                    2,
                    // # of configured monitored items
                    3,
                    // # of configured event items
                    1
                }
            };


        private readonly ITestOutputHelper _output;
        private static List<PublisherConfigurationFileEntryLegacyModel> _configurationFileEntries;
    }
}
