using System.Collections.Generic;
using System.Net;
using OpcPublisher.AIT;
using Xunit;

namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Newtonsoft.Json;
    using OpcPublisher.Crypto;
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Xunit.Abstractions;
    using static OpcApplicationConfiguration;
    using static Program;

    [Collection("Need PLC and publisher config")]
    public sealed class ConfigurationViaIotMethodUnitTests
    {
        public ConfigurationViaIotMethodUnitTests(ITestOutputHelper output, PlcOpcUaServerFixture server)
        {
            // xunit output
            _output = output;
            _server = server;

            // init configuration objects
            TelemetryConfiguration = PublisherTelemetryConfiguration.Instance;

            // These tests required the dummy edge device registered in the dev iot hub...
            IotHubCommunication.DeviceConnectionString = Environment.GetEnvironmentVariable("DeviceConnectionString") ?? "HostName=<hostname>;DeviceId=UnitTestDummyDevice_DoNotDelete;SharedAccessKey=<sharedAccessKey>";
        }

        /// <summary>
        /// Test that configured OPC UA Events can be loaded via IotHubDirectMethodCall.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcDeleteConfiguredEndpoint))]
        public async void DeleteConfiguredEndpoint(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/ait/{payloadFilename}";
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
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 2, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 2, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 4, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 2, "wrong # of monitored items");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == 2, "wrong # of monitored events");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                _output.WriteLine($"events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcEventMonitoredItemsToRemove}");

                MethodRequest methodRequest = new MethodRequest("DeleteConfiguredEndpoint", File.ReadAllBytes(fqPayloadFilename));
                var methodResponse = await Hub.HandleDeleteConfiguredEndpointMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();

                Assert.True(methodResponse.Status == (int)HttpStatusCode.OK ||
                            methodResponse.Status == (int)HttpStatusCode.Accepted);

                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 2, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == 1, "wrong # of monitored events");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                _output.WriteLine($"events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcEventMonitoredItemsToRemove}");
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OPC UA Events is persisted when configured with direct method call.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcEventPayloadPublishing))]
        public async void OpcEventPublishWithValidPayload(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/event/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/event/{testFilename}";
            fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
            if (File.Exists(fqTempFilename))
            {
                File.Delete(fqTempFilename);
            }
            // source, destination
            // source: testdata/event/pn_plc_simple.json
            // destination: tempdata/OpcEventPublishWithValidPayload_pn_plc_simple.json
            File.Copy(fqTestFilename, fqTempFilename);
            PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
            _output.WriteLine($"now testing: {PublisherNodeConfiguration.PublisherNodeConfigurationFilename}");
            Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

            UnitTestHelper.SetPublisherDefaults();

            try
            {
                Hub = new IotHubCommunication();
                CryptoProvider = new StandaloneCryptoProvider();
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 2, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 3, "wrong # of monitored items");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == 1, "wrong # of monitored events");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                _output.WriteLine($"events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcEventMonitoredItemsToRemove}");
                
                MethodRequest methodRequest = new MethodRequest("PublishEvents", File.ReadAllBytes(fqPayloadFilename));
                var methodResponse = await Hub.HandlePublishEventsMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();

                Assert.True(methodResponse.Status == (int)HttpStatusCode.OK ||
                            methodResponse.Status == (int)HttpStatusCode.Accepted);

                //Ensure that NodeConfiguration is updated after remote configuration method call
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcEventSubscriptions.Count == 2);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 3, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 3, "wrong # of monitored items");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == 2, "wrong # of monitored events");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");

                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 1);

                //Check configuration file entries after method call
                Assert.True(_configurationFileEntries[0].OpcEvents.Count == 2);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].SelectClauses.Count == 4);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].WhereClause.Count == 1);
                Assert.True(_configurationFileEntries[0].OpcEvents[0].IotCentralEventPublishMode == IotCentralEventPublishMode.Property);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OPC UA Events is not persisted when configured with invalid data of direct method call.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcEventAndEmptyPayloadPublishing))]
        public async void OpcEventPublishWithInvalidPayload(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/event/{payloadFilename}";
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
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 2, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 3, "wrong # of monitored items");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == 1, "wrong # of monitored events");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                _output.WriteLine($"events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcEventMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 1);
                MethodRequest methodRequest = new MethodRequest("PublishEvents", File.ReadAllBytes(fqPayloadFilename));
                var response = await Hub.HandlePublishEventsMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(response.Status == (int)HttpStatusCode.InternalServerError);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that configured OPC UA Events can be loaded via IotHubDirectMethodCall.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcGetConfiguredEvents))]
        public async void GetConfiguredOpcEvents(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/event/{payloadFilename}";
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
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 2, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 3, "wrong # of monitored items");
                Assert.True(NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured == 1, "wrong # of monitored events");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                _output.WriteLine($"events configured {NodeConfiguration.NumberOfOpcEventMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcEventMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcEventMonitoredItemsToRemove}");

                MethodRequest methodRequest = new MethodRequest("GetConfiguredEventNodesOnEndpoint", File.ReadAllBytes(fqPayloadFilename));
                var methodResponse = await Hub.HandleGetConfiguredEventsOnEndpointMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();

                Assert.True(methodResponse.Status == (int)HttpStatusCode.OK ||
                            methodResponse.Status == (int)HttpStatusCode.Accepted);

                var configuredEventNodesOnEndpoint =
                    JsonConvert.DeserializeObject<GetConfiguredEventNodesOnEndpointMethodResponseModel>(methodResponse
                        .ResultAsJson);

                Assert.True(configuredEventNodesOnEndpoint.EndpointId == "eeea7191-bb7c-49e8-8362-29236a4f8468");
                Assert.True(configuredEventNodesOnEndpoint.EventNodes.Count == 1);
                Assert.True(configuredEventNodesOnEndpoint.EventNodes[0].SelectClauses.Count == 4);
                Assert.True(configuredEventNodesOnEndpoint.EventNodes[0].WhereClause.Count == 1);
                Assert.True(configuredEventNodesOnEndpoint.EventNodes[0].IotCentralEventPublishMode == IotCentralEventPublishMode.Property);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OpcPublishingInterval setting is not persisted when not configured via method.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadPublishingIntervalUnset))]
        public async Task OpcPublishingIntervalUnsetAndIsNotPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcpublishinginterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcpublishinginterval/{testFilename}";
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
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].RequestedPublishingInterval == OpcApplicationConfiguration.OpcPublishingInterval);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcPublishingInterval == null);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OpcPublishingInterval setting is persisted when configured via method and is different as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadPublishingInterval2000))]
        public async Task OpcPublishingInterval2000AndDifferentAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcpublishinginterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcpublishinginterval/{testFilename}";
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
            OpcPublishingInterval = 3000;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].RequestedPublishingInterval == 2000);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcPublishingInterval == 2000);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OpcPublishingInterval setting is persisted when configured via method and is same as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcPublishingInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadPublishingInterval2000))]
        public async Task OpcPublishingInterval2000AndSameAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcpublishinginterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcpublishinginterval/{testFilename}";
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
            OpcPublishingInterval = 2000;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].RequestedPublishingInterval == 2000);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcPublishingInterval == 2000);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OpcSamplingInterval setting is not persisted when not configured via method.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcSamplingInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSamplingIntervalUnset))]
        public async Task OpcSamplingIntervalUnsetAndIsNotPersisted(string testFilename, string payloadFilename) {

            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcsamplinginterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcsamplinginterval/{testFilename}";
            fqTempFilename = $"{Directory.GetCurrentDirectory()}/tempdata/{methodName}_{testFilename}";
            if (File.Exists(fqTempFilename)) {
                File.Delete(fqTempFilename);
            }
            File.Copy(fqTestFilename, fqTempFilename);
            PublisherNodeConfiguration.PublisherNodeConfigurationFilename = fqTempFilename;
            _output.WriteLine($"now testing: {PublisherNodeConfiguration.PublisherNodeConfigurationFilename}");
            Assert.True(File.Exists(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));

            UnitTestHelper.SetPublisherDefaults();

            try {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].RequestedSamplingInterval == OpcApplicationConfiguration.OpcSamplingInterval);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcSamplingInterval == null);
            }
            finally {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OpcSamplingInterval setting is persisted when configured via method and is different as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcSamplingInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSamplingInterval2000))]
        public async Task OpcSamplingInterval2000AndDifferentAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcsamplinginterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcsamplinginterval/{testFilename}";
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
            OpcSamplingInterval = 3000;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].RequestedSamplingInterval == 2000);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcSamplingInterval == 2000);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that OpcSamplingInterval setting is persisted when configured via method and is same as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "OpcSamplingInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSamplingInterval2000))]
        public async Task OpcSamplingInterval2000AndSameAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcsamplinginterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/opcsamplinginterval/{testFilename}";
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
            OpcSamplingInterval = 2000;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].RequestedSamplingInterval == 2000);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].OpcSamplingInterval == 2000);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that SkipFirst setting is not persisted when default is false and not configured via method.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "SkipFirst")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSkipFirstUnset))]
        public async Task SkipFirstUnsetDefaultFalseAndIsNotPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{testFilename}";
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
            OpcMonitoredItem.SkipFirstDefault = false;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].SkipFirst == false);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].SkipFirst == null);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that SkipFirst setting is not persisted when default is true and not configured via method.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "SkipFirst")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSkipFirstUnset))]
        public async Task SkipFirstUnsetDefaultTrueAndIsNotPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{testFilename}";
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
            OpcMonitoredItem.SkipFirstDefault = true;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].SkipFirst == true);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].SkipFirst == null);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that skipFirst setting is persisted when configured true via method and is different as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "SkipFirst")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSkipFirstTrue))]
        public async Task SkipFirstTrueAndDifferentAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{testFilename}";
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
            OpcMonitoredItem.SkipFirstDefault = false;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].SkipFirst == true);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].SkipFirst == true);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that skipFirst setting is persisted when configured true via method and is save as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "SkipFirst")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSkipFirstTrue))]
        public async Task SkipFirstTrueAndSameAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{testFilename}";
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
            OpcMonitoredItem.SkipFirstDefault = true;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].SkipFirst == true);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].SkipFirst == true);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that skipFirst setting is persisted when configured false via method and is different as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "SkipFirst")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSkipFirstFalse))]
        public async Task SkipFirstFalseAndDifferentAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{testFilename}";
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
            OpcMonitoredItem.SkipFirstDefault = true;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].SkipFirst == false);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].SkipFirst == false);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that skipFirst setting is persisted when configured false via method and is same as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "SkipFirst")]
        [MemberData(nameof(PnPlcEmptyAndPayloadSkipFirstFalse))]
        public async Task SkipFirstFalseAndSameAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/skipfirst/{testFilename}";
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
            OpcMonitoredItem.SkipFirstDefault = false;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].SkipFirst == false);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].SkipFirst == false);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that HeartbeatInterval setting is not persisted when default is 0 not configured via method.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "HeartbeatInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadHeartbeatIntervalUnset))]
        public async Task HeartbeatIntervalUnsetDefaultZeroAndIsNotPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{testFilename}";
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
            OpcMonitoredItem.HeartbeatIntervalDefault = 0;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].HeartbeatInterval == 0);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].HeartbeatInterval == null);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that HeartbeatInterval setting is not persisted when default is 2 and not configured via method.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "HeartbeatInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadHeartbeatIntervalUnset))]
        public async Task HeartbeatIntervalUnsetDefaultNotZeroAndIsNotPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{testFilename}";
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
            OpcMonitoredItem.HeartbeatIntervalDefault = 2;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                await Task.Yield();
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].HeartbeatInterval == 2);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].HeartbeatInterval == null);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that HeartbeatInterval setting is persisted when configured 2 via method and is different as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "HeartbeatInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadHeartbeatInterval2))]
        public async Task HeartbeatInterval2AndDifferentAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{testFilename}";
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
            OpcMonitoredItem.HeartbeatIntervalDefault = 1;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].HeartbeatInterval == 2);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].HeartbeatInterval == 2);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        /// <summary>
        /// Test that HeartbeatInterval setting is persisted when configured 2 via method and is save as default.
        /// </summary>
        [Theory]
        [Trait("Configuration", "DirectMethod")]
        [Trait("ConfigurationSetting", "HeartbeatInterval")]
        [MemberData(nameof(PnPlcEmptyAndPayloadHeartbeatInterval2))]
        public async Task HeartbeatInterval2AndSameAsDefaultAndIsPersisted(string testFilename, string payloadFilename)
        {
            string methodName = UnitTestHelper.GetMethodName();
            string fqTempFilename = string.Empty;
            string fqPayloadFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{payloadFilename}";
            string fqTestFilename = $"{Directory.GetCurrentDirectory()}/testdata/heartbeatinterval/{testFilename}";
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
            OpcMonitoredItem.HeartbeatIntervalDefault = 2;

            try
            {
                Hub = IotHubCommunication.Instance;
                NodeConfiguration = PublisherNodeConfiguration.Instance;
                Assert.True(NodeConfiguration.OpcSessions.Count == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 0, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 0, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 0, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(_configurationFileEntries.Count == 0);
                MethodRequest methodRequest = new MethodRequest("PublishNodes", File.ReadAllBytes(fqPayloadFilename));
                await Hub.HandlePublishNodesMethodAsync(methodRequest, null).ConfigureAwait(false);
                Assert.True(NodeConfiguration.OpcSessions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions.Count == 1);
                Assert.True(NodeConfiguration.OpcSessions[0].OpcSubscriptions[0].OpcMonitoredItems[0].HeartbeatInterval == 2);
                await NodeConfiguration.UpdateNodeConfigurationFileAsync().ConfigureAwait(false);
                _configurationFileEntries = new List<PublisherConfigurationFileEntryLegacyModel>();
                _configurationFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(File.ReadAllText(PublisherNodeConfiguration.PublisherNodeConfigurationFilename));
                Assert.True(NodeConfiguration.OpcSessions.Count == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSessionsConfigured == 1, "wrong # of sessions");
                Assert.True(NodeConfiguration.NumberOfOpcSubscriptionsConfigured == 1, "wrong # of subscriptions");
                Assert.True(NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured == 1, "wrong # of monitored items");
                _output.WriteLine($"sessions configured {NodeConfiguration.NumberOfOpcSessionsConfigured}, connected {NodeConfiguration.NumberOfOpcSessionsConnected}");
                _output.WriteLine($"subscriptions configured {NodeConfiguration.NumberOfOpcSubscriptionsConfigured}, connected {NodeConfiguration.NumberOfOpcSubscriptionsConnected}");
                _output.WriteLine($"items configured {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsConfigured}, monitored {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsMonitored}, toRemove {NodeConfiguration.NumberOfOpcDataChangeMonitoredItemsToRemove}");
                Assert.True(_configurationFileEntries[0].OpcNodes[0].HeartbeatInterval == 2);
            }
            finally
            {
                NodeConfiguration.Dispose();
                NodeConfiguration = null;
                Hub.Dispose();
                Hub = null;
            }
        }

        public static IEnumerable<object[]> PnPlcDeleteConfiguredEndpoint =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_multipleEndpoints.json"),
                    // method payload file
                    new string($"pn_plc_payload_deleteConfiguredEndpoint.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadPublishingInterval2000 =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_publishinginterval_2000.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadPublishingIntervalUnset =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_publishinginterval_unset.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEventPayloadPublishing =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple.json"),
                    // method payload file
                    new string($"pn_plc_payload.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcGetConfiguredEvents =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple.json"),
                    // method payload file
                    new string($"pn_plc_payload_getconfiguredevents.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEventAndEmptyPayloadPublishing =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_simple.json"),
                    // method payload file
                    new string($"pn_plc_empty-payload.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadSamplingInterval2000 =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_samplinginterval_2000.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadSamplingIntervalUnset =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_samplinginterval_unset.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadSkipFirstUnset =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_skipfirst_unset.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadSkipFirstTrue =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_skipfirst_true.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadSkipFirstFalse =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_skipfirst_false.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadHeartbeatIntervalUnset =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_heartbeatinterval_unset.json"),
                },
            };

        public static IEnumerable<object[]> PnPlcEmptyAndPayloadHeartbeatInterval2 =>
            new List<object[]>
            {
                new object[] {
                    // published nodes configuration file
                    new string($"pn_plc_empty.json"),
                    // method payload file
                    new string($"pn_plc_request_payload_heartbeatinterval_2.json"),
                },
            };

        private readonly ITestOutputHelper _output;
        private readonly PlcOpcUaServerFixture _server;
        private static List<PublisherConfigurationFileEntryLegacyModel> _configurationFileEntries;
    }
}
