using Opc.Ua.Client;
using System;
using System.Linq;
using OpcPublisher.AIT;

namespace OpcPublisher
{
    using Opc.Ua;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using static HubCommunicationBase;
    using static OpcApplicationConfiguration;
    using static Program;

    /// <summary>
    /// Base class for DataChange and Event messages.
    /// </summary>
    public class MessageDataBase
    {
        /// <summary>
        /// The endpoint ID the monitored item belongs to.
        /// </summary>
        public string EndpointId { get; set; }

        /// <summary>
        /// The endpoint URL the monitored item belongs to.
        /// </summary>
        public string EndpointUrl { get; set; }

        /// <summary>
        /// The OPC UA NodeId of the monitored item.
        /// </summary>
        public string NodeId { get; set; }

        /// <summary>
        /// The OPC UA Node Id with the namespace expanded.
        /// </summary>
        public string ExpandedNodeId { get; set; }

        /// <summary>
        /// The Application URI of the OPC UA server the node belongs to.
        /// </summary>
        public string ApplicationUri { get; set; }

        /// <summary>
        /// The display name of the node.
        /// </summary>
        public string DisplayName { get; set; }

        /// <summary>
        /// The key under which to publish the node
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Ctor of the object.
        /// </summary>
        public MessageDataBase()
        {
        }

        /// <summary>
        /// Apply the patterns specified in the telemetry configuration on the message data fields.
        /// </summary>
        public void ApplyPatterns(EndpointTelemetryConfigurationModel telemetryConfiguration)
        {
            if(telemetryConfiguration.EndpointId.Publish == true)
            {
                EndpointId = telemetryConfiguration.EndpointId.PatternMatch(EndpointId);
            }
            if (telemetryConfiguration.EndpointUrl.Publish == true)
            {
                EndpointUrl = telemetryConfiguration.EndpointUrl.PatternMatch(EndpointUrl);
            }
            if (telemetryConfiguration.NodeId.Publish == true)
            {
                NodeId = telemetryConfiguration.NodeId.PatternMatch(NodeId);
            }
            if (telemetryConfiguration.MonitoredItem.ApplicationUri.Publish == true)
            {
                ApplicationUri = telemetryConfiguration.MonitoredItem.ApplicationUri.PatternMatch(ApplicationUri);
            }
            if (telemetryConfiguration.MonitoredItem.DisplayName.Publish == true)
            {
                DisplayName = telemetryConfiguration.MonitoredItem.DisplayName.PatternMatch(DisplayName);
            }
        }
    }


    /// <summary>
    /// Class used to pass data from the Event MonitoredItem event notification to the hub message processing.
    /// </summary>
    public class EventMessageData : MessageDataBase
    {
        /// <summary>
        /// The value of the node.
        /// </summary>
        public List<EventValue> EventValues { get; set; }

        /// <summary>
        /// The publish time of the event.
        /// </summary>
        public string PublishTime { get; set; }

        /// <summary>
        /// This property is used to publish all event select clauses as IoT Central event.
        /// </summary>
        public IotCentralEventPublishMode? IotCentralEventPublishMode { get; set; }


        /// <summary>
        /// Ctor of the object.
        /// </summary>
        public EventMessageData()
        {
            EventValues = new List<EventValue>();
            PublishTime = null;
        }

        /// <summary>
        /// Apply the patterns specified in the telemetry configuration on the message data fields.
        /// </summary>
        public new void ApplyPatterns(EndpointTelemetryConfigurationModel telemetryConfiguration)
        {
            base.ApplyPatterns(telemetryConfiguration);

            if (telemetryConfiguration.Value.PublishTime.Publish == true)
            {
                PublishTime = telemetryConfiguration.Value.PublishTime.PatternMatch(PublishTime);
            }
        }
    }

    /// <summary>
    /// Class used to pass data from the DataChange MonitoredItem notification to the hub message processing.
    /// </summary>
    public class DataChangeMessageData : MessageDataBase
    {
        /// <summary>
        /// The value of the node.
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Flag if the encoding of the value should preserve quotes.
        /// </summary>
        public bool PreserveValueQuotes { get; set; }

        /// <summary>
        /// The OPC UA source timestamp the value was seen.
        /// </summary>
        public string SourceTimestamp { get; set; }

        /// <summary>
        /// The timestamp when the value was received by the opc publisher.
        /// </summary>
        public string ReceiveTimestamp { get; set; }

        /// <summary>
        /// The OPC UA status code of the value.
        /// </summary>
        public uint? StatusCode { get; set; }

        /// <summary>
        /// The OPC UA status of the value.
        /// </summary>
        public string Status { get; set; }

        public IotCentralItemPublishMode? IotCentralItemPublishMode { get; set; }

        /// <summary>
        /// Ctor of the object.
        /// </summary>
        public DataChangeMessageData()
        {
        }

        /// <summary>
        /// Apply the patterns specified in the telemetry configuration on the message data fields.
        /// </summary>
        public new void ApplyPatterns(EndpointTelemetryConfigurationModel telemetryConfiguration)
        {
            base.ApplyPatterns(telemetryConfiguration);

            if (telemetryConfiguration.Value.Value.Publish == true)
            {
                Value = telemetryConfiguration.Value.Value.PatternMatch(Value);
            }
            if (telemetryConfiguration.Value.SourceTimestamp.Publish == true)
            {
                SourceTimestamp = telemetryConfiguration.Value.SourceTimestamp.PatternMatch(SourceTimestamp);
            }
            if (telemetryConfiguration.Value.ReceiveTimestamp.Publish == true)
            {
                ReceiveTimestamp = telemetryConfiguration.Value.ReceiveTimestamp.PatternMatch(ReceiveTimestamp);
            }
            if (telemetryConfiguration.Value.StatusCode.Publish == true && StatusCode != null)
            {
                if (!string.IsNullOrEmpty(telemetryConfiguration.Value.StatusCode.Pattern))
                {
                    Logger.Information($"'Pattern' settngs for StatusCode are ignored.");
                }
            }
            if (telemetryConfiguration.Value.Status.Publish == true)
            {
                Status = telemetryConfiguration.Value.Status.PatternMatch(Status);
            }
        }
    }

    /// <summary>
    /// Class used to pass data from the MonitoredItem notifications to the hub message processing.
    /// </summary>
    public class MessageData
    {
        /// <summary>
        /// Data from a data notification.
        /// </summary>
        public DataChangeMessageData DataChangeMessageData;

        /// <summary>
        /// Data from an event notification.
        /// </summary>
        public EventMessageData EventMessageData;

        /// <summary>
        /// Ctor of the object.
        /// </summary>
        public MessageData()
        {
            DataChangeMessageData = null;
            EventMessageData = null;
        }
    }

    /// <summary>
    /// Class used to pass key/value pairs of event field data from the MonitoredItem event notification to the hub message processing.
    /// </summary>
    public class EventValue
    {
        /// <summary>
        /// Ctor of the class
        /// </summary>
        public EventValue()
        {
            Name = string.Empty;
            Value = string.Empty;
            PreserveValueQuotes = false;
        }

        /// <summary>
        /// The name of the field.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The value of the field
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Flag to control quote handling in the value.
        /// </summary>
        public bool PreserveValueQuotes { get; set; }

        /// <summary>
        /// Property to control publishing mode in IoT-Central
        /// </summary>
        public IotCentralEventPublishMode IotCentralEventPublishMode { get; set; }
    }

    /// <summary>
    /// Class to manage the OPC monitored items, which are the nodes we need to publish.
    /// </summary>
    public class OpcMonitoredItem : IOpcMonitoredItem
    {
        /// <summary>
        /// The state of the monitored item.
        /// </summary>
        public enum OpcMonitoredItemState
        {
            Unmonitored = 0,
            UnmonitoredNamespaceUpdateRequested,
            Monitored,
            RemovalRequested,
        }

        /// <summary>
        /// The configuration type of the monitored item.
        /// </summary>
        public enum OpcMonitoredItemConfigurationType
        {
            NodeId = 0,
            ExpandedNodeId
        }

        /// <summary>
        /// A human readable key to uniquely identify the node. DisplayName is not Unique, NodeId might be not human readable in case of ints, GUIDs
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// The display name to use in the telemetry event for the monitored item.
        /// </summary>
        public string DisplayName { get; set; }

        /// <summary>
        /// Flag to signal that the display name was requested by the node configuration.
        /// </summary>
        public bool DisplayNameFromConfiguration { get; set; }

        /// <summary>
        /// The state of the monitored item.
        /// </summary>
        public OpcMonitoredItemState State { get; set; }

        /// <summary>
        /// The OPC UA attributes to use when monitoring the node.
        /// </summary>
        public uint AttributeId { get; set; }

        /// <summary>
        /// The OPC UA monitoring mode to use when monitoring the node.
        /// </summary>
        public MonitoringMode MonitoringMode { get; set; }

        /// <summary>
        /// The requested sampling interval to be used for the node.
        /// </summary>
        public int RequestedSamplingInterval { get; set; }

        /// <summary>
        /// The actual sampling interval used for the node.
        /// </summary>
        public double SamplingInterval { get; set; }

        /// <summary>
        /// Flag to signal that the sampling interval was requested by the node configuration.
        /// </summary>
        public bool RequestedSamplingIntervalFromConfiguration { get; set; }

        /// <summary>
        /// The OPC UA queue size to use for the node monitoring.
        /// </summary>
        public uint QueueSize { get; set; }

        /// <summary>
        /// A flag to control the queue behaviour of the OPC UA stack for the node.
        /// </summary>
        public bool DiscardOldest { get; set; }

        /// <summary>
        /// The event handler of the node in case the OPC UA stack detected a change.
        /// </summary>
        public MonitoredItemNotificationEventHandler NotificationEventHandler { get; set; }

        /// <summary>
        /// The endpoint URL of the OPC UA server this nodes is residing on.
        /// </summary>
        public Guid EndpointId { get; set; }

        /// <summary>
        /// The endpoint URL of the OPC UA server this nodes is residing on.
        /// </summary>
        public string EndpointUrl { get; set; }

        /// <summary>
        /// The OPC UA stacks monitored item object.
        /// </summary>
        public IOpcUaMonitoredItem OpcUaClientMonitoredItem { get; set; }

        /// <summary>
        /// The OPC UA identifier of the node in NodeId ("ns=") syntax.
        /// </summary>
        public NodeId ConfigNodeId { get; set; }

        /// <summary>
        /// The OPC UA identifier of the node in ExpandedNodeId ("nsu=") syntax.
        /// </summary>
        public ExpandedNodeId ConfigExpandedNodeId { get; set; }

        /// <summary>
        /// The OPC UA identifier of the node as it was configured.
        /// </summary>
        public string OriginalId { get; set; }

        /// <summary>
        /// The OPC UA identifier of the node as it was configured.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The unique id of the event if this monitored node is an event.
        /// </summary>
        public Guid EventId { get; set; }

        // todo use the same model for nodes as we use now for events to store the original setting
        /// <summary>
        /// The OPC UA identifier of the node as ExpandedNodeId ("nsu=").
        /// </summary>
        public ExpandedNodeId IdAsExpandedNodeId { get; set; }

        /// <summary>
        /// The OPC UA identifier of the node as NodeId ("ns=").
        /// </summary>
        public NodeId IdAsNodeId { get; set; }

        /// <summary>
        /// The OPC UA event filter as configured, if the monitored item is for an event.
        /// </summary>
        public EventConfigurationModel EventConfiguration { get; set; }

        ///// <summary>
        ///// The OPC UA data filter if the monitored items is for node values.
        ///// </summary>
        //public EventFilter OpcUaDataFilter { get; set; }

        /// <summary>
        /// Identifies the configuration type of the node.
        /// </summary>
        public OpcMonitoredItemConfigurationType ConfigType { get; set; }

        /// <summary>
        /// Configure how IoT Central should publish the monitored Item.
        /// </summary>
        public IotCentralItemPublishMode? IotCentralItemPublishMode { get; set; }

        public const int HeartbeatIntvervalMax = 24 * 60 * 60;

        public static int? HeartbeatIntervalDefault { get; set; } = 0;

        public int HeartbeatInterval
        {
            get => _heartbeatInterval;
            set => _heartbeatInterval = (value <= 0 ? 0 : value > HeartbeatIntvervalMax ? HeartbeatIntvervalMax : value);
        }

        public bool HeartbeatIntervalFromConfiguration { get; set; } = false;

        public DataChangeMessageData HeartbeatMessage { get; set; } = null;

        public Timer HeartbeatSendTimer { get; set; } = null;

        public bool SkipNextEvent { get; set; } = false;

        public static bool SkipFirstDefault { get; set; } = false;

        public bool SkipFirst { get; set; }

        public bool SkipFirstFromConfiguration { get; set; } = false;

        public const string SuppressedOpcStatusCodesDefault = "BadNoCommunication, BadWaitingForInitialData";

        public static List<uint> SuppressedOpcStatusCodes { get; } = new List<uint>();

        /// <summary>
        /// Ctor using NodeId (ns syntax for namespace).
        /// </summary>
        public OpcMonitoredItem(NodeId nodeId, Guid sessionEndpointId, string sessionEndpointUrl, int? samplingInterval, string key,
            string displayName, int? heartbeatInterval, bool? skipFirst, IotCentralItemPublishMode? iotCentralItemPublishMode)
        {
            ConfigNodeId = nodeId;
            ConfigExpandedNodeId = null;
            OriginalId = nodeId.ToString();
            ConfigType = OpcMonitoredItemConfigurationType.NodeId;
            Init(sessionEndpointId, sessionEndpointUrl, samplingInterval, key, displayName, heartbeatInterval, skipFirst);
            State = OpcMonitoredItemState.Unmonitored;
            IotCentralItemPublishMode = iotCentralItemPublishMode;
        }

        /// <summary>
        /// Ctor using ExpandedNodeId ("nsu=") syntax.
        /// </summary>
        public OpcMonitoredItem(ExpandedNodeId expandedNodeId, Guid sessionEndpointId, string sessionEndpointUrl, int? samplingInterval, string key,
            string displayName, int? heartbeatInterval, bool? skipFirst, IotCentralItemPublishMode? iotCentralItemPublishMode)
        {
            ConfigNodeId = null;
            ConfigExpandedNodeId = expandedNodeId;
            OriginalId = expandedNodeId.ToString();
            ConfigType = OpcMonitoredItemConfigurationType.ExpandedNodeId;
            Init(sessionEndpointId, sessionEndpointUrl, samplingInterval, key, displayName, heartbeatInterval, skipFirst);
            State = OpcMonitoredItemState.UnmonitoredNamespaceUpdateRequested;
            IotCentralItemPublishMode = iotCentralItemPublishMode;
        }

        /// <summary>
        /// Ctor for event
        /// </summary>
        public OpcMonitoredItem(EventConfigurationModel opcEvent, Guid sessionEndpointId, string sessionEndpointUrl)
        {
            Id = opcEvent.EventNotifierId;
            Key = opcEvent.Key;
            EventId = opcEvent.Id;
            if (Id.StartsWith("nsu=", StringComparison.InvariantCulture))
            {
                ConfigType = OpcMonitoredItemConfigurationType.ExpandedNodeId;
            }
            else
            {
                ConfigType = OpcMonitoredItemConfigurationType.NodeId;
            }
            DisplayName = opcEvent.Key;
            DisplayNameFromConfiguration = string.IsNullOrEmpty(opcEvent.Key) ? false : true;
            EventConfiguration = opcEvent;
            State = OpcMonitoredItemState.Unmonitored;
            AttributeId = Attributes.EventNotifier;
            MonitoringMode = MonitoringMode.Reporting;
            // todo need to check if we use Uint32.Max
            QueueSize = 0;
            DiscardOldest = true;
            NotificationEventHandler = new MonitoredItemNotificationEventHandler(MonitoredItemEventNotificationEventHandler);
            EndpointId = sessionEndpointId;
            EndpointUrl = sessionEndpointUrl;
            RequestedSamplingInterval = OpcSamplingInterval;
            RequestedSamplingIntervalFromConfiguration = false;
            SamplingInterval = RequestedSamplingInterval;
            HeartbeatInterval = 0;
            HeartbeatIntervalFromConfiguration = false;
            SkipFirst = false;
            SkipFirstFromConfiguration = false;            
        }

        /// <summary>
        /// Checks if the monitored item does monitor the node described by the given objects.
        /// </summary>
        public bool IsMonitoringThisNode(NodeId nodeId, ExpandedNodeId expandedNodeId, NamespaceTable namespaceTable)
        {
            if (State == OpcMonitoredItemState.RemovalRequested)
            {
                return false;
            }
            if (ConfigType == OpcMonitoredItemConfigurationType.NodeId)
            {
                if (nodeId != null)
                {
                    if (ConfigNodeId == nodeId)
                    {
                        return true;
                    }
                }
                if (expandedNodeId != null)
                {
                    string namespaceUri = namespaceTable.ToArray().ElementAtOrDefault(ConfigNodeId.NamespaceIndex);
                    if (expandedNodeId.NamespaceUri != null && expandedNodeId.NamespaceUri.Equals(namespaceUri, StringComparison.OrdinalIgnoreCase))
                    {
                        if (expandedNodeId.Identifier.ToString().Equals(ConfigNodeId.Identifier.ToString(), StringComparison.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    }
                }
            }
            if (ConfigType == OpcMonitoredItemConfigurationType.ExpandedNodeId)
            {
                if (nodeId != null)
                {
                    int namespaceIndex = namespaceTable.GetIndex(ConfigExpandedNodeId?.NamespaceUri);
                    if (nodeId.NamespaceIndex == namespaceIndex)
                    {
                        if (nodeId.Identifier.ToString().Equals(ConfigExpandedNodeId.Identifier.ToString(), StringComparison.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    }
                }
                if (expandedNodeId != null)
                {
                    if (ConfigExpandedNodeId.NamespaceUri != null &&
                        ConfigExpandedNodeId.NamespaceUri.Equals(expandedNodeId.NamespaceUri, StringComparison.OrdinalIgnoreCase) &&
                        ConfigExpandedNodeId.Identifier.ToString().Equals(expandedNodeId.Identifier.ToString(), StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// The notification that the data for a monitored item has changed on an OPC UA server.
        /// </summary>
        public void MonitoredItemDataChangeNotificationEventHandler(MonitoredItem monitoredItem, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                if (e == null || e.NotificationValue == null || monitoredItem == null || monitoredItem.Subscription == null || monitoredItem.Subscription.Session == null)
                {
                    return;
                }

                if (!(e.NotificationValue is MonitoredItemNotification notification))
                {
                    return;
                }

                if (!(notification.Value is DataValue value))
                {
                    return;
                }

                // filter out configured suppression status codes
                if (SuppressedOpcStatusCodes != null && SuppressedOpcStatusCodes.Contains(notification.Value.StatusCode.Code))
                {
                    Logger.Debug($"Filtered notification with status code '{notification.Value.StatusCode.Code}'");
                    return;
                }

                // stop the heartbeat timer
                HeartbeatSendTimer?.Change(Timeout.Infinite, Timeout.Infinite);

                DataChangeMessageData dataChangeMessageData = new DataChangeMessageData();
                dataChangeMessageData.EndpointId = EndpointId.ToString();
                dataChangeMessageData.Key = Key;
                dataChangeMessageData.IotCentralItemPublishMode = IotCentralItemPublishMode;

                // update the required message data to pass only the required data to the hub communication
                // since the router relies on a fixed message format, we dont allow per-endpoint configuration and use the default for all endpoints
                var telemetryConfiguration = TelemetryConfiguration.DefaultEndpointTelemetryConfiguration;

                // the endpoint URL is required to allow HubCommunication lookup the telemetry configuration
                dataChangeMessageData.EndpointUrl = EndpointUrl;

                if (telemetryConfiguration.ExpandedNodeId.Publish == true)
                {
                    dataChangeMessageData.ExpandedNodeId = ConfigExpandedNodeId?.ToString();
                }
                if (telemetryConfiguration.NodeId.Publish == true)
                {
                    dataChangeMessageData.NodeId = OriginalId;
                }
                if (telemetryConfiguration.MonitoredItem.ApplicationUri.Publish == true)
                {
                    dataChangeMessageData.ApplicationUri = (monitoredItem.Subscription.Session.Endpoint.Server.ApplicationUri + (string.IsNullOrEmpty(OpcSession.PublisherSite) ? "" : $":{OpcSession.PublisherSite}"));
                }
                if (telemetryConfiguration.MonitoredItem.DisplayName.Publish == true && monitoredItem.DisplayName != null)
                {
                    // use the DisplayName as reported in the MonitoredItem
                    dataChangeMessageData.DisplayName = monitoredItem.DisplayName;
                }
                if (telemetryConfiguration.Value.SourceTimestamp.Publish == true && value.SourceTimestamp != null)
                {
                    // use the SourceTimestamp as reported in the notification event argument in ISO8601 format
                    dataChangeMessageData.SourceTimestamp = value.SourceTimestamp.ToString("o", CultureInfo.InvariantCulture);
                }
                if (telemetryConfiguration.Value.ReceiveTimestamp.Publish == true)
                {
                    // use the ReceiveTimestamp as reported in the notification event argument in ISO8601 format
                    dataChangeMessageData.ReceiveTimestamp = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture);
                }
                if (telemetryConfiguration.Value.StatusCode.Publish == true && value.StatusCode != null)
                {
                    // use the StatusCode as reported in the notification event argument
                    dataChangeMessageData.StatusCode = value.StatusCode.Code;
                }
                if (telemetryConfiguration.Value.Status.Publish == true && value.StatusCode != null)
                {
                    // use the StatusCode as reported in the notification event argument to lookup the symbolic name
                    dataChangeMessageData.Status = StatusCode.LookupSymbolicId(value.StatusCode.Code);
                }
                if (telemetryConfiguration.Value.Value.Publish == true && value.Value != null)
                {
                    string encodedValue = string.Empty;
                    EncodeValue(value, monitoredItem.Subscription.Session.MessageContext, out encodedValue, out bool preserveValueQuotes);
                    dataChangeMessageData.Value = encodedValue;
                    dataChangeMessageData.PreserveValueQuotes = preserveValueQuotes;
                }

                // currently the pattern processing is done here, which adds runtime to the notification processing.
                // In case of perf issues it can be also done in CreateJsonForDataChangeAsync of IoTHubMessaging.cs.

                // apply patterns
                dataChangeMessageData.ApplyPatterns(telemetryConfiguration);

                Logger.Debug($"   ApplicationUri: {dataChangeMessageData.ApplicationUri}");
                Logger.Debug($"   EndpointUrl: {dataChangeMessageData.EndpointUrl}");
                Logger.Debug($"   DisplayName: {dataChangeMessageData.DisplayName}");
                Logger.Debug($"   Value: {dataChangeMessageData.Value}");

                // add message to fifo send queue
                if (monitoredItem.Subscription == null)
                {
                    Logger.Debug($"Subscription already removed. No more details available.");
                }
                else
                {
                    Logger.Debug($"EnqueueProperty a new message from subscription {(monitoredItem.Subscription == null ? "removed" : monitoredItem.Subscription.Id.ToString(CultureInfo.InvariantCulture))}");
                    Logger.Debug($" with publishing interval: {monitoredItem?.Subscription?.PublishingInterval} and sampling interval: {monitoredItem?.SamplingInterval}):");
                }

                // setupo heartbeat processing
                if (HeartbeatInterval > 0)
                {
                    if (HeartbeatMessage != null)
                    {
                        // ensure that the timestamp of the message is larger than the current heartbeat message
                        lock (HeartbeatMessage)
                        {
                            if (DateTime.TryParse(dataChangeMessageData.SourceTimestamp, out DateTime sourceTimestamp) && DateTime.TryParse(HeartbeatMessage.SourceTimestamp, out DateTime heartbeatSourceTimestamp))
                            {
                                if (heartbeatSourceTimestamp >= sourceTimestamp)
                                {
                                    Logger.Warning($"HeartbeatMessage has larger or equal timestamp than message. Adjusting...");
                                    sourceTimestamp.AddMilliseconds(1);
                                }
                                dataChangeMessageData.SourceTimestamp = sourceTimestamp.ToString("o", CultureInfo.InvariantCulture);
                            }

                            // store the message for the heartbeat
                            HeartbeatMessage = dataChangeMessageData;
                        }
                    }
                    else
                    {
                        HeartbeatMessage = dataChangeMessageData;
                    }

                    // recharge the heartbeat timer
                    HeartbeatSendTimer.Change(HeartbeatInterval * 1000, HeartbeatInterval * 1000);
                    Logger.Debug($"Setting up {HeartbeatInterval} sec heartbeat for node '{DisplayName}'.");
                }

                // skip event if needed
                if (SkipNextEvent)
                {
                    Logger.Debug($"Skipping first telemetry event for node '{DisplayName}'.");
                    SkipNextEvent = false;
                }
                else
                {
                    // enqueue the telemetry event
                    MessageData messageData = new MessageData();
                    messageData.DataChangeMessageData = dataChangeMessageData;
                    if (SendHub != null)
                    {
                        if (messageData.DataChangeMessageData.IotCentralItemPublishMode == OpcPublisher.AIT.IotCentralItemPublishMode.Setting)
                            SendHub.EnqueueSetting(messageData);
                        else if (messageData.DataChangeMessageData.IotCentralItemPublishMode == OpcPublisher.AIT.IotCentralItemPublishMode.Property)
                            SendHub.EnqueueProperty(messageData);
                        else
                            SendHub.Enqueue(messageData);
                    }
                    else
                    {
                        if (messageData.DataChangeMessageData.IotCentralItemPublishMode == OpcPublisher.AIT.IotCentralItemPublishMode.Setting)
                            Hub.EnqueueSetting(messageData);
                        else if (messageData.DataChangeMessageData.IotCentralItemPublishMode == OpcPublisher.AIT.IotCentralItemPublishMode.Property)
                            Hub.EnqueueProperty(messageData);
                        else
                            Hub.Enqueue(messageData);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "Error processing monitored item notification");
            }
        }


        /// <summary>
        /// The notification that a monitored item event has occured on an OPC UA server.
        /// </summary>
        public void MonitoredItemEventNotificationEventHandler(MonitoredItem monitoredItem, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                if (e == null || e.NotificationValue == null || monitoredItem == null || monitoredItem.Subscription == null || monitoredItem.Subscription.Session == null)
                {
                    return;
                }

                if (!(e.NotificationValue is EventFieldList notificationValue))
                {
                    return;
                }

                if (!(notificationValue.Message is NotificationMessage message))
                {
                    return;
                }

                if (!(message.NotificationData is ExtensionObjectCollection notificationData) || notificationData.Count == 0)
                {
                    return;
                }

                EventMessageData eventMessageData = new EventMessageData();
                eventMessageData.EndpointId = EndpointId.ToString();
                eventMessageData.EndpointUrl = EndpointUrl;
                eventMessageData.PublishTime = message.PublishTime.ToString("o", CultureInfo.InvariantCulture);
                eventMessageData.ApplicationUri = monitoredItem.Subscription.Session.Endpoint.Server.ApplicationUri + (string.IsNullOrEmpty(OpcSession.PublisherSite) ? "" : $":{OpcSession.PublisherSite}");
                eventMessageData.DisplayName = monitoredItem.DisplayName;
                eventMessageData.NodeId = monitoredItem.StartNodeId.ToString();
                eventMessageData.IotCentralEventPublishMode = EventConfiguration.IotCentralEventPublishMode;
                foreach (var eventList in notificationData)
                {
                    EventNotificationList eventNotificationList = eventList.Body as EventNotificationList;
                    foreach (var eventFieldList in eventNotificationList.Events)
                    {
                        int i = 0;
                        foreach (var eventField in eventFieldList.EventFields)
                        {
                            // prepare event field values
                            EventValue eventValue = new EventValue();
                            eventValue.Name = monitoredItem.GetFieldName(i++);

                            // use the Value as reported in the notification event argument encoded with the OPC UA JSON endcoder
                            DataValue value = new DataValue(eventField);
                            string encodedValue = string.Empty;
                            EncodeValue(value, monitoredItem.Subscription.Session.MessageContext, out encodedValue, out bool preserveValueQuotes);
                            eventValue.Value = encodedValue;
                            eventValue.PreserveValueQuotes = preserveValueQuotes;
                            var selectClause = EventConfiguration.SelectClauses.SingleOrDefault(w => w.BrowsePaths.Any(x => eventValue.Name.Contains(x)));
                            eventMessageData.EventValues.Add(eventValue);
                            Logger.Debug($"Event notification field name: '{eventValue.Name}', value: '{eventValue.Value}'");
                        }
                    }
                }

                // add message to fifo send queue
                if (monitoredItem.Subscription == null)
                {
                    Logger.Debug($"Subscription already removed. No more details available.");
                }
                else
                {
                    Logger.Debug($"EnqueueProperty a new message from subscription {(monitoredItem.Subscription == null ? "removed" : monitoredItem.Subscription.Id.ToString(CultureInfo.InvariantCulture))}");
                    Logger.Debug($" with publishing interval: {monitoredItem?.Subscription?.PublishingInterval} and sampling interval: {monitoredItem?.SamplingInterval}):");
                }

                // enqueue the telemetry event
                MessageData messageData = new MessageData();
                messageData.EventMessageData = eventMessageData;
                if (SendHub != null)
                {
                    Logger.Debug("SendHub is used for Telemetry sending");
                    if (messageData.EventMessageData.IotCentralEventPublishMode == IotCentralEventPublishMode.Property)
                        SendHub.EnqueueProperty(messageData);
                    else if(messageData.EventMessageData.IotCentralEventPublishMode == IotCentralEventPublishMode.Event)
                        SendHub.EnqueueEvent(messageData);
                    else
                        SendHub.Enqueue(messageData);
                }
                else
                {
                    if (messageData.EventMessageData.IotCentralEventPublishMode == IotCentralEventPublishMode.Property)
                        Hub.EnqueueProperty(messageData);
                    else if (messageData.EventMessageData.IotCentralEventPublishMode == IotCentralEventPublishMode.Event)
                        Hub.EnqueueEvent(messageData);
                    else
                        Hub.Enqueue(messageData);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "Error processing monitored item notification");
            }
        }

        /// <summary>
        /// Encode a value and returns is as string. If the value is a string with quotes, we need to preserve the quotes.
        /// </summary>
        private void EncodeValue(DataValue value, ServiceMessageContext messageContext, out string encodedValue, out bool preserveValueQuotes)
        {
            // use the Value as reported in the notification event argument encoded with the OPC UA JSON endcoder
            JsonEncoder encoder = new JsonEncoder(messageContext, false);
            value.ServerTimestamp = DateTime.MinValue;
            value.SourceTimestamp = DateTime.MinValue;
            value.StatusCode = StatusCodes.Good;
            encoder.WriteDataValue("Value", value);
            string valueString = encoder.CloseAndReturnText();
            // we only want the value string, search for everything till the real value starts
            // and get it
            string marker = "{\"Value\":{\"Value\":";
            int markerStart = valueString.IndexOf(marker, StringComparison.InvariantCulture);
            preserveValueQuotes = true;
            if (markerStart >= 0)
            {
                // we either have a value in quotes or just a value
                int valueLength;
                int valueStart = marker.Length;
                if (valueString.IndexOf("\"", valueStart, StringComparison.InvariantCulture) >= 0)
                {
                    // value is in quotes and two closing curly brackets at the end
                    valueStart++;
                    valueLength = valueString.Length - valueStart - 3;
                }
                else
                {
                    // value is without quotes with two curly brackets at the end
                    valueLength = valueString.Length - marker.Length - 2;
                    preserveValueQuotes = false;
                }
                encodedValue = valueString.Substring(valueStart, valueLength);
            }
            else
            {
                encodedValue = string.Empty;
            }
        }

        /// <summary>
        /// Init instance variables.
        /// </summary>
        private void Init(Guid sessionEndpointId, string sessionEndpointUrl, int? samplingInterval, string key, string displayName, int? heartbeatInterval, bool? skipFirst)
        {
            State = OpcMonitoredItemState.Unmonitored;
            AttributeId = Attributes.Value;
            MonitoringMode = MonitoringMode.Reporting;
            QueueSize = 0;
            DiscardOldest = true;
            NotificationEventHandler = new MonitoredItemNotificationEventHandler(MonitoredItemDataChangeNotificationEventHandler);
            EndpointId = sessionEndpointId;
            EndpointUrl = sessionEndpointUrl;
            Key = key;
            DisplayName = displayName;
            DisplayNameFromConfiguration = string.IsNullOrEmpty(displayName) ? false : true;
            RequestedSamplingInterval = samplingInterval ?? OpcSamplingInterval;
            RequestedSamplingIntervalFromConfiguration = samplingInterval != null ? true : false;
            SamplingInterval = RequestedSamplingInterval;
            HeartbeatInterval = (int)(heartbeatInterval == null ? HeartbeatIntervalDefault : heartbeatInterval);
            HeartbeatIntervalFromConfiguration = heartbeatInterval != null ? true : false;
            SkipFirst = skipFirst ?? SkipFirstDefault;
            SkipFirstFromConfiguration = skipFirst != null ? true : false;
        }

        /// <summary>
        /// Timer callback for heartbeat telemetry send.
        /// </summary>
        internal void HeartbeatSend(object state)
        {
            // send the last known message
            lock (HeartbeatMessage)
            {
                if (HeartbeatMessage != null)
                {
                    // advance the SourceTimestamp
                    if (DateTime.TryParse(HeartbeatMessage.SourceTimestamp, out DateTime sourceTimestamp))
                    {
                        sourceTimestamp = sourceTimestamp.AddSeconds(HeartbeatInterval);
                        HeartbeatMessage.SourceTimestamp = sourceTimestamp.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture);
                    }
                    HeartbeatMessage.EndpointId = EndpointId.ToString();
                    // enqueue the message
                    MessageData messageData = new MessageData();
                    messageData.DataChangeMessageData = HeartbeatMessage;
                    SendHub.Enqueue(messageData);
                    Logger.Debug($"Message enqueued for heartbeat with sourceTimestamp '{HeartbeatMessage.SourceTimestamp}'.");
                }
                else
                {
                    Logger.Warning($"No message is available for heartbeat.");
                }
            }
        }

        private int _heartbeatInterval = 0;
    }
}
