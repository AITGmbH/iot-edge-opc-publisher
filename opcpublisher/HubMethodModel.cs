using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;

namespace OpcPublisher
{
    /// <summary>
    /// Model for a get info response.
    /// </summary>
    public class GetInfoMethodResponseModel
    {
        public GetInfoMethodResponseModel()
        {
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int VersionMajor { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int VersionMinor { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int VersionPatch { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string SemanticVersion { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string InformationalVersion { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string OS { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public Architecture OSArchitecture { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string FrameworkDescription { get; set; }
    }

    /// <summary>
    /// Model for a diagnostic info response.
    /// </summary>
    public class DiagnosticInfoMethodResponseModel
    {
        public DiagnosticInfoMethodResponseModel()
        {
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public DateTime PublisherStartTime { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcSessionsConfigured { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcSessionsConnected { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcSubscriptionsConfigured { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcSubscriptionsConnected { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcDataChangeMonitoredItemsConfigured { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcDataChangeMonitoredItemsMonitored { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcDataChangeMonitoredItemsToRemove { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcEventMonitoredItemsConfigured { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcEventMonitoredItemsMonitored { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int NumberOfOpcEventMonitoredItemsToRemove { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int MonitoredItemsQueueCapacity { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int MonitoredPropertiesQueueCapacity { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int MonitoredSettingsQueueCapacity { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long MonitoredItemsQueueCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long MonitoredPropertiesQueueCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long MonitoredSettingsQueueCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long EnqueueCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long EnqueueFailureCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long NumberOfDataChangeEvents { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long NumberOfEvents { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long SentMessages { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public DateTime SentLastTime { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long SentBytes { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long FailedMessages { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long TooLargeCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long MissedSendIntervalCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long WorkingSetMB { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int DefaultSendIntervalSeconds { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public uint HubMessageSize { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public TransportType HubProtocol { get; set; }
        
        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long SentProperties { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long SentSettings { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public long SentIoTCEvents { get; set; }
    }

    /// <summary>
    /// Model for a diagnostic log response.
    /// </summary>
    public class DiagnosticLogMethodResponseModel
    {
        public DiagnosticLogMethodResponseModel()
        {
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int MissedMessageCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int LogMessageCount { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public List<string> Log { get; } = new List<string>();
    }

    /// <summary>
    /// Model for an exit application request.
    /// </summary>
    public class ExitApplicationMethodRequestModel
    {
        public ExitApplicationMethodRequestModel()
        {
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public int SecondsTillExit { get; set; }
    }

    /// <summary>
    /// ´Model for a publish node request.
    /// </summary>
    public class PublishNodesMethodRequestModel
    {
        public PublishNodesMethodRequestModel(string endpointId, string endpointName, string endpointUrl, bool useSecurity = true, string userName = null, string password = null)
        {
            OpcNodes = new List<OpcNodeOnEndpointModel>();
            EndpointId = endpointId;
            EndpointName = endpointName;
            EndpointUrl = endpointUrl;
            UseSecurity = useSecurity;
            UserName = userName;
            Password = password;
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string EndpointId { get; set; }
        public string EndpointName { get; set; }
        public string EndpointUrl { get; set; }
        public List<OpcNodeOnEndpointModel> OpcNodes { get; }
        public List<OpcEventOnEndpointModel> OpcEvents { get; } = new List<OpcEventOnEndpointModel>();

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        [JsonConverter(typeof(StringEnumConverter))]
        public OpcAuthenticationMode? OpcAuthenticationMode { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public bool UseSecurity { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string UserName { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public string Password { get; set; }
    }

    /// <summary>
    /// ´Model for a publish node request.
    /// </summary>
    public class PublishNodesMethodResponseModel
    {
        public PublishNodesMethodResponseModel(string endpointId)
        {
            EndpointId = endpointId;
        }

        public string EndpointId { get; set; }
    }

    /// <summary>
    /// Model for an unpublish node request.
    /// </summary>
    public class UnpublishNodesMethodRequestModel
    {
        public UnpublishNodesMethodRequestModel(string endpointId)
        {
            EndpointId = endpointId;
        }

        public string EndpointId { get; set; }

        public List<OpcNodeOnEndpointModel> OpcNodes { get; } = new List<OpcNodeOnEndpointModel>();
        
        public List<OpcEventOnEndpointModel> OpcEvents { get; } = new List<OpcEventOnEndpointModel>();
    }

    /// <summary>
    /// Model for an unpublish all nodes request.
    /// </summary>
    public class UnpublishAllNodesMethodRequestModel
    {
        public UnpublishAllNodesMethodRequestModel(string endpointId)
        {
            EndpointId = endpointId;
        }

        public string EndpointId { get; set; }
    }

    /// <summary>
    /// Model for a get configured endpoints request.
    /// </summary>
    public class GetConfiguredEndpointsMethodRequestModel
    {
        public GetConfiguredEndpointsMethodRequestModel(ulong? continuationToken = null)
        {
            ContinuationToken = continuationToken;
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public ulong? ContinuationToken { get; set; }
    }

    public class DeleteConfiguredEndpointMethodRequestModel
    {
        public DeleteConfiguredEndpointMethodRequestModel(string endpointId, ulong? continuationToken = null)
        {
            EndpointId = endpointId;
            ContinuationToken = continuationToken;
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string EndpointId { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public ulong? ContinuationToken { get; set; }
    }

    /// <summary>
    /// Model for configured endpoint response element.
    /// </summary>
    public class ConfiguredEndpointModel
    {
        public ConfiguredEndpointModel(string endpointId, string endpointName, string endpointUrl)
        {
            EndpointId = endpointId;
            EndpointName = endpointName;
            EndpointUrl = endpointUrl;
        }

        public string EndpointId { get; set; }

        public string EndpointName { get; set; }
        public string EndpointUrl { get; set; }
    }

    /// <summary>
    /// Model for a get configured endpoints response.
    /// </summary>
    public class GetConfiguredEndpointsMethodResponseModel
    {
        public GetConfiguredEndpointsMethodResponseModel()
        {
            Endpoints = new List<ConfiguredEndpointModel>();
        }

        public GetConfiguredEndpointsMethodResponseModel(List<ConfiguredEndpointModel> endpoints)
        {
            Endpoints = endpoints;
        }
        public List<ConfiguredEndpointModel> Endpoints { get; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public ulong? ContinuationToken { get; set; }
    }

    /// <summary>
    /// Model for a get configured nodes on endpoint request.
    /// </summary>
    public class GetConfiguredNodesOnEndpointMethodRequestModel
    {
        public GetConfiguredNodesOnEndpointMethodRequestModel(string endpointId, ulong? continuationToken = null)
        {
            EndpointId = endpointId;
            ContinuationToken = continuationToken;
        }
        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string EndpointId { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public ulong? ContinuationToken { get; set; }
    }

    /// <summary>
    /// Model class for a get configured nodes on endpoint response.
    /// </summary>
    public class GetConfiguredNodesOnEndpointMethodResponseModel
    {
        public GetConfiguredNodesOnEndpointMethodResponseModel()
        {
            OpcNodes = new List<OpcNodeOnEndpointModel>();
        }

        /// <param name="nodes"></param>
        public GetConfiguredNodesOnEndpointMethodResponseModel(List<OpcNodeOnEndpointModel> nodes)
        {
            OpcNodes = nodes;
        }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public string EndpointId { get; set; }

        [JsonProperty(NullValueHandling = NullValueHandling.Include)]
        public List<OpcNodeOnEndpointModel> OpcNodes { get; }

        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public ulong? ContinuationToken { get; set; }
    }

    /// <summary>
    /// Model class for a get message properties response.
    /// </summary>
    public class GetMessagePropertiesReponseModel
    {
        /// <summary>
        /// Gets or sets the message properties.
        /// </summary>
        public List<MessageProperty> Items { get; set; } = new List<MessageProperty>();

        public GetMessagePropertiesReponseModel()
        {            
        }

        public GetMessagePropertiesReponseModel(IEnumerable<MessageProperty> items)
        {
            Items = items.ToList();
        }
    }

    /// <summary>
    /// Model class for a message property.
    /// </summary>
    public class MessageProperty
    {
        /// <summary>
        /// Gets or sets the name of the message property.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the text that is displayed when selecting a message property.
        /// </summary>
        public string DisplayText { get; set; }

        /// <summary>
        /// Gets or sets the message property values.
        /// </summary>
        public List<MessagePropertyValue> Values { get; set; }

        public MessageProperty()
        {
        }

        public MessageProperty(string displayText, string name)
        {
            DisplayText = displayText;
            Name = name;
        }

        public MessageProperty(string displayText, string name, IEnumerable<MessagePropertyValue> values) : this(displayText, name)
        {
            Values = values.ToList();
        }
    }

    /// <summary>
    /// Model class for a message property value.
    /// </summary>
    public class MessagePropertyValue
    {
        /// <summary>
        /// Gets or sets the value of the message property.
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Gets or sets the text that is displayed when selecting a message property value.
        /// </summary>
        public string DisplayText { get; set; }

        public MessagePropertyValue()
        {
        }

        public MessagePropertyValue(string displayText, string value)
        {
            DisplayText = displayText;
            Value = value;
        }
    }
}
