﻿namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Newtonsoft.Json;
    using Opc.Ua;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Collections.Concurrent;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using OpcPublisher.AIT;
    using OpcPublisher.Crypto;
    using static OpcApplicationConfiguration;
    using static OpcMonitoredItem;
    using static Program;

    /// <summary>
    /// Class to handle all IoTHub/EdgeHub communication.
    /// </summary>
    public partial class HubCommunicationBase : IHubCommunication, IDisposable
    {
        private static Regex nodeKeyValidation = new Regex("[^.$#\\s]+");

        /// <summary>
        /// Specifies the queue capacity for monitored item events.
        /// </summary>
        public static int MonitoredItemsQueueCapacity { get; set; } = 8192;

        /// <summary>
        /// Number of events in the monitored items queue.
        /// </summary>
        public static long MonitoredItemsQueueCount => _monitoredItemsDataQueue == null ? 0 : _monitoredItemsDataQueue.Count;

        /// <summary>
        /// Number of events we enqueued.
        /// </summary>
        public static long EnqueueCount => _enqueueCount;

        /// <summary>
        /// Number of times enqueueing of events failed.
        /// </summary>
        public static long EnqueueFailureCount => _enqueueFailureCount;

        /// <summary>
        /// Specifies max message size in byte for hub communication allowed.
        /// </summary>
        public const uint HubMessageSizeMax = 256 * 1024;

        /// <summary>
        /// Specifies the message size in bytes used for hub communication.
        /// </summary>
        public uint HubMessageSize { get; set; } = HubMessageSizeDefault;

        public static uint HubMessageSizeDefault { get; set; } = HubMessageSizeMax;

        /// <summary>
        /// Specifies the send interval in seconds after which a message is sent to the hub.
        /// </summary>
        public int SendIntervalSeconds { get; set; } = SendIntervalSecondsDefault;

        public static int SendIntervalSecondsDefault { get; set; } = 10;

        /// <summary>
        /// Number of DataChange notification events sent to the cloud.
        /// </summary>
        public static long NumberOfDataChangeEvents { get; set; }

        /// <summary>
        /// Number of Events notification sent to the cloud.
        /// </summary>
        public static long NumberOfEvents { get; set; }

        /// <summary>
        /// Number of times we were not able to make the send interval, because too high load.
        /// </summary>
        public static long MissedSendIntervalCount { get; set; }

        /// <summary>
        /// Number of times the isze fo the event payload was too large for a telemetry message.
        /// </summary>
        public static long TooLargeCount { get; set; }

        /// <summary>
        /// Number of payload bytes we sent to the cloud.
        /// </summary>
        public static long SentBytes { get; set; }

        /// <summary>
        /// Number of messages we sent to the cloud.
        /// </summary>
        public static long SentMessages { get; set; }

        /// <summary>
        /// Time when we sent the last telemetry message.
        /// </summary>
        public static DateTime SentLastTime { get; set; }

        /// <summary>
        /// Number of times we were not able to sent the telemetry message to the cloud.
        /// </summary>
        public static long FailedMessages { get; set; }

        /// <summary>
        /// Allow to ingest data into IoT Central.
        /// </summary>
        public static bool IotCentralMode { get; set; } = false;

        /// <summary>
        /// Max allowed payload of an IoTHub direct method call response.
        /// </summary>
        public static int MaxResponsePayloadLength { get; } = 128 * 1024 - 256;

        /// <summary>
        /// The protocol to use for hub communication.
        /// </summary>
        public const TransportType IotHubProtocolDefault = TransportType.Mqtt_WebSocket_Only;
        public const TransportType IotEdgeHubProtocolDefault = TransportType.Amqp_Tcp_Only;
        public static TransportType HubProtocol { get; set; } = IotHubProtocolDefault;

        /// <summary>
        /// Dictionary of available IoTHub direct methods.
        /// </summary>
        public Dictionary<string, MethodCallback> IotHubDirectMethods { get; } = new Dictionary<string, MethodCallback>();

        /// <summary>
        /// Check if transport type to use is HTTP.
        /// </summary>
        bool IsHttp1Transport() => HubProtocol == TransportType.Http1;

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubCommunicationBase()
        {
            _hubCommunicationCts = new CancellationTokenSource();
            _shutdownToken = _hubCommunicationCts.Token;
            IotHubDirectMethods.Add("PublishNodes", HandlePublishNodesMethodAsync);
            IotHubDirectMethods.Add("PublishEvents", HandlePublishEventsMethodAsync);
            IotHubDirectMethods.Add("UnpublishNodes", HandleUnpublishNodesMethodAsync);
            IotHubDirectMethods.Add("UnublishEvents", HandleUnpublishEventsMethodAsync);
            IotHubDirectMethods.Add("UnpublishAllNodes", HandleUnpublishAllNodesMethodAsync);
            IotHubDirectMethods.Add("GetConfiguredEndpoints", HandleGetConfiguredEndpointsMethodAsync);
            IotHubDirectMethods.Add("DeleteConfiguredEndpoint", HandleDeleteConfiguredEndpointMethodAsync);
            IotHubDirectMethods.Add("GetConfiguredNodesOnEndpoint", HandleGetConfiguredNodesOnEndpointMethodAsync);
            IotHubDirectMethods.Add("GetConfiguredEventNodesOnEndpoint", HandleGetConfiguredEventsOnEndpointMethodAsync);
            IotHubDirectMethods.Add("GetOpcPublishedConfigurationAsJson", HandleGetOpcPublishedConfigurationAsJson);
            IotHubDirectMethods.Add("SaveOpcPublishedConfigurationAsJson", HandleSaveOpcPublishedConfigurationAsJson);
            IotHubDirectMethods.Add("GetDiagnosticInfo", HandleGetDiagnosticInfoMethodAsync);
            IotHubDirectMethods.Add("GetDiagnosticLog", HandleGetDiagnosticLogMethodAsync);
            IotHubDirectMethods.Add("GetDiagnosticStartupLog", HandleGetDiagnosticStartupLogMethodAsync);
            IotHubDirectMethods.Add("ExitApplication", HandleExitApplicationMethodAsync);
            IotHubDirectMethods.Add("GetInfo", HandleGetInfoMethodAsync);
            IotHubDirectMethods.Add("GetMessageProperties", HandleGetMessagePropertiesMethodAsync);            
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // send cancellation token and wait for last IoT Hub message to be sent.
                _hubCommunicationCts?.Cancel();
                try
                {
                    _monitoredItemsDataQueue = null;
                    _iotcEventsProcessor?.Dispose();

                    _monitoredItemsProcessorTask = null;
                    _hubClient?.Dispose();
                    _hubClient = null;
                }
                catch (Exception e)
                {
                    Logger.Error(e, "Failure while shutting down hub messaging.");
                }
                _hubCommunicationCts?.Dispose();
                _hubCommunicationCts = null;
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
        /// Initializes edge message broker communication.
        /// </summary>
        public async Task<bool> InitHubCommunicationAsync(IHubClient hubClient, bool registerMethodHandlers, bool listenMessages)
        {
            try
            {
                // set hub communication parameters
                _hubClient = hubClient;
                ExponentialBackoff exponentialRetryPolicy = new ExponentialBackoff(int.MaxValue, TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(1024), TimeSpan.FromMilliseconds(3));

                // show IoTCentral mode
                Logger.Information($"IoTCentral mode: {IotCentralMode}");

                _hubClient.ProductInfo = "OpcPublisher";
                _hubClient.SetRetryPolicy(exponentialRetryPolicy);
                // register connection status change handler
                _hubClient.SetConnectionStatusChangesHandler(ConnectionStatusChange);

                // open connection
                Logger.Debug($"Open hub communication");
                await _hubClient.OpenAsync().ConfigureAwait(false);

                // init twin properties and method callbacks (not supported for HTTP)
                // todo check if this is required at all
                if (!IsHttp1Transport())
                {
                    if (registerMethodHandlers)
                    {
                        // init twin properties and method callbacks
                        Logger.Debug($"Register desired properties and method callbacks");
                        // register method handlers
                        foreach (var iotHubMethod in IotHubDirectMethods)
                        {
                            await _hubClient.SetMethodHandlerAsync(iotHubMethod.Key, iotHubMethod.Value)
                                .ConfigureAwait(false);
                        }

                        await _hubClient.SetMethodDefaultHandlerAsync(_hubClient.DefaultCommandHandlerAsync).ConfigureAwait(false);
                    }
                }

                if (!listenMessages) return true;
                Logger.Debug($"Init D2C message processing");
                return await InitMessageProcessingAsync().ConfigureAwait(false);

            }
            catch (Exception e)
            {
                Logger.Error(e, "Failure initializing hub communication processing.");
                return false;
            }
        }

        /// <summary>
        /// Handle connection status change notifications.
        /// </summary>
        public void ConnectionStatusChange(ConnectionStatus status, ConnectionStatusChangeReason reason)
        {
            if (reason == ConnectionStatusChangeReason.Connection_Ok || ShutdownTokenSource.IsCancellationRequested)
            {
                Logger.Information($"Connection status changed to '{status}', reason '{reason}'");
            }
            else
            {
                Logger.Error($"Connection status changed to '{status}', reason '{reason}'");
            }
        }

        /// <summary>
        /// Handle publish node method call.
        /// </summary>
        public virtual async Task<MethodResponse> HandlePublishNodesMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandlePublishNodesMethodAsync:";
            bool useSecurity = true;
            Guid endpointId = Guid.Empty;
            string endpointName = null;
            Uri endpointUri = null;
            IOpcSession opcSession = null;

            OpcAuthenticationMode? desiredAuthenticationMode = null;
            EncryptedNetworkCredential desiredEncryptedCredential = null;

            PublishNodesMethodRequestModel publishNodesMethodData = null;
            PublishNodesMethodResponseModel publishedNodesMethodResponse = null;
            HttpStatusCode statusCode = HttpStatusCode.OK;
            HttpStatusCode nodeStatusCode = HttpStatusCode.InternalServerError;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;
            try
            {
                Logger.Debug($"{logPrefix} called");
                publishNodesMethodData = JsonConvert.DeserializeObject<PublishNodesMethodRequestModel>(methodRequest.DataAsJson);
                endpointId = publishNodesMethodData.EndpointId == null ? Guid.Empty : new Guid(publishNodesMethodData.EndpointId);
                endpointName = publishNodesMethodData.EndpointName;
                endpointUri = new Uri(publishNodesMethodData.EndpointUrl);
                useSecurity = publishNodesMethodData.UseSecurity;

                if (publishNodesMethodData.OpcAuthenticationMode == OpcAuthenticationMode.UsernamePassword)
                {
                    if (string.IsNullOrWhiteSpace(publishNodesMethodData.UserName) && string.IsNullOrWhiteSpace(publishNodesMethodData.Password))
                    {
                        throw new ArgumentException($"If {nameof(publishNodesMethodData.OpcAuthenticationMode)} is set to '{OpcAuthenticationMode.UsernamePassword}', you have to specify '{nameof(publishNodesMethodData.UserName)}' and/or '{nameof(publishNodesMethodData.Password)}'.");
                    }

                    desiredAuthenticationMode = OpcAuthenticationMode.UsernamePassword;
                    desiredEncryptedCredential = await EncryptedNetworkCredential.FromPlainCredential(publishNodesMethodData.UserName, publishNodesMethodData.Password);
                }
            }
            catch (UriFormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointUrl '{publishNodesMethodData.EndpointUrl}'";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.NotAcceptable;
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{publishNodesMethodData.EndpointId}'";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.NotAcceptable;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            // unpublish all removed nodes
            // update means we unpublish and publish again
            var nodesToRemoveOrUpdate = publishNodesMethodData.OpcNodes.Where(n =>
                n.OpcPublisherPublishState == OpcPublisherPublishState.Remove || n.OpcPublisherPublishState == OpcPublisherPublishState.Update);
                
            if (statusCode == HttpStatusCode.OK && nodesToRemoveOrUpdate.Any())
            {
                var unpublishStatusResponse = new List<string>();
                (statusCode, unpublishStatusResponse) = await UnpublishNodesAsync(endpointId, nodesToRemoveOrUpdate).ConfigureAwait(false);
                statusResponse.AddRange(unpublishStatusResponse);
            }

            // process all nodes                        
            var nodesToAddOrUpdate = publishNodesMethodData.OpcNodes.Where(n =>
                n.OpcPublisherPublishState == OpcPublisherPublishState.Add || n.OpcPublisherPublishState == OpcPublisherPublishState.Update);

            if (statusCode == HttpStatusCode.OK && nodesToAddOrUpdate.Any())
            {
                // find/create a session to the endpoint URL and start monitoring the node.
                try
                {
                    // lock the publishing configuration till we are done
                    await NodeConfiguration.OpcSessionsListSemaphore.WaitAsync().ConfigureAwait(false);

                    if (ShutdownTokenSource.IsCancellationRequested)
                    {
                        statusMessage = $"Publisher is in shutdown";
                        Logger.Warning($"{logPrefix} {statusMessage}");
                        statusResponse.Add(statusMessage);
                        statusCode = HttpStatusCode.Gone;
                    }
                    else
                    {
                        /* we create new sessions in two cases
                            1. For new endpoints
                            2. For existing endpoints which do not have a OpcSession configured: 
                               this happens if for an existing endpoint all monitored items, commands and events are removed (unused sessions are removed). 
                        */
                        var isNewEndpoint = endpointId == Guid.Empty;
                        var isExistingEndpointWithoutSession = !isNewEndpoint && NodeConfiguration.OpcSessions.FirstOrDefault(s => s.EndpointId.Equals(endpointId)) == null;
                        if (isNewEndpoint || isExistingEndpointWithoutSession)
                        {
                            // if the no OpcAuthenticationMode is specified, we create the new session with "Anonymous" auth
                            if (!desiredAuthenticationMode.HasValue)
                            {
                                desiredAuthenticationMode = OpcAuthenticationMode.Anonymous;
                            }

                            if (isNewEndpoint)
                            {
                                endpointId = Guid.NewGuid();
                            }
                            // create new session info
                            opcSession = new OpcSession(endpointId, endpointName, endpointUri.OriginalString, useSecurity, OpcSessionCreationTimeout, desiredAuthenticationMode.Value, desiredEncryptedCredential);
                            NodeConfiguration.OpcSessions.Add(opcSession);
                            Logger.Information($"{logPrefix} No matching session found for endpoint '{endpointUri.OriginalString}'. Requested to create a new one.");
                        }
                        else
                        {
                            // find the session we need to monitor the node
                            opcSession = NodeConfiguration.OpcSessions.FirstOrDefault(s => s.EndpointId.Equals(endpointId));
                            // a session already exists, so we check, if we need to change authentication settings. This is only true, if the payload contains an OpcAuthenticationMode-Property
                            if (desiredAuthenticationMode.HasValue)
                            {
                                bool reconnectRequired = false;

                                if (opcSession.OpcAuthenticationMode != desiredAuthenticationMode.Value)
                                {
                                    opcSession.OpcAuthenticationMode = desiredAuthenticationMode.Value;
                                    reconnectRequired = true;
                                }

                                if (opcSession.EncryptedAuthCredential != desiredEncryptedCredential)
                                {
                                    opcSession.EncryptedAuthCredential = desiredEncryptedCredential;
                                    reconnectRequired = true;
                                }

                                if (reconnectRequired)
                                {
                                    await opcSession.Reconnect().ConfigureAwait(false);
                                }
                            }
                        }

                        if (statusCode != HttpStatusCode.InternalServerError)
                        {
                            foreach (var node in nodesToAddOrUpdate)
                            {
                                // support legacy format
                                if (string.IsNullOrEmpty(node.Id) && !string.IsNullOrEmpty(node.ExpandedNodeId))
                                {
                                    node.Id = node.ExpandedNodeId;
                                }

                                if (HasDuplicateKey(endpointId, node.Key))
                                {
                                    statusMessage = $"'{node.Id}' has duplicate key '{node.Key}'!";
                                    Logger.Error($"{logPrefix} {statusMessage}");
                                    statusResponse.Add(statusMessage);
                                    statusCode = HttpStatusCode.BadRequest;
                                    continue;
                                }

                                if (node.Key.Length > 128 || !nodeKeyValidation.IsMatch(node.Key))
                                {
                                    statusMessage = $"'{node.Id}' with key '{node.Key}' is either too long or has invalid characters!";
                                    Logger.Error($"{logPrefix} {statusMessage}");
                                    statusResponse.Add(statusMessage);
                                    statusCode = HttpStatusCode.BadRequest;
                                    continue;
                                }

                                NodeId nodeId = null;
                                ExpandedNodeId expandedNodeId = null;
                                bool isNodeIdFormat = true;
                                try
                                {
                                    if (node.Id.Contains("nsu=", StringComparison.InvariantCulture))
                                    {
                                        expandedNodeId = ExpandedNodeId.Parse(node.Id);
                                        isNodeIdFormat = false;
                                    }
                                    else
                                    {
                                        nodeId = NodeId.Parse(node.Id);
                                        isNodeIdFormat = true;
                                    }
                                }
                                catch (Exception e)
                                {
                                    statusMessage = $"Exception ({e.Message}) while formatting node '{node.Key}'!";
                                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                                    statusResponse.Add(statusMessage);
                                    statusCode = HttpStatusCode.NotAcceptable;
                                    continue;
                                }

                                try
                                {
                                    if (isNodeIdFormat)
                                    {
                                        // add the node info to the subscription with the default publishing interval, execute syncronously
                                        Logger.Debug($"{logPrefix} Request to monitor item with NodeId '{node.Id}' with key '{node.Key}' (PublishingInterval: {node.OpcPublishingInterval.ToString() ?? "--"}, SamplingInterval: {node.OpcSamplingInterval.ToString() ?? "--"})");
                                        nodeStatusCode = await opcSession.AddNodeForMonitoringAsync(nodeId, null,
                                            node.OpcPublishingInterval, node.OpcSamplingInterval, node.Key, node.DisplayName,
                                            node.HeartbeatInterval, node.SkipFirst,
                                            ShutdownTokenSource.Token, node.IotCentralItemPublishMode).ConfigureAwait(false);
                                    }
                                    else
                                    {
                                        // add the node info to the subscription with the default publishing interval, execute syncronously
                                        Logger.Debug($"{logPrefix} Request to monitor item with ExpandedNodeId '{node.Id}' with key '{node.Key}' (PublishingInterval: {node.OpcPublishingInterval.ToString() ?? "--"}, SamplingInterval: {node.OpcSamplingInterval.ToString() ?? "--"})");
                                        nodeStatusCode = await opcSession.AddNodeForMonitoringAsync(null, expandedNodeId,
                                            node.OpcPublishingInterval, node.OpcSamplingInterval, node.Key, node.DisplayName,
                                            node.HeartbeatInterval, node.SkipFirst,
                                            ShutdownTokenSource.Token, node.IotCentralItemPublishMode).ConfigureAwait(false);
                                    }

                                    // check and store a result message in case of an error
                                    switch (nodeStatusCode)
                                    {
                                        case HttpStatusCode.OK:
                                            statusMessage = $"'{node.Key}': already monitored";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            break;

                                        case HttpStatusCode.Accepted:
                                            statusMessage = $"'{node.Key}': added";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            break;

                                        case HttpStatusCode.Gone:
                                            statusMessage = $"'{node.Key}': session to endpoint does not exist anymore";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            statusCode = HttpStatusCode.Gone;
                                            break;

                                        case HttpStatusCode.InternalServerError:
                                            statusMessage = $"'{node.Key}': error while trying to configure";
                                            Logger.Debug($"{logPrefix} {statusMessage}");
                                            statusResponse.Add(statusMessage);
                                            statusCode = HttpStatusCode.InternalServerError;
                                            break;
                                    }
                                }
                                catch (Exception e)
                                {
                                    statusMessage = $"Exception ({e.Message}) while trying to configure publishing node '{node.Id}' with key '{node.Key}'";
                                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                                    statusResponse.Add(statusMessage);
                                    statusCode = HttpStatusCode.InternalServerError;
                                }
                            }
                        }

                        publishedNodesMethodResponse = new PublishNodesMethodResponseModel(endpointId.ToString());
                    }
                }
                catch (AggregateException e)
                {
                    foreach (Exception ex in e.InnerExceptions)
                    {
                        Logger.Error(ex, $"{logPrefix} Exception");
                    }
                    statusMessage = $"EndpointUrl: '{publishNodesMethodData.EndpointUrl}': exception ({e.Message}) while trying to publish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                catch (Exception e)
                {
                    statusMessage = $"EndpointUrl: '{publishNodesMethodData.EndpointUrl}': exception ({e.Message}) while trying to publish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                finally
                {
                    NodeConfiguration.OpcSessionsListSemaphore.Release();
                }
                
                // wait until the session is saved
                if (opcSession != null)
                {
                    await opcSession.ConnectAndMonitorAsync().ConfigureAwait(false);
                }
            }

            // build response
            string resultString = statusCode == HttpStatusCode.OK ?
                    JsonConvert.SerializeObject(publishedNodesMethodResponse) :
                    JsonConvert.SerializeObject(statusResponse);
            byte[] result = Encoding.UTF8.GetBytes(resultString);

            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }

            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        private bool HasDuplicateKey(Guid endpointId, string key) => NodeConfiguration.OpcSessions
            .Where(session => session.EndpointId.Equals(endpointId))
            .Any(session => session.OpcSubscriptions.Concat(session.OpcEventSubscriptions).Any(subscription => subscription.OpcMonitoredItems.Any(
                    item => string.Compare(item.Key, key, CultureInfo.InvariantCulture, CompareOptions.OrdinalIgnoreCase) == 0)));

        /// <summary>
        /// Handle unpublish node method call.
        /// </summary>
        public virtual async Task<MethodResponse> HandleUnpublishNodesMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleUnpublishNodesMethodAsync:";
            Guid endpointId = Guid.Empty;
            UnpublishNodesMethodRequestModel unpublishNodesMethodData = null;
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;
            try
            {
                Logger.Debug($"{logPrefix} called");
                unpublishNodesMethodData = JsonConvert.DeserializeObject<UnpublishNodesMethodRequestModel>(methodRequest.DataAsJson);
                endpointId = new Guid(unpublishNodesMethodData.EndpointId);
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{unpublishNodesMethodData.EndpointId}'";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            var unpublishStatusResponse = new List<string>();
            (statusCode, unpublishStatusResponse) = await UnpublishNodesAsync(endpointId, unpublishNodesMethodData.OpcNodes).ConfigureAwait(false);
            statusResponse.AddRange(unpublishStatusResponse);

            // adjust response size
            AdjustResponse(ref statusResponse);

            // build response
            string resultString = JsonConvert.SerializeObject(statusResponse);
            byte[] result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        private async Task<(HttpStatusCode statusCode, List<string> statusResponse)> UnpublishNodesAsync(Guid endpointId, IEnumerable<OpcNodeOnEndpointModel> opcNodes)
        {
            string logPrefix = "UnpublishNodesAsync:";
            NodeId nodeId = null;
            IOpcSession opcSession = null;
            ExpandedNodeId expandedNodeId = null;
            bool isNodeIdFormat = true;
            HttpStatusCode nodeStatusCode = HttpStatusCode.InternalServerError;
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            if (statusCode == HttpStatusCode.OK)
            {
                try
                {
                    await NodeConfiguration.OpcSessionsListSemaphore.WaitAsync().ConfigureAwait(false);
                    if (ShutdownTokenSource.IsCancellationRequested)
                    {
                        statusMessage = $"Publisher is in shutdown";
                        Logger.Error($"{logPrefix} {statusMessage}");
                        statusResponse.Add(statusMessage);
                        statusCode = HttpStatusCode.Gone;
                    }
                    else
                    {
                        // find the session we need to monitor the node
                        try
                        {
                            opcSession = NodeConfiguration.OpcSessions.FirstOrDefault(s => s.EndpointId.Equals(endpointId));
                        }
                        catch
                        {
                            opcSession = null;
                        }

                        if (opcSession == null)
                        {
                            // do nothing if there is no session for this endpoint.
                            statusMessage = $"Session for endpoint '{endpointId.ToString()}' not found.";
                            Logger.Error($"{logPrefix} {statusMessage}");
                            statusResponse.Add(statusMessage);
                            statusCode = HttpStatusCode.Gone;
                        }
                        else
                        {

                            // unpublish all nodes on one endpoint or nodes requested
                            if (opcNodes != null && opcNodes.Any())
                            {
                                foreach (var node in opcNodes)
                                {
                                    // support legacy format
                                    if (string.IsNullOrEmpty(node.Id) && !string.IsNullOrEmpty(node.ExpandedNodeId))
                                    {
                                        node.Id = node.ExpandedNodeId;
                                    }

                                    try
                                    {
                                        if (node.Id.Contains("nsu=", StringComparison.InvariantCulture))
                                        {
                                            expandedNodeId = ExpandedNodeId.Parse(node.Id);
                                            isNodeIdFormat = false;
                                        }
                                        else
                                        {
                                            nodeId = NodeId.Parse(node.Id);
                                            isNodeIdFormat = true;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        statusMessage = $"Exception ({e.Message}) while formatting node '{node.Id}' with key '{node.Key}'!";
                                        Logger.Error(e, $"{logPrefix} {statusMessage}");
                                        statusResponse.Add(statusMessage);
                                        statusCode = HttpStatusCode.NotAcceptable;
                                        continue;
                                    }

                                    try
                                    {
                                        if (isNodeIdFormat)
                                        {
                                            // stop monitoring the node, execute synchronously
                                            Logger.Information($"{logPrefix} Request to stop monitoring item with NodeId '{nodeId.ToString()}' and key '{node.Key}'");
                                            nodeStatusCode = await opcSession.RequestMonitorItemRemovalAsync(nodeId, null, ShutdownTokenSource.Token).ConfigureAwait(false);
                                        }
                                        else
                                        {
                                            // stop monitoring the node, execute synchronously
                                            Logger.Information($"{logPrefix} Request to stop monitoring item with ExpandedNodeId '{expandedNodeId.ToString()} and key '{node.Key}'");
                                            nodeStatusCode = await opcSession.RequestMonitorItemRemovalAsync(null, expandedNodeId, ShutdownTokenSource.Token).ConfigureAwait(false);
                                        }

                                        // check and store a result message in case of an error
                                        switch (nodeStatusCode)
                                        {
                                            case HttpStatusCode.OK:
                                                statusMessage = $"Key '{node.Key}': was not configured";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                break;

                                            case HttpStatusCode.Accepted:
                                                statusMessage = $"Key '{node.Key}': tagged for removal";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                break;

                                            case HttpStatusCode.Gone:
                                                statusMessage = $"Key '{node.Key}': session to endpoint does not exist anymore";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                statusCode = HttpStatusCode.Gone;
                                                break;

                                            case HttpStatusCode.InternalServerError:
                                                statusMessage = $"Key '{node.Key}': error while trying to remove";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                statusCode = HttpStatusCode.InternalServerError;
                                                break;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        statusMessage = $"Exception ({e.Message}) while trying to tag node '{node.Key}' for removal";
                                        Logger.Error(e, $"{logPrefix} {statusMessage}");
                                        statusResponse.Add(statusMessage);
                                        statusCode = HttpStatusCode.InternalServerError;
                                    }
                                }
                            }
                        }
                    }
                }
                catch (AggregateException e)
                {
                    foreach (Exception ex in e.InnerExceptions)
                    {
                        Logger.Error(ex, $"{logPrefix} Exception");
                    }
                    statusMessage = $"EndpointUrl: '{endpointId}': exception while trying to unpublish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                catch (Exception e)
                {
                    statusMessage = $"EndpointUrl: '{endpointId}': exception ({e.Message}) while trying to unpublish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                finally
                {
                    NodeConfiguration.OpcSessionsListSemaphore.Release();
                }
            }

            // wait until the session is saved
            if (opcSession != null)
            {
                await opcSession.ConnectAndMonitorAsync().ConfigureAwait(false);
            }

            return (statusCode, statusResponse);
        }

        /// <summary>
        /// Handle unpublish all nodes method call.
        /// </summary>
        public virtual async Task<MethodResponse> HandleUnpublishAllNodesMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleUnpublishAllNodesMethodAsync:";
            Guid endpointId = Guid.Empty;
            UnpublishAllNodesMethodRequestModel unpublishAllNodesMethodData = null;
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            try
            {
                Logger.Debug($"{logPrefix} called");
                if (!string.IsNullOrEmpty(methodRequest.DataAsJson))
                {
                    unpublishAllNodesMethodData = JsonConvert.DeserializeObject<UnpublishAllNodesMethodRequestModel>(methodRequest.DataAsJson);
                }
                if (unpublishAllNodesMethodData != null && unpublishAllNodesMethodData?.EndpointId != null)
                {
                    endpointId = new Guid(unpublishAllNodesMethodData.EndpointId);
                }
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{unpublishAllNodesMethodData.EndpointId}'";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            if (statusCode == HttpStatusCode.OK)
            {
                // schedule to remove all nodes on all sessions
                try
                {
                    await NodeConfiguration.OpcSessionsListSemaphore.WaitAsync().ConfigureAwait(false);
                    if (ShutdownTokenSource.IsCancellationRequested)
                    {
                        statusMessage = $"Publisher is in shutdown";
                        Logger.Error($"{logPrefix} {statusMessage}");
                        statusResponse.Add(statusMessage);
                        statusCode = HttpStatusCode.Gone;
                    }
                    else
                    {
                        // loop through all sessions
                        foreach (var session in NodeConfiguration.OpcSessions)
                        {
                            bool sessionLocked = false;
                            try
                            {
                                // is an endpoint was given, limit unpublish to this endpoint
                                if (endpointId != Guid.Empty && !endpointId.Equals(session.EndpointId))
                                {
                                    continue;
                                }

                                sessionLocked = await session.LockSessionAsync().ConfigureAwait(false);
                                if (!sessionLocked || ShutdownTokenSource.IsCancellationRequested)
                                {
                                    break;
                                }

                                // loop through all subscriptions of a connected session
                                foreach (var subscription in session.OpcSubscriptions)
                                {
                                    // loop through all monitored items
                                    foreach (var monitoredItem in subscription.OpcMonitoredItems)
                                    {
                                        if (monitoredItem.ConfigType == OpcMonitoredItemConfigurationType.NodeId)
                                        {
                                            await session.RequestMonitorItemRemovalAsync(monitoredItem.ConfigNodeId, null, ShutdownTokenSource.Token, false).ConfigureAwait(false);
                                        }
                                        else
                                        {
                                            await session.RequestMonitorItemRemovalAsync(null, monitoredItem.ConfigExpandedNodeId, ShutdownTokenSource.Token, false).ConfigureAwait(false);
                                        }
                                    }
                                }
                            }
                            finally
                            {
                                if (sessionLocked)
                                {
                                    session.ReleaseSession();
                                }
                            }
                        }
                        // build response
                        statusMessage = $"All monitored items in all subscriptions{(endpointId != Guid.Empty ? $" on endpoint '{endpointId.ToString()}'" : " ")} tagged for removal";
                        statusResponse.Add(statusMessage);
                        Logger.Information($"{logPrefix} {statusMessage}");
                    }
                }
                catch (Exception e)
                {
                    statusMessage = $"EndpointId: '{unpublishAllNodesMethodData?.EndpointId}': exception ({e.Message}) while trying to unpublish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                finally
                {
                    NodeConfiguration.OpcSessionsListSemaphore.Release();
                }
            }

            // adjust response size to available package size and keep proper json syntax
            byte[] result;
            int maxIndex = statusResponse.Count();
            string resultString = string.Empty;
            while (true)
            {
                resultString = JsonConvert.SerializeObject(statusResponse.GetRange(0, maxIndex));
                result = Encoding.UTF8.GetBytes(resultString);
                if (result.Length > MaxResponsePayloadLength)
                {
                    maxIndex /= 2;
                    continue;
                }
                else
                {
                    break;
                }
            }
            if (maxIndex != statusResponse.Count())
            {
                statusResponse.RemoveRange(maxIndex, statusResponse.Count() - maxIndex);
                statusResponse.Add("Results have been cropped due to package size limitations.");
            }

            // build response
            resultString = JsonConvert.SerializeObject(statusResponse);
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        /// <summary>
        /// Handle method call to get all endpoints which published nodes.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetConfiguredEndpointsMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleGetConfiguredEndpointsMethodAsync:";
            GetConfiguredEndpointsMethodRequestModel getConfiguredEndpointsMethodRequest = null;
            GetConfiguredEndpointsMethodResponseModel getConfiguredEndpointsMethodResponse = new GetConfiguredEndpointsMethodResponseModel();
            uint actualEndpointsCount = 0;
            uint availableEndpointCount = 0;
            uint nodeConfigVersion = 0;
            uint startIndex = 0;
            List<ConfiguredEndpointModel> endpoints = new List<ConfiguredEndpointModel>();
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            try
            {
                Logger.Debug($"{logPrefix} called");
                if (!string.IsNullOrEmpty(methodRequest.DataAsJson))
                {
                    getConfiguredEndpointsMethodRequest = JsonConvert.DeserializeObject<GetConfiguredEndpointsMethodRequestModel>(methodRequest.DataAsJson);
                }
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            if (statusCode == HttpStatusCode.OK)
            {
                // get the list of all endpoints
                endpoints = NodeConfiguration.GetPublisherConfigurationFileEntries(Guid.Empty, false, out nodeConfigVersion)
                    .Select(e => new ConfiguredEndpointModel(e.EndpointId.ToString(), e.EndpointName, e.EndpointUrl.OriginalString))
                    .ToList();
                uint endpointsCount = (uint)endpoints.Count;

                // validate version
                if (getConfiguredEndpointsMethodRequest?.ContinuationToken != null)
                {
                    uint requestedNodeConfigVersion = (uint)(getConfiguredEndpointsMethodRequest.ContinuationToken >> 32);
                    if (nodeConfigVersion != requestedNodeConfigVersion)
                    {
                        statusMessage = $"The node configuration has changed between calls. Requested version: {requestedNodeConfigVersion:X8}, Current version '{nodeConfigVersion:X8}'";
                        Logger.Information($"{logPrefix} {statusMessage}");
                        statusResponse.Add(statusMessage);
                        statusCode = HttpStatusCode.Gone;
                    }
                    startIndex = (uint)(getConfiguredEndpointsMethodRequest.ContinuationToken & 0x0FFFFFFFFL);
                }

                if (statusCode == HttpStatusCode.OK)
                {
                    // set count
                    uint requestedEndpointsCount = endpointsCount - startIndex;
                    availableEndpointCount = endpointsCount - startIndex;
                    actualEndpointsCount = Math.Min(requestedEndpointsCount, availableEndpointCount);

                    // generate response
                    string endpointsString;
                    byte[] endpointsByteArray;
                    while (true)
                    {
                        endpointsString = JsonConvert.SerializeObject(endpoints.GetRange((int)startIndex, (int)actualEndpointsCount));
                        endpointsByteArray = Encoding.UTF8.GetBytes(endpointsString);
                        if (endpointsByteArray.Length > MaxResponsePayloadLength)
                        {
                            actualEndpointsCount /= 2;
                            continue;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }

            // build response
            byte[] result = null;
            string resultString = null;
            if (statusCode == HttpStatusCode.OK)
            {
                getConfiguredEndpointsMethodResponse.ContinuationToken = null;
                if (actualEndpointsCount < availableEndpointCount)
                {
                    getConfiguredEndpointsMethodResponse.ContinuationToken = ((ulong)nodeConfigVersion << 32) | actualEndpointsCount + startIndex;
                }
                getConfiguredEndpointsMethodResponse.Endpoints.AddRange(endpoints.GetRange((int)startIndex, (int)actualEndpointsCount));
                resultString = JsonConvert.SerializeObject(getConfiguredEndpointsMethodResponse);
                result = Encoding.UTF8.GetBytes(resultString);
                Logger.Information($"{logPrefix} returning {actualEndpointsCount} endpoint(s) (node config version: {nodeConfigVersion:X8})!");
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }

            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }

        /// <summary>
        /// Handle method call to get list of configured nodes on a specific endpoint.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetConfiguredNodesOnEndpointMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleGetConfiguredNodesOnEndpointMethodAsync:";
            Guid endpointId = Guid.Empty;
            GetConfiguredNodesOnEndpointMethodRequestModel getConfiguredNodesOnEndpointMethodRequest = null;
            uint nodeConfigVersion = 0;
            GetConfiguredNodesOnEndpointMethodResponseModel getConfiguredNodesOnEndpointMethodResponse = new GetConfiguredNodesOnEndpointMethodResponseModel();
            uint actualNodeCount = 0;
            uint availableNodeCount = 0;
            uint requestedNodeCount = 0;
            List<OpcNodeOnEndpointModel> opcNodes = new List<OpcNodeOnEndpointModel>();
            uint startIndex = 0;
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            try
            {
                Logger.Debug($"{logPrefix} called");
                getConfiguredNodesOnEndpointMethodRequest = JsonConvert.DeserializeObject<GetConfiguredNodesOnEndpointMethodRequestModel>(methodRequest.DataAsJson);
                if (getConfiguredNodesOnEndpointMethodRequest.EndpointId == null)
                {
                    statusMessage = $"New endpoint: there are no nodes configured";
                    Logger.Information($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.NotFound;
                }
                else
                {
                    endpointId = new Guid(getConfiguredNodesOnEndpointMethodRequest.EndpointId);
                }
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{getConfiguredNodesOnEndpointMethodRequest.EndpointId}'";
                Logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            if (statusCode == HttpStatusCode.OK)
            {
                // get the list of published nodes for the endpoint
                List<PublisherConfigurationFileEntryModel> configFileEntries = NodeConfiguration.GetPublisherConfigurationFileEntries(endpointId, false, out nodeConfigVersion);

                // return if there are no nodes configured for this endpoint
                if (configFileEntries.Count == 0)
                {
                    statusMessage = $"There are no nodes configured for endpoint '{endpointId.ToString()}'";
                    Logger.Information($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.NotFound;
                }
                else
                {
                    foreach (var configFileEntry in configFileEntries)
                    {
                        if (configFileEntry.OpcNodes != null)
                        {
                            opcNodes.AddRange(configFileEntry.OpcNodes);
                        }
                    }
                    uint configuredNodesOnEndpointCount = (uint)opcNodes.Count();

                    // validate version
                    startIndex = 0;
                    if (getConfiguredNodesOnEndpointMethodRequest?.ContinuationToken != null)
                    {
                        uint requestedNodeConfigVersion = (uint)(getConfiguredNodesOnEndpointMethodRequest.ContinuationToken >> 32);
                        if (nodeConfigVersion != requestedNodeConfigVersion)
                        {
                            statusMessage = $"The node configuration has changed between calls. Requested version: {requestedNodeConfigVersion:X8}, Current version '{nodeConfigVersion:X8}'!";
                            Logger.Information($"{logPrefix} {statusMessage}");
                            statusResponse.Add(statusMessage);
                            statusCode = HttpStatusCode.Gone;
                        }
                        startIndex = (uint)(getConfiguredNodesOnEndpointMethodRequest.ContinuationToken & 0x0FFFFFFFFL);
                    }

                    if (statusCode == HttpStatusCode.OK)
                    {
                        // set count
                        requestedNodeCount = configuredNodesOnEndpointCount - startIndex;
                        availableNodeCount = configuredNodesOnEndpointCount - startIndex;
                        actualNodeCount = Math.Min(requestedNodeCount, availableNodeCount);

                        // generate response
                        string publishedNodesString;
                        byte[] publishedNodesByteArray;
                        while (true)
                        {
                            publishedNodesString = JsonConvert.SerializeObject(opcNodes.GetRange((int)startIndex, (int)actualNodeCount));
                            publishedNodesByteArray = Encoding.UTF8.GetBytes(publishedNodesString);
                            if (publishedNodesByteArray.Length > MaxResponsePayloadLength)
                            {
                                actualNodeCount /= 2;
                                continue;
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                }
            }

            // build response
            byte[] result = null;
            string resultString = null;
            if (statusCode == HttpStatusCode.OK)
            {
                getConfiguredNodesOnEndpointMethodResponse.ContinuationToken = null;
                if (actualNodeCount < availableNodeCount)
                {
                    getConfiguredNodesOnEndpointMethodResponse.ContinuationToken = (ulong)nodeConfigVersion << 32 | actualNodeCount + startIndex;
                }
                opcNodes.ForEach(x => x.OpcPublisherPublishState = OpcPublisherPublishState.Published);
                getConfiguredNodesOnEndpointMethodResponse.OpcNodes.AddRange(opcNodes.GetRange((int)startIndex, (int)actualNodeCount).Select(n => new OpcNodeOnEndpointModel(n.Id)
                {
                    OpcPublishingInterval = n.OpcPublishingInterval,
                    OpcSamplingInterval = n.OpcSamplingInterval,
                    Key = n.Key,
                    Id = n.Id,
                    DisplayName = n.DisplayName,
                    IotCentralItemPublishMode = n.IotCentralItemPublishMode,
                    OpcPublisherPublishState = n.OpcPublisherPublishState
                }).ToList());
                getConfiguredNodesOnEndpointMethodResponse.EndpointId = endpointId.ToString();
                resultString = JsonConvert.SerializeObject(getConfiguredNodesOnEndpointMethodResponse);
                Logger.Information($"{logPrefix} Success returning {actualNodeCount} node(s) of {availableNodeCount} (start: {startIndex}) (node config version: {nodeConfigVersion:X8})!");
            }
            else if (statusCode == HttpStatusCode.NotFound)
            {
                resultString = JsonConvert.SerializeObject(getConfiguredNodesOnEndpointMethodResponse);
                Logger.Information($"{logPrefix} Success returning 0 nodes.");
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }

        /// <summary>
        /// Handle method call to get diagnostic information.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetDiagnosticInfoMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleGetDiagnosticInfoMethodAsync:";
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            // get the diagnostic info
            DiagnosticInfoMethodResponseModel diagnosticInfo = new DiagnosticInfoMethodResponseModel();
            try
            {
                diagnosticInfo = Diag.GetDiagnosticInfo();
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while reading diagnostic info";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            // build response
            byte[] result = null;
            string resultString = null;
            if (statusCode == HttpStatusCode.OK)
            {
                resultString = JsonConvert.SerializeObject(diagnosticInfo);
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }

        /// <summary>
        /// Handle method call to get log information.
        /// </summary>
        public virtual async Task<MethodResponse> HandleGetDiagnosticLogMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleGetDiagnosticLogMethodAsync:";
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            // get the diagnostic info
            DiagnosticLogMethodResponseModel diagnosticLogMethodResponseModel = new DiagnosticLogMethodResponseModel();
            try
            {
                diagnosticLogMethodResponseModel = await Diag.GetDiagnosticLogAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while reading diagnostic log";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            // build response
            byte[] result = null;
            string resultString = null;
            if (statusCode == HttpStatusCode.OK)
            {
                resultString = JsonConvert.SerializeObject(diagnosticLogMethodResponseModel);
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        /// <summary>
        /// Handle method call to get log information.
        /// </summary>
        public virtual async Task<MethodResponse> HandleGetDiagnosticStartupLogMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleGetDiagnosticStartupLogMethodAsync:";
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            // get the diagnostic info
            DiagnosticLogMethodResponseModel diagnosticLogMethodResponseModel = new DiagnosticLogMethodResponseModel();
            try
            {
                diagnosticLogMethodResponseModel = await Diag.GetDiagnosticStartupLogAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while reading diagnostic startup log";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            // build response
            byte[] result = null;
            string resultString = null;
            if (statusCode == HttpStatusCode.OK)
            {
                resultString = JsonConvert.SerializeObject(diagnosticLogMethodResponseModel);
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        /// <summary>
        /// Handle method call to get log information.
        /// </summary>
        public virtual Task<MethodResponse> HandleExitApplicationMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleExitApplicationMethodAsync:";
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            ExitApplicationMethodRequestModel exitApplicationMethodRequest = null;
            try
            {
                if (!string.IsNullOrEmpty(methodRequest.DataAsJson))
                {
                    exitApplicationMethodRequest = JsonConvert.DeserializeObject<ExitApplicationMethodRequestModel>(methodRequest.DataAsJson);
                }
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            if (statusCode == HttpStatusCode.OK)
            {
                // get the parameter
                ExitApplicationMethodRequestModel exitApplication = new ExitApplicationMethodRequestModel();
                try
                {
                    int secondsTillExit = exitApplicationMethodRequest != null ? exitApplicationMethodRequest.SecondsTillExit : 5;
                    secondsTillExit = secondsTillExit < 5 ? 5 : secondsTillExit;
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    Task.Run(async () => await ExitApplicationAsync(secondsTillExit).ConfigureAwait(false));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    statusMessage = $"Module will exit now...";
                    Logger.Information($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                }
                catch (Exception e)
                {
                    statusMessage = $"Exception ({e.Message}) while scheduling application exit";
                    Logger.Error(e, $"{logPrefix} Exception");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
            }

            // build response
            byte[] result = null;
            string resultString = null;
            resultString = JsonConvert.SerializeObject(statusResponse);
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }

        /// <summary>
        /// Handle method call to get application information.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetInfoMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleGetInfoMethodAsync:";
            GetInfoMethodResponseModel getInfoMethodResponseModel = new GetInfoMethodResponseModel();
            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            try
            {
                // get the info
                getInfoMethodResponseModel.VersionMajor = Assembly.GetExecutingAssembly().GetName().Version.Major;
                getInfoMethodResponseModel.VersionMinor = Assembly.GetExecutingAssembly().GetName().Version.Minor;
                getInfoMethodResponseModel.VersionPatch = Assembly.GetExecutingAssembly().GetName().Version.Build;
                getInfoMethodResponseModel.SemanticVersion = (Attribute.GetCustomAttribute(Assembly.GetEntryAssembly(), typeof(AssemblyInformationalVersionAttribute)) as AssemblyInformationalVersionAttribute).InformationalVersion;
                getInfoMethodResponseModel.InformationalVersion = (Attribute.GetCustomAttribute(Assembly.GetEntryAssembly(), typeof(AssemblyInformationalVersionAttribute)) as AssemblyInformationalVersionAttribute).InformationalVersion;
                getInfoMethodResponseModel.OS = RuntimeInformation.OSDescription;
                getInfoMethodResponseModel.OSArchitecture = RuntimeInformation.OSArchitecture;
                getInfoMethodResponseModel.FrameworkDescription = RuntimeInformation.FrameworkDescription;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while retrieving info";
                Logger.Error(e, $"{logPrefix} Exception");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            // build response
            byte[] result = null;
            string resultString = null;
            if (statusCode == HttpStatusCode.OK)
            {
                resultString = JsonConvert.SerializeObject(getInfoMethodResponseModel);
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }

        /// <summary>
        /// Method that is called for any unimplemented call. Just returns that info to the caller
        /// </summary>
        public virtual Task<MethodResponse> DefaultMethodHandlerAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "DefaultMethodHandlerAsync:";
            string errorMessage = $"Method '{methodRequest.Name}' successfully received, but this method is not implemented";
            Logger.Information($"{logPrefix} {errorMessage}");

            string resultString = JsonConvert.SerializeObject(errorMessage);
            byte[] result = Encoding.UTF8.GetBytes(resultString);
            MethodResponse methodResponse = new MethodResponse(result, (int)HttpStatusCode.NotImplemented);
            return Task.FromResult(methodResponse);
        }

        /// <summary>
        /// Initializes internal message processing.
        /// </summary>
        private Task<bool> InitMessageProcessingAsync()
        {
            try
            {
                _logger = Logger;

                // show config
                Logger.Information($"Message processing and hub communication configured with a send interval of {SendIntervalSecondsDefault} sec and a message buffer size of {HubMessageSize} bytes.");

                // create the queue for monitored items
                _monitoredItemsDataQueue = new BlockingCollection<MessageData>(MonitoredItemsQueueCapacity);

                Logger.Information("Creating task process and batch monitored item data updates...");

                _monitoredItemsProcessorTask = Task.Factory.StartNew(async () => await MonitoredItemsProcessorAsync().ConfigureAwait(false),
                    _shutdownToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                _iotcEventsProcessor = new IoTCEventsProcessor(_logger, _hubClient, HubMessageSizeDefault, HubMessageSizeMax, SendIntervalSecondsDefault, _shutdownToken);

                _monitoredEventsProcessorTask = Task.Factory.StartNew(async () => await _iotcEventsProcessor.MonitoredIoTCEventsProcessorAsync().ConfigureAwait(false),
                    _shutdownToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                _settingsProcessor = new SettingsProcessor(_logger, _hubClient, SendIntervalSecondsDefault, _shutdownToken);

                _monitoredSettingsProcessorTask = Task.Factory.StartNew(async () => await _settingsProcessor.MonitoredSettingsProcessorAsync().ConfigureAwait(false),
                    _shutdownToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                _propertiesProcessor = new PropertiesProcessor(_logger, _hubClient, SendIntervalSecondsDefault, _shutdownToken);

                _monitoredPropertiesProcessorTask = Task.Factory.StartNew(async () => await _propertiesProcessor.MonitoredPropertiesProcessorAsync().ConfigureAwait(false),
                    _shutdownToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                return Task.FromResult(true);
            }
            catch (Exception e)
            {
                Logger.Error(e, "Failure initializing message processing.");
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// EnqueueProperty a message for sending to IoTHub.
        /// </summary>
        public virtual void Enqueue(MessageData json)
        {
            // Try to add the message.
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredItemsDataQueue.TryAdd(json) == false)
            {
                Interlocked.Increment(ref _enqueueFailureCount);
                if (_enqueueFailureCount % 10000 == 0)
                {
                    Logger.Information($"The internal monitored item message queue is above its capacity of {_monitoredItemsDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
                }
            }
        }

        /// <summary>
        /// Creates a IoTHub JSON message for a data change notification, based on the telemetry configuration for the endpoint.
        /// </summary>
        private async Task<string> CreateJsonForDataChangeAsync(DataChangeMessageData messageData)
        {
            try
            {
                // since the router relies on a fixed message format, we dont allow per-endpoint configuration and use the default for all endpoints
                var telemetryConfiguration = TelemetryConfiguration.DefaultEndpointTelemetryConfiguration;

                // currently the pattern processing is done in MonitoredItemNotificationHandler of OpcSession.cs. in case of perf issues
                // it can be also done here, the risk is then to lose messages in the communication queue. if you enable it here, disable it in OpcSession.cs
                // messageData.ApplyPatterns(telemetryConfiguration);

                // build the JSON message
                StringBuilder _jsonStringBuilder = new StringBuilder();
                StringWriter _jsonStringWriter = new StringWriter(_jsonStringBuilder);
                using (JsonWriter _jsonWriter = new JsonTextWriter(_jsonStringWriter))
                {
                    await _jsonWriter.WriteStartObjectAsync().ConfigureAwait(false);
                    string telemetryValue = string.Empty;

                    // process EndpointUrl
                    if ((bool)telemetryConfiguration.EndpointUrl.Publish)
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.EndpointUrl.Name).ConfigureAwait(false);
                        await _jsonWriter.WriteValueAsync(messageData.EndpointUrl).ConfigureAwait(false);
                    }

                    // process NodeId
                    if (!string.IsNullOrEmpty(messageData.NodeId))
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.NodeId.Name).ConfigureAwait(false);
                        await _jsonWriter.WriteValueAsync(messageData.NodeId).ConfigureAwait(false);
                    }

                    if (!string.IsNullOrEmpty(messageData.ExpandedNodeId))
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.ExpandedNodeId.Name).ConfigureAwait(false);
                        await _jsonWriter.WriteValueAsync(messageData.ExpandedNodeId).ConfigureAwait(false);
                    }

                    // process MonitoredItem object properties
                    if (!string.IsNullOrEmpty(messageData.ApplicationUri) || !string.IsNullOrEmpty(messageData.DisplayName) || !string.IsNullOrEmpty(messageData.Key))
                    {
                        if (!(bool)telemetryConfiguration.MonitoredItem.Flat)
                        {
                            await _jsonWriter.WritePropertyNameAsync("MonitoredItem").ConfigureAwait(false);
                            await _jsonWriter.WriteStartObjectAsync().ConfigureAwait(false);
                        }

                        // process ApplicationUri
                        if (!string.IsNullOrEmpty(messageData.ApplicationUri))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.ApplicationUri.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.ApplicationUri).ConfigureAwait(false);
                        }

                        // process DisplayName
                        if (!string.IsNullOrEmpty(messageData.DisplayName))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.DisplayName.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.DisplayName).ConfigureAwait(false);
                        }

                        // process Key
                        if (!string.IsNullOrEmpty(messageData.Key))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.Key.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.Key).ConfigureAwait(false);
                        }

                        if (!(bool)telemetryConfiguration.MonitoredItem.Flat)
                        {
                            await _jsonWriter.WriteEndObjectAsync().ConfigureAwait(false);
                        }
                    }

                    // process Value object properties
                    if (!string.IsNullOrEmpty(messageData.Value) || !string.IsNullOrEmpty(messageData.SourceTimestamp) ||
                       messageData.StatusCode != null || !string.IsNullOrEmpty(messageData.Status))
                    {
                        if (!(bool)telemetryConfiguration.Value.Flat)
                        {
                            await _jsonWriter.WritePropertyNameAsync("Value").ConfigureAwait(false);
                            await _jsonWriter.WriteStartObjectAsync().ConfigureAwait(false);
                        }

                        // process Value
                        if (!string.IsNullOrEmpty(messageData.Value))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.Value.Name).ConfigureAwait(false);
                            if (messageData.PreserveValueQuotes)
                            {
                                await _jsonWriter.WriteValueAsync(messageData.Value).ConfigureAwait(false);
                            }
                            else
                            {
                                await _jsonWriter.WriteRawValueAsync(messageData.Value).ConfigureAwait(false);
                            }
                        }

                        // process SourceTimestamp
                        if (!string.IsNullOrEmpty(messageData.SourceTimestamp))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.SourceTimestamp.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.SourceTimestamp).ConfigureAwait(false);
                        }

                        // process ReceiveTimestamp
                        if (!string.IsNullOrEmpty(messageData.ReceiveTimestamp))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.ReceiveTimestamp.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.ReceiveTimestamp).ConfigureAwait(false);
                        }

                        // process StatusCode
                        if (messageData.StatusCode != null)
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.StatusCode.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.StatusCode).ConfigureAwait(false);
                        }

                        // process Status
                        if (!string.IsNullOrEmpty(messageData.Status))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.Status.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(messageData.Status).ConfigureAwait(false);
                        }

                        if (!(bool)telemetryConfiguration.Value.Flat)
                        {
                            await _jsonWriter.WriteEndObjectAsync().ConfigureAwait(false);
                        }
                    }

                    await _jsonWriter.WriteEndObjectAsync().ConfigureAwait(false);
                    await _jsonWriter.FlushAsync().ConfigureAwait(false);
                }
                return _jsonStringBuilder.ToString();
            }
            catch (Exception e)
            {
                Logger.Error(e, "Generation of JSON message failed.");
            }
            return string.Empty;
        }

        /// <summary>
        /// Creates a IoTHub JSON message for an event notification, based on the telemetry configuration for the endpoint.
        /// </summary>
        private async Task<string> CreateJsonForEventAsync(EventMessageData eventData)
        {
            try
            {
                // get telemetry configration
                EndpointTelemetryConfigurationModel telemetryConfiguration = TelemetryConfiguration.GetEndpointTelemetryConfiguration(eventData.EndpointUrl);

                // currently the pattern processing is done in MonitoredItemNotificationHandler of OpcSession.cs. in case of perf issues
                // it can be also done here, the risk is then to lose messages in the communication queue. if you enable it here, disable it in OpcSession.cs
                // messageData.ApplyPatterns(telemetryConfiguration);

                // build the JSON message
                StringBuilder _jsonStringBuilder = new StringBuilder();
                StringWriter _jsonStringWriter = new StringWriter(_jsonStringBuilder);
                using (JsonWriter _jsonWriter = new JsonTextWriter(_jsonStringWriter))
                {
                    await _jsonWriter.WriteStartObjectAsync().ConfigureAwait(false);
                    string telemetryValue = string.Empty;

                    // process EndpointUrl
                    if ((bool)telemetryConfiguration.EndpointUrl.Publish)
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.EndpointUrl.Name).ConfigureAwait(false);
                        await _jsonWriter.WriteValueAsync(eventData.EndpointUrl).ConfigureAwait(false);
                    }

                    // process NodeId
                    if (!string.IsNullOrEmpty(eventData.NodeId))
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.NodeId.Name).ConfigureAwait(false);
                        await _jsonWriter.WriteValueAsync(eventData.NodeId).ConfigureAwait(false);
                    }

                    // process MonitoredItem object properties
                    if (!string.IsNullOrEmpty(eventData.ApplicationUri) || !string.IsNullOrEmpty(eventData.DisplayName) || !string.IsNullOrEmpty(eventData.Key))
                    {
                        if (!(bool)telemetryConfiguration.MonitoredItem.Flat)
                        {
                            await _jsonWriter.WritePropertyNameAsync("MonitoredItem").ConfigureAwait(false);
                            await _jsonWriter.WriteStartObjectAsync().ConfigureAwait(false);
                        }

                        // process ApplicationUri
                        if (!string.IsNullOrEmpty(eventData.ApplicationUri))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.ApplicationUri.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(eventData.ApplicationUri).ConfigureAwait(false);
                        }

                        // process DisplayName
                        if (!string.IsNullOrEmpty(eventData.DisplayName))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.DisplayName.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(eventData.DisplayName).ConfigureAwait(false);
                        }

                        // process Key
                        if (!string.IsNullOrEmpty(eventData.Key))
                        {
                            await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.MonitoredItem.Key.Name).ConfigureAwait(false);
                            await _jsonWriter.WriteValueAsync(eventData.Key).ConfigureAwait(false);
                        }

                        if (!(bool)telemetryConfiguration.MonitoredItem.Flat)
                        {
                            await _jsonWriter.WriteEndObjectAsync().ConfigureAwait(false);
                        }
                    }

                    // process EventValues object properties
                    if (eventData.EventValues != null && eventData.EventValues.Count > 0)
                    {
                        foreach (var eventValue in eventData.EventValues)
                        {
                            await _jsonWriter.WritePropertyNameAsync(eventValue.Name).ConfigureAwait(false);
                            
                            if (eventValue.Value is string stringValue)
                            {
                                await _jsonWriter.WriteRawValueAsync(stringValue?.Replace(",", ".")).ConfigureAwait(false);
                            }
                            else
                            {
                                await _jsonWriter.WriteRawValueAsync(eventValue.Value?.ToString()?.Replace(",", ".")).ConfigureAwait(false);
                            }
                        }
                    }

                    // process PublishTime
                    if (!string.IsNullOrEmpty(eventData.PublishTime))
                    {
                        await _jsonWriter.WritePropertyNameAsync(telemetryConfiguration.Value.PublishTime.Name).ConfigureAwait(false);
                        await _jsonWriter.WriteValueAsync(eventData.PublishTime).ConfigureAwait(false);
                    }

                    await _jsonWriter.WriteEndObjectAsync().ConfigureAwait(false);
                    await _jsonWriter.FlushAsync().ConfigureAwait(false);
                }
                return _jsonStringBuilder.ToString();
            }
            catch (Exception e)
            {
                Logger.Error(e, "Generation of JSON message failed.");
            }
            return string.Empty;
        }

        /// <summary>
        /// Dequeue monitored item notification messages, batch them for send (if needed) and send them to IoTHub.
        /// </summary>
        public virtual async Task MonitoredItemsProcessorAsync()
        {
            uint jsonSquareBracketLength = 2;
            Message tempMsg = new Message();
            // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
            int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString(CultureInfo.InvariantCulture).Length;
            int applicationPropertyLength = Encoding.UTF8.GetByteCount($"iothub-content-type={CONTENT_TYPE_OPCUAJSON}") + Encoding.UTF8.GetByteCount($"iothub-content-encoding={CONTENT_ENCODING_UTF8}");
            // if batching is requested the buffer will have the requested size, otherwise we reserve the max size
            uint hubMessageBufferSize = (HubMessageSize > 0 ? HubMessageSize : HubMessageSizeMax) - (uint)systemPropertyLength - jsonSquareBracketLength - (uint)applicationPropertyLength;
            byte[] hubMessageBuffer = new byte[hubMessageBufferSize];
            MemoryStream hubMessage = new MemoryStream(hubMessageBuffer);
            DateTime nextSendTime = DateTime.UtcNow + TimeSpan.FromSeconds(SendIntervalSeconds);
            double millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
            bool singleMessageSend = SendIntervalSeconds == 0 && HubMessageSize == 0;

            using (hubMessage)
            {
                try
                {
                    string jsonMessage = string.Empty;
                    bool needToBufferMessage = false;
                    int jsonMessageSize = 0;
                    EventMessageData eventMessageData = null;
                    DataChangeMessageData dataChangeMessageData = null;

                    hubMessage.Position = 0;
                    hubMessage.SetLength(0);
                    if (!singleMessageSend)
                    {
                        hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                    }

                    while (!_shutdownToken.IsCancellationRequested)
                    {
                        // sanity check the send interval, compute the timeout and get the next monitored item message
                        if (SendIntervalSeconds > 0)
                        {
                            millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                            if (millisecondsTillNextSend < 0)
                            {
                                MissedSendIntervalCount++;
                                // do not wait if we missed the send interval
                                millisecondsTillNextSend = 0;
                            }
                        }
                        else
                        {
                            // if we are in shutdown do not wait, else wait infinite if send interval is not set
                            millisecondsTillNextSend = _shutdownToken.IsCancellationRequested ? 0 : Timeout.Infinite;
                        }

                        bool gotItem = _monitoredItemsDataQueue.TryTake(out MessageData messageData, (int)millisecondsTillNextSend, _shutdownToken);

                        // the two commandline parameter --ms (message size) and --si (send interval) control when data is sent to IoTHub/EdgeHub
                        // pls see detailed comments on performance and memory consumption at https://github.com/Azure/iot-edge-opc-publisher

                        // check if we got an item or if we hit the timeout or got canceled
                        if (gotItem)
                        {
                            dataChangeMessageData = messageData?.DataChangeMessageData;
                            eventMessageData = messageData?.EventMessageData;

                            // create a JSON message from notification data
                            if (dataChangeMessageData != null)
                            {
                                NumberOfDataChangeEvents++;
                                jsonMessage = await CreateJsonForDataChangeAsync(dataChangeMessageData).ConfigureAwait(false);
                            }
                            else if (eventMessageData != null)
                            {
                                NumberOfEvents++;
                                jsonMessage = await CreateJsonForEventAsync(eventMessageData).ConfigureAwait(false);
                            }

                            jsonMessageSize = Encoding.UTF8.GetByteCount(jsonMessage.ToString(CultureInfo.InvariantCulture));

                            // sanity check that the user has set a large enough messages size
                            if ((HubMessageSize > 0 && jsonMessageSize > HubMessageSize) || (HubMessageSize == 0 && jsonMessageSize > hubMessageBufferSize))
                            {
                                Logger.Error($"There is a telemetry message (size: {jsonMessageSize}), which will not fit into an hub message (max size: {hubMessageBufferSize}].");
                                Logger.Error($"Please check your hub message size settings. The telemetry message will be discarded silently. Sorry:(");
                                TooLargeCount++;
                                continue;
                            }

                            // if batching is requested or we need to send at intervals, batch it otherwise send it right away
                            needToBufferMessage = false;
                            if (HubMessageSize > 0 || (HubMessageSize == 0 && SendIntervalSeconds > 0))
                            {
                                // if there is still space to batch, do it. otherwise send the buffer and flag the message for later buffering
                                if (hubMessage.Position + jsonMessageSize + 1 <= hubMessage.Capacity)
                                {
                                    // add the message and a comma to the buffer
                                    hubMessage.Write(Encoding.UTF8.GetBytes(jsonMessage.ToString(CultureInfo.InvariantCulture)), 0, jsonMessageSize);
                                    hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                    Logger.Debug($"Added new message with size {jsonMessageSize} to hub message (size is now {(hubMessage.Position - 1)}).");
                                    continue;
                                }
                                else
                                {
                                    needToBufferMessage = true;
                                }
                            }
                        }
                        else
                        {
                            // if we got no message, we either reached the interval or we are in shutdown and have processed all messages
                            if (_shutdownToken.IsCancellationRequested)
                            {
                                Logger.Information($"Cancellation requested.");
                                _monitoredItemsDataQueue.CompleteAdding();
                                _monitoredItemsDataQueue.Dispose();
                                break;
                            }
                        }

                        // the batching is completed or we reached the send interval or got a cancelation request
                        try
                        {
                            Microsoft.Azure.Devices.Client.Message encodedhubMessage = null;

                            // if we reached the send interval, but have nothing to send (only the opening square bracket is there), we continue
                            if (!gotItem && hubMessage.Position == 1)
                            {
                                nextSendTime += TimeSpan.FromSeconds(SendIntervalSeconds);
                                hubMessage.Position = 0;
                                hubMessage.SetLength(0);
                                if (!singleMessageSend)
                                {
                                    hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                                }
                                continue;
                            }

                            // if there is no batching and no send interval configured, we send the JSON message we just got, otherwise we send the buffer
                            if (singleMessageSend)
                            {
                                // create the message without brackets
                                encodedhubMessage = new Message(Encoding.UTF8.GetBytes(jsonMessage.ToString(CultureInfo.InvariantCulture)));
                            }
                            else
                            {
                                // remove the trailing comma and add a closing square bracket
                                hubMessage.SetLength(hubMessage.Length - 1);
                                hubMessage.Write(Encoding.UTF8.GetBytes("]"), 0, 1);
                                encodedhubMessage = new Message(hubMessage.ToArray());
                            }
                            if (_hubClient != null)
                            {
                                encodedhubMessage.ContentType = CONTENT_TYPE_OPCUAJSON;
                                encodedhubMessage.ContentEncoding = CONTENT_ENCODING_UTF8;

                                nextSendTime += TimeSpan.FromSeconds(SendIntervalSeconds);
                                try
                                {
                                    encodedhubMessage.Properties["endpointId"] = dataChangeMessageData?.EndpointId ?? eventMessageData?.EndpointId;
                                    SentBytes += encodedhubMessage.GetBytes().Length;
                                    await _hubClient.SendEventAsync(encodedhubMessage).ConfigureAwait(false);
                                    SentMessages++;
                                    SentLastTime = DateTime.UtcNow;
                                    Logger.Debug($"Sending {encodedhubMessage.BodyStream.Length} bytes to hub.");
                                    Logger.Debug($"Message sent was: {jsonMessage}");
                                }
                                catch
                                {
                                    FailedMessages++;
                                }

                                // reset the messaage
                                hubMessage.Position = 0;
                                hubMessage.SetLength(0);
                                if (!singleMessageSend)
                                {
                                    hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                                }

                                // if we had not yet buffered the last message because there was not enough space, buffer it now
                                if (needToBufferMessage)
                                {
                                    // add the message and a comma to the buffer
                                    hubMessage.Write(Encoding.UTF8.GetBytes(jsonMessage.ToString(CultureInfo.InvariantCulture)), 0, jsonMessageSize);
                                    hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                }
                            }
                            else
                            {
                                Logger.Information("No hub client available. Dropping messages...");
                            }
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, "Exception while sending message to hub. Dropping message...");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!(e is OperationCanceledException))
                    {
                        Logger.Error(e, "Error while processing monitored item messages.");
                    }
                }
            }
        }

        /// <summary>
        /// Exit the application.
        /// </summary>
        public virtual async Task ExitApplicationAsync(int secondsTillExit)
        {
            string logPrefix = "ExitApplicationAsync:";

            // sanity check parameter
            if (secondsTillExit <= 0)
            {
                Logger.Information($"{logPrefix} Time to exit adjusted to {secondsTillExit} seconds...");
                secondsTillExit = 5;
            }

            // wait and exit
            while (secondsTillExit > 0)
            {
                Logger.Information($"{logPrefix} Exiting in {secondsTillExit} seconds...");
                secondsTillExit--;
                await Task.Delay(1000).ConfigureAwait(false);
            }

            // exit
            Environment.Exit(2);
        }

        /// <summary>
        /// Adjust the method response to the max payload size.
        /// </summary>
        private static void AdjustResponse(ref List<string> statusResponse)
        {
            byte[] result;
            int maxIndex = statusResponse.Count();
            string resultString = string.Empty;
            while (true)
            {
                resultString = JsonConvert.SerializeObject(statusResponse.GetRange(0, maxIndex));
                result = Encoding.UTF8.GetBytes(resultString);
                if (result.Length > MaxResponsePayloadLength)
                {
                    maxIndex /= 2;
                    continue;
                }
                else
                {
                    break;
                }
            }
            if (maxIndex != statusResponse.Count())
            {
                statusResponse.RemoveRange(maxIndex, statusResponse.Count() - maxIndex);
                statusResponse.Add("Results have been cropped due to package size limitations.");
            }
        }

        private const string CONTENT_TYPE_OPCUAJSON = "application/opcua+uajson";
        private const string CONTENT_ENCODING_UTF8 = "UTF-8";

        private static long _enqueueCount;
        private static long _enqueueFailureCount;
        private IoTCEventsProcessor _iotcEventsProcessor;
        private Task _monitoredEventsProcessorTask;
        private static SettingsProcessor _settingsProcessor;
        private Task _monitoredSettingsProcessorTask;
        private static PropertiesProcessor _propertiesProcessor;
        private Task _monitoredPropertiesProcessorTask;
        private static BlockingCollection<MessageData> _monitoredItemsDataQueue = null;
        private static Task _monitoredItemsProcessorTask;
        private IHubClient _hubClient;
        private CancellationTokenSource _hubCommunicationCts;
        private CancellationToken _shutdownToken;
    }
}