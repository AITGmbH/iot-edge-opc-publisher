﻿using Serilog.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using OpcPublisher.AIT;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Opc.Ua;
using OpcPublisher.Crypto;

namespace OpcPublisher
{
    using static OpcApplicationConfiguration;
    using static Program;

    public partial class HubCommunicationBase
    {
        public void EnqueueProperty(MessageData message) => _propertiesProcessor.EnqueueProperty(message);

        public void EnqueueEvent(MessageData message) => _iotcEventsProcessor.EnqueueEvent(message);

        public void EnqueueSetting(MessageData message) => _settingsProcessor.EnqueueSetting(message);

        /// <summary>
        /// Handle get message properties method call.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetMessagePropertiesMethodAsync(MethodRequest methodRequest, object userContext)
        {
            var response = new GetMessagePropertiesReponseModel();

            var endpoints = NodeConfiguration.GetPublisherConfigurationFileEntries(Guid.Empty, false, out _)
                .Select(e => new MessagePropertyValue(e.EndpointName, e.EndpointId.ToString()));
            var endpointMessageProperty = new MessageProperty("OPC UA endpoint", "endpointId", endpoints);
            response.Items.Add(endpointMessageProperty);
            
            string resultString = JsonConvert.SerializeObject(response);
            byte[] result = Encoding.UTF8.GetBytes(resultString);
            return Task.FromResult(new MethodResponse(result, (int)HttpStatusCode.OK));
        }

        /// <summary>
        /// Handle publish event node method call.
        /// </summary>
        public virtual async Task<MethodResponse> HandlePublishEventsMethodAsync(MethodRequest methodRequest, object userContext)
        {
            var logPrefix = "HandlePublishEventsMethodAsync:";
            var useSecurity = true;
            Guid endpointId = Guid.Empty;
            string endpointName = null;
            Uri endpointUri = null;
            IOpcSession opcSession = null;

            OpcAuthenticationMode? desiredAuthenticationMode = null;
            EncryptedNetworkCredential desiredEncryptedCredential = null;

            PublishNodesMethodRequestModel publishEventsMethodData = null;
            PublishNodesMethodResponseModel publishedEventMethodResponse = null;
            var statusCode = HttpStatusCode.OK;
            var statusResponse = new List<string>();
            string statusMessage;
            try
            {
                _logger.Debug($"{logPrefix} called");
                publishEventsMethodData = JsonConvert.DeserializeObject<PublishNodesMethodRequestModel>(methodRequest.DataAsJson);
                endpointId = publishEventsMethodData.EndpointId == null ? Guid.Empty : new Guid(publishEventsMethodData.EndpointId);
                endpointName = publishEventsMethodData.EndpointName;
                endpointUri = new Uri(publishEventsMethodData.EndpointUrl);
                useSecurity = publishEventsMethodData.UseSecurity;

                if (publishEventsMethodData.OpcAuthenticationMode == OpcAuthenticationMode.UsernamePassword)
                {
                    if (string.IsNullOrWhiteSpace(publishEventsMethodData.UserName) && string.IsNullOrWhiteSpace(publishEventsMethodData.Password))
                    {
                        throw new ArgumentException($"If {nameof(publishEventsMethodData.OpcAuthenticationMode)} is set to '{OpcAuthenticationMode.UsernamePassword}', you have to specify '{nameof(publishEventsMethodData.UserName)}' and/or '{nameof(publishEventsMethodData.Password)}'.");
                    }

                    desiredAuthenticationMode = OpcAuthenticationMode.UsernamePassword;
                    desiredEncryptedCredential = await EncryptedNetworkCredential.FromPlainCredential(publishEventsMethodData.UserName, publishEventsMethodData.Password);
                }
            }
            catch (UriFormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointUrl '{publishEventsMethodData?.EndpointUrl}'";
                _logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.NotAcceptable;
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{publishEventsMethodData?.EndpointId}'";
                _logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.NotAcceptable;
            }
            catch (Exception e)
            {
                statusMessage = $"Exception ({e.Message}) while deserializing message payload";
                _logger.Error(e, $"{logPrefix} {statusMessage}");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.InternalServerError;
            }

            // unpublish all removed events
            // update means we unpublish and publish again
            var eventsToRemoveOrUpdate = publishEventsMethodData.OpcEvents.Where(n =>
                n.OpcPublisherPublishState == OpcPublisherPublishState.Remove || n.OpcPublisherPublishState == OpcPublisherPublishState.Update);
            if (statusCode == HttpStatusCode.OK && eventsToRemoveOrUpdate.Any())
            {
                var unpublishStatusResponse = new List<string>();
                (statusCode, unpublishStatusResponse) = await UnpublishEventsAsync(endpointId, eventsToRemoveOrUpdate).ConfigureAwait(false);
                statusResponse.AddRange(unpublishStatusResponse);
            }

            // process all nodes
            var eventsToAddOrUpdate = publishEventsMethodData.OpcEvents.Where(n =>
                n.OpcPublisherPublishState == OpcPublisherPublishState.Add || n.OpcPublisherPublishState == OpcPublisherPublishState.Update);

            if (statusCode == HttpStatusCode.OK && eventsToAddOrUpdate.Any())
            {
                // find/create a session to the endpoint URL and start monitoring the node.
                try
                {
                    // lock the publishing configuration till we are done
                    await NodeConfiguration.OpcSessionsListSemaphore.WaitAsync(_shutdownToken).ConfigureAwait(false);

                    if (ShutdownTokenSource.IsCancellationRequested)
                    {
                        statusMessage = $"Publisher is in shutdown";
                        _logger.Warning($"{logPrefix} {statusMessage}");
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
                            // create new session info.
                            opcSession = new OpcSession(endpointId, endpointName, endpointUri.OriginalString, useSecurity, OpcSessionCreationTimeout, desiredAuthenticationMode.Value, desiredEncryptedCredential);
                            NodeConfiguration.OpcSessions.Add(opcSession);
                            Logger.Information($"{logPrefix} No matching session found for endpoint '{endpointUri.OriginalString}'. Requested to create a new one.");
                        }
                        else
                        {
                            // find the session we need to monitor the node
                            opcSession = NodeConfiguration.OpcSessions.FirstOrDefault(s => s.EndpointUrl.Equals(endpointUri?.OriginalString, StringComparison.OrdinalIgnoreCase));

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
                                    await opcSession.Reconnect();
                                }

                            }
                        }

                        foreach (var eventNode in eventsToAddOrUpdate)
                        {
                            NodeId nodeId = null;
                            ExpandedNodeId expandedNodeId = null;
                            bool isNodeIdFormat;
                            try
                            {
                                if (eventNode.EventNotifierId.Contains("nsu=", StringComparison.InvariantCulture))
                                {
                                    expandedNodeId = ExpandedNodeId.Parse(eventNode.EventNotifierId);
                                    isNodeIdFormat = false;
                                }
                                else
                                {
                                    nodeId = NodeId.Parse(eventNode.EventNotifierId);
                                    isNodeIdFormat = true;
                                }
                            }
                            catch (Exception e)
                            {
                                statusMessage = $"Exception in ({e.Message}) while formatting node '{eventNode.Id}' with key '{eventNode.Key}'!";
                                Logger.Error(e, $"{logPrefix} {statusMessage}");
                                statusResponse.Add(statusMessage);
                                statusCode = HttpStatusCode.NotAcceptable;
                                continue;
                            }

                            if (HasDuplicateKey(endpointId, eventNode.Key))
                            {
                                statusMessage = $"'{eventNode.Id}' has duplicate key '{eventNode.Key}'!";
                                Logger.Error($"{logPrefix} {statusMessage}");
                                statusResponse.Add(statusMessage);
                                statusCode = HttpStatusCode.BadRequest;
                                continue;
                            }

                            if (eventNode.Key.Length > 25 || eventNode.Key.Length < 4 || !nodeKeyValidation.IsMatch(eventNode.Key))
                            {
                                statusMessage = $"'{eventNode.Id}' with key '{eventNode.Key}' is either too long, too short or has invalid characters!";
                                Logger.Error($"{logPrefix} {statusMessage}");
                                statusResponse.Add(statusMessage);
                                statusCode = HttpStatusCode.BadRequest;
                                continue;
                            }

                            try
                            {
                                HttpStatusCode nodeStatusCode;
                                if (isNodeIdFormat)
                                {
                                    // add the event node info to the subscription with the default publishing interval, execute synchronously
                                    Logger.Debug(
                                        $"{logPrefix} Request to monitor eventNode with NodeId '{eventNode.Id}' with key '{eventNode.Key}'");
                                    nodeStatusCode = await opcSession.AddEventNodeForMonitoringAsync(nodeId, null, 5000,
                                            2000, eventNode.Key, null, null, ShutdownTokenSource.Token,
                                            null, publishEventsMethodData, eventNode)
                                        .ConfigureAwait(false);
                                }
                                else
                                {
                                    // add the event node info to the subscription with the default publishing interval, execute synchronously
                                    Logger.Debug(
                                        $"{logPrefix} Request to monitor eventNode with ExpandedNodeId '{eventNode.Id}' with key '{eventNode.Key}'");
                                    nodeStatusCode = await opcSession.AddEventNodeForMonitoringAsync(null, expandedNodeId, 5000,
                                            2000, eventNode.Key, null, null, ShutdownTokenSource.Token,
                                            null, publishEventsMethodData, eventNode)
                                        .ConfigureAwait(false);
                                }

                                // check and store a result message in case of an error
                                switch (nodeStatusCode)
                                {
                                    case HttpStatusCode.OK:
                                        statusMessage = $"'{eventNode.Key}': already monitored";
                                        Logger.Debug($"{logPrefix} {statusMessage}");
                                        statusResponse.Add(statusMessage);
                                        break;

                                    case HttpStatusCode.Accepted:
                                        statusMessage = $"'{eventNode.Key}': added";
                                        Logger.Debug($"{logPrefix} {statusMessage}");
                                        statusResponse.Add(statusMessage);
                                        break;

                                    case HttpStatusCode.Gone:
                                        statusMessage =
                                            $"'{eventNode.Key}': session to endpoint does not exist anymore";
                                        Logger.Debug($"{logPrefix} {statusMessage}");
                                        statusResponse.Add(statusMessage);
                                        statusCode = HttpStatusCode.Gone;
                                        break;

                                    case HttpStatusCode.InternalServerError:
                                        statusMessage = $"'{eventNode.Key}': error while trying to configure";
                                        Logger.Debug($"{logPrefix} {statusMessage}");
                                        statusResponse.Add(statusMessage);
                                        statusCode = HttpStatusCode.InternalServerError;
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                statusMessage =
                                    $"Exception ({e.Message}) while trying to configure publishing node '{eventNode.Id}' with key '{eventNode.Key}'";
                                Logger.Error(e, $"{logPrefix} {statusMessage}");
                                statusResponse.Add(statusMessage);
                                statusCode = HttpStatusCode.InternalServerError;
                            }
                        }
                    }
                }
                catch (AggregateException e)
                {
                    foreach (var ex in e.InnerExceptions)
                    {
                        Logger.Error(ex, $"{logPrefix} Exception");
                    }
                    statusMessage = $"EndpointUrl: '{publishEventsMethodData.EndpointUrl}': exception ({e.Message}) while trying to publish";
                    Logger.Error(e, $"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                catch (Exception e)
                {
                    statusMessage = $"EndpointUrl: '{publishEventsMethodData.EndpointUrl}': exception ({e.Message}) while trying to publish";
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
            publishedEventMethodResponse = new PublishNodesMethodResponseModel(endpointId.ToString());
            string resultString = statusCode == HttpStatusCode.OK || statusCode == HttpStatusCode.Accepted ?
                JsonConvert.SerializeObject(publishedEventMethodResponse) :
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

        /// <summary>
        /// Handle unpublish node method call.
        /// </summary>
        public virtual async Task<MethodResponse> HandleUnpublishEventsMethodAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "HandleUnpublishEventsMethodAsync:";
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
            (statusCode, unpublishStatusResponse) = await UnpublishEventsAsync(endpointId, unpublishNodesMethodData.OpcEvents).ConfigureAwait(false);
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

        private async Task<(HttpStatusCode statusCode, List<string> statusResponse)> UnpublishEventsAsync(Guid endpointId, IEnumerable<OpcEventOnEndpointModel> opcEvents)
        {
            string logPrefix = "UnpublishEventsAsync:";
            IOpcSession opcSession = null;
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
                            if (opcEvents != null && opcEvents.Any())
                            {
                                foreach (var opcEvent in opcEvents)
                                {
                                    try
                                    {
                                        // stop monitoring the node, execute synchronously
                                        Logger.Information($"{logPrefix} Request to stop monitoring item with id '{opcEvent.Id.ToString()}')");
                                        nodeStatusCode = await opcSession.RequestEventNodeRemovalAsync(opcEvent.Id, ShutdownTokenSource.Token).ConfigureAwait(false);

                                        // check and store a result message in case of an error
                                        switch (nodeStatusCode)
                                        {
                                            case HttpStatusCode.OK:
                                                statusMessage = $"Id '{opcEvent.Id}': was not configured";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                break;

                                            case HttpStatusCode.Accepted:
                                                statusMessage = $"Id '{opcEvent.Id}': tagged for removal";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                break;

                                            case HttpStatusCode.Gone:
                                                statusMessage = $"Id '{opcEvent.Id}': session to endpoint does not exist anymore";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                statusCode = HttpStatusCode.Gone;
                                                break;

                                            case HttpStatusCode.InternalServerError:
                                                statusMessage = $"Id '{opcEvent.Id}': error while trying to remove";
                                                Logger.Debug($"{logPrefix} {statusMessage}");
                                                statusResponse.Add(statusMessage);
                                                statusCode = HttpStatusCode.InternalServerError;
                                                break;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        statusMessage = $"Exception ({e.Message}) while trying to tag node '{opcEvent.Id}' for removal";
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
        /// Handle method call to get list of configured nodes on a specific endpoint.
        /// </summary>
        public virtual Task<MethodResponse> HandleGetConfiguredEventsOnEndpointMethodAsync(MethodRequest methodRequest, object userContext)
        {
            const string logPrefix = "HandleGetConfiguredEventsOnEndpointMethodAsync:";
            Guid endpointId = Guid.Empty;
            string endpointName = null;
            string endpointUrl = null;
            GetConfiguredNodesOnEndpointMethodRequestModel getConfiguredEventNodesOnEndpointMethodRequest = null;
            uint nodeConfigVersion = 0;
            GetConfiguredEventNodesOnEndpointMethodResponseModel getConfiguredEventNodesOnEndpointMethodResponse = new GetConfiguredEventNodesOnEndpointMethodResponseModel();
            uint actualNodeCount = 0;
            uint availableEventNodeCount = 0;
            var opcEvents = new List<OpcEventOnEndpointModel>();
            uint startIndex = 0;
            var statusCode = HttpStatusCode.OK;
            var statusResponse = new List<string>();
            string statusMessage;

            try
            {
                Logger.Debug($"{logPrefix} called");
                getConfiguredEventNodesOnEndpointMethodRequest = JsonConvert.DeserializeObject<GetConfiguredNodesOnEndpointMethodRequestModel>(methodRequest.DataAsJson);
                if (getConfiguredEventNodesOnEndpointMethodRequest.EndpointId == null)
                {
                    statusMessage = $"New endpoint: there are no event nodes configured";
                    Logger.Information($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.NoContent;
                }
                else
                {
                    endpointId = new Guid(getConfiguredEventNodesOnEndpointMethodRequest.EndpointId);
                }
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{getConfiguredEventNodesOnEndpointMethodRequest?.EndpointId}'";
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
                    statusMessage = $"There are no event nodes configured for endpoint '{endpointId.ToString()}'";
                    Logger.Information($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.NoContent;
                }
                else
                {
                    endpointName = configFileEntries.First().EndpointName;
                    endpointUrl = configFileEntries.First().EndpointUrl.ToString();
                    foreach (var configFileEntry in configFileEntries)
                    {
                        if (configFileEntry?.OpcEvents != null)
                        {
                            opcEvents.AddRange(configFileEntry.OpcEvents);
                        }
                    }
                    uint configuredEventNodesOnEndpointCount = (uint)opcEvents.Count();

                    // validate version
                    startIndex = 0;
                    if (getConfiguredEventNodesOnEndpointMethodRequest?.ContinuationToken != null)
                    {
                        uint requestedNodeConfigVersion = (uint)(getConfiguredEventNodesOnEndpointMethodRequest.ContinuationToken >> 32);
                        if (nodeConfigVersion != requestedNodeConfigVersion)
                        {
                            statusMessage = $"The event node configuration has changed between calls. Requested version: {requestedNodeConfigVersion:X8}, Current version '{nodeConfigVersion:X8}'!";
                            Logger.Information($"{logPrefix} {statusMessage}");
                            statusResponse.Add(statusMessage);
                            statusCode = HttpStatusCode.Gone;
                        }
                        startIndex = (uint)(getConfiguredEventNodesOnEndpointMethodRequest.ContinuationToken & 0x0FFFFFFFFL);
                    }

                    if (statusCode == HttpStatusCode.OK)
                    {
                        // set count
                        var requestedEventNodeCount = configuredEventNodesOnEndpointCount - startIndex;
                        availableEventNodeCount = configuredEventNodesOnEndpointCount - startIndex;
                        actualNodeCount = Math.Min(requestedEventNodeCount, availableEventNodeCount);
                        opcEvents.ForEach(x => x.OpcPublisherPublishState = OpcPublisherPublishState.Published);

                        // generate response
                        while (true)
                        {
                            string publishedNodesString = JsonConvert.SerializeObject(opcEvents.GetRange((int)startIndex, (int)actualNodeCount));
                            var publishedNodesByteArray = Encoding.UTF8.GetBytes(publishedNodesString);
                            if (publishedNodesByteArray.Length > MaxResponsePayloadLength)
                            {
                                actualNodeCount /= 2;
                                continue;
                            }

                            break;
                        }
                    }
                }
            }

            // build response
            string resultString;
            if (statusCode == HttpStatusCode.OK)
            {
                getConfiguredEventNodesOnEndpointMethodResponse.ContinuationToken = null;
                if (actualNodeCount < availableEventNodeCount)
                {
                    getConfiguredEventNodesOnEndpointMethodResponse.ContinuationToken = (ulong)nodeConfigVersion << 32 | actualNodeCount + startIndex;
                }

                // Todo: check if EventConfigurationModel mit endpointName = endpointUrl = null ok?
                getConfiguredEventNodesOnEndpointMethodResponse.EventNodes
                    .AddRange(opcEvents
                        .GetRange((int)startIndex, (int)actualNodeCount)
                        .Select(n =>
                            new OpcEventOnEndpointModel(
                                new EventConfigurationModel(
                                    endpointId.ToString(),
                                    endpointName,
                                    endpointUrl,
                                    null,
                                    OpcAuthenticationMode.Anonymous,
                                    null,
                                    n.Id,
                                    n.EventNotifierId,
                                    n.Key,
                                    n.SelectClauses,
                                    n.WhereClause,
                                    n.IotCentralEventPublishMode
                               ),
                               OpcPublisherPublishState.Published
                           )
                        )
                    );
                getConfiguredEventNodesOnEndpointMethodResponse.EndpointId = endpointId.ToString();
                resultString = JsonConvert.SerializeObject(getConfiguredEventNodesOnEndpointMethodResponse);
                Logger.Information($"{logPrefix} Success returning {actualNodeCount} event node(s) of {availableEventNodeCount} (start: {startIndex}) (node config version: {nodeConfigVersion:X8})!");
            }
            else if (statusCode == HttpStatusCode.NoContent)
            {
                resultString = JsonConvert.SerializeObject(getConfiguredEventNodesOnEndpointMethodResponse);
                Logger.Information($"{logPrefix} Success returning 0 event nodes.");
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
            byte[] result = Encoding.UTF8.GetBytes(resultString);
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return Task.FromResult(methodResponse);
        }

        public virtual async Task<MethodResponse> HandleDeleteConfiguredEndpointMethodAsync(MethodRequest methodRequest, object userContext)
        {
            const string logPrefix = "HandleDeleteConfiguredEndpointMethodAsync:";
            Guid endpointId = Guid.Empty;
            DeleteConfiguredEndpointMethodRequestModel deleteConfiguredEndpointMethodRequest = null;

            HttpStatusCode statusCode = HttpStatusCode.OK;
            List<string> statusResponse = new List<string>();
            string statusMessage = string.Empty;

            try
            {
                Logger.Debug($"{logPrefix} called");
                deleteConfiguredEndpointMethodRequest = JsonConvert.DeserializeObject<DeleteConfiguredEndpointMethodRequestModel>(methodRequest.DataAsJson);

                if (deleteConfiguredEndpointMethodRequest.EndpointId == null)
                {
                    statusMessage = $"EndpointId can not be null.";
                    Logger.Error($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                endpointId = new Guid(deleteConfiguredEndpointMethodRequest.EndpointId);
            }
            catch (FormatException e)
            {
                statusMessage = $"Exception ({e.Message}) while parsing EndpointId '{deleteConfiguredEndpointMethodRequest?.EndpointId}'";
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
                // Unpublish all nodes from endpoint
                var unpublishNodesRequestModel = new UnpublishAllNodesMethodRequestModel(endpointId.ToString());
                var json = JsonConvert.SerializeObject(unpublishNodesRequestModel);
                var jsonBytes = Encoding.UTF8.GetBytes(json);

                var unpublishNodesRequest = new MethodRequest("UnpublishNodes", jsonBytes, TimeSpan.FromSeconds(150), TimeSpan.FromSeconds(60));
                var unpublishNodesResponse = await HandleUnpublishAllNodesMethodAsync(unpublishNodesRequest, userContext);

                if (unpublishNodesResponse.Status != (int)HttpStatusCode.OK)
                {
                    statusMessage = $"Error unpublishing nodes from endpoint {endpointId.ToString()}.";
                    Logger.Error($"{logPrefix} {statusMessage}");
                    statusResponse.Add(statusMessage);
                    statusCode = HttpStatusCode.InternalServerError;
                }
                else
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
                            var session = NodeConfiguration.OpcSessions.SingleOrDefault(s => s.EndpointId == endpointId);
                            if (session == null)
                            {
                                statusMessage = $"No matching session for endpoint {endpointId.ToString()}.";
                                Logger.Warning($"{logPrefix} {statusMessage}");
                                statusResponse.Add(statusMessage);
                                statusCode = HttpStatusCode.NotFound;
                            }
                            else
                            {
                                NodeConfiguration.OpcSessions.Remove(session);

                                statusMessage = $"Endpoint {endpointId.ToString()} has been removed..";
                                Logger.Warning($"{logPrefix} {statusMessage}");
                                statusResponse.Add(statusMessage);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        statusMessage = $"EndpointId: '{endpointId.ToString()}': exception ({e.Message}) while trying to delete";
                        Logger.Error(e, $"{logPrefix} {statusMessage}");
                        statusResponse.Add(statusMessage);
                        statusCode = HttpStatusCode.InternalServerError;
                    }
                    finally
                    {
                        NodeConfiguration.OpcSessionsListSemaphore.Release();
                    }
                }
            }

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

        public virtual async Task<MethodResponse> HandleGetOpcPublishedConfigurationAsJson(MethodRequest methodRequest, object userContext)
        {
            const string logPrefix = "HandleGetOpcPublishedConfigurationAsJson:";
            var statusCode = HttpStatusCode.OK;
            GetOpcPublishedConfigurationMethodResponseModel getOpcPublishedConfigurationMethodResponseModel = new GetOpcPublishedConfigurationMethodResponseModel();
            var statusResponse = new List<string>();

            var configJson = await NodeConfiguration.ReadConfigAsyncAsJson();
            if (configJson == null)
            {
                var statusMessage = $"Error while reading opc publisher json configuration.";
                Logger.Information($"{logPrefix} there is no valid configuration file to return.");
                statusResponse.Add(statusMessage);
                statusCode = HttpStatusCode.NotFound;
            }

            getOpcPublishedConfigurationMethodResponseModel.ConfigurationJson = configJson;
            // build response
            string resultString;
            if (statusCode == HttpStatusCode.OK)
            {
                resultString = JsonConvert.SerializeObject(getOpcPublishedConfigurationMethodResponseModel);
                Logger.Information($"{logPrefix} Success returning current JSON configuration of OPC Publisher!");
            }
            else
            {
                resultString = JsonConvert.SerializeObject(statusResponse);
            }
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

        public async Task<MethodResponse> HandleSaveOpcPublishedConfigurationAsJson(MethodRequest methodRequest, object userContext)
        {
            const string logPrefix = "HandleGetOpcPublishedConfigurationAsJson:";
            var statusCode = HttpStatusCode.OK;
            SaveOpcPublishedConfigurationMethodResponseModel saveOpcPublishedConfigurationMethodResponseModel = new SaveOpcPublishedConfigurationMethodResponseModel();

            // save json file.
            var methodRequestData = JsonConvert.DeserializeObject<HandleSaveOpcPublishedConfigurationMethodRequestModel>(methodRequest.DataAsJson);
            var success = await NodeConfiguration.SaveJsonAsPublisherNodeConfiguration(methodRequestData.ConfigurationJsonString);

            if (!success)
            {
                Logger.Information($"{logPrefix} the configuration file is not valid.");
                statusCode = HttpStatusCode.BadRequest;
            }
            else
            {
                // Reinitialize the configurations.
                await NodeConfiguration.InitAsync();
            }

            saveOpcPublishedConfigurationMethodResponseModel.Success = success;
            byte[] result = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(saveOpcPublishedConfigurationMethodResponseModel));
            if (result.Length > MaxResponsePayloadLength)
            {
                Logger.Error($"{logPrefix} Response size is too long");
                Array.Resize(ref result, result.Length > MaxResponsePayloadLength ? MaxResponsePayloadLength : result.Length);
            }
            MethodResponse methodResponse = new MethodResponse(result, (int)statusCode);
            Logger.Information($"{logPrefix} completed with result {statusCode.ToString()}");
            return methodResponse;
        }

        public static int MonitoredSettingsQueueCapacity => _settingsProcessor.MonitoredSettingsQueueCapacity;
        public static long MonitoredSettingsQueueCount => _settingsProcessor.MonitoredSettingsQueueCount;
        public static long SentSettings => _settingsProcessor.SentSettings;
        public static long SentProperties => _propertiesProcessor.SentProperties;
        public static long SentIoTcEvents => IoTCEventsProcessor.SentIoTcEvents;
        public static int MonitoredPropertiesQueueCapacity => _propertiesProcessor.MonitoredPropertiesQueueCapacity;
        public static long MonitoredPropertiesQueueCount => _propertiesProcessor.MonitoredPropertiesQueueCount;
        public static TransportType SendHubProtocol { get; set; } = IotHubProtocolDefault;
        private static Logger _logger;
    }
}
