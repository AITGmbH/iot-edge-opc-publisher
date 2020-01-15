namespace OpcPublisher
{
    using Opc.Ua;
    using OpcPublisher.AIT;
    using OpcPublisher.Crypto;
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using static OpcApplicationConfiguration;
    using static OpcPublisher.OpcMonitoredItem;
    using static Program;

    public partial class OpcSession
    {
        /// <summary>
        /// Adds a event node to be monitored. If there is no subscription with the requested publishing interval,
        /// one is created.
        /// </summary>
        public async Task<HttpStatusCode> AddEventNodeForMonitoringAsync(NodeId nodeId, ExpandedNodeId expandedNodeId,
            int? opcPublishingInterval, int? opcSamplingInterval, string displayName,
            int? heartbeatInterval, bool? skipFirst, CancellationToken ct, IotCentralItemPublishMode? iotCentralItemPublishMode,
            PublishNodesMethodRequestModel publishEventsMethodData)
        {
            string logPrefix = "AddEventNodeForMonitoringAsync:";
            bool sessionLocked = false;
            try
            {
                sessionLocked = await LockSessionAsync().ConfigureAwait(false);
                if (!sessionLocked || ct.IsCancellationRequested)
                {
                    return HttpStatusCode.Gone;
                }

                // check if there is already a subscription with the same publishing interval, which can be used to monitor the node
                int opcPublishingIntervalForNode = opcPublishingInterval ?? OpcPublishingIntervalDefault;
                IOpcSubscription opcEventSubscription = OpcEventSubscriptions.FirstOrDefault(s => s.RequestedPublishingInterval == opcPublishingIntervalForNode);

                // if there was none found, create one
                if (opcEventSubscription == null)
                {
                    if (opcPublishingInterval == null)
                    {
                        Logger.Information($"{logPrefix} No matching subscription with default publishing interval found.");
                        Logger.Information($"Create a new subscription with a default publishing interval.");
                    }
                    else
                    {
                        Logger.Information($"{logPrefix} No matching subscription with publishing interval of {opcPublishingInterval} found.");
                        Logger.Information($"Create a new subscription with a publishing interval of {opcPublishingInterval}.");
                    }
                    opcEventSubscription = new OpcSubscription(opcPublishingInterval);
                    OpcEventSubscriptions.Add(opcEventSubscription);
                }

                // create objects for publish check
                ExpandedNodeId expandedNodeIdCheck = expandedNodeId;
                NodeId nodeIdCheck = nodeId;
                if (State == SessionState.Connected)
                {
                    if (expandedNodeId == null)
                    {
                        string namespaceUri = _namespaceTable.ToArray().ElementAtOrDefault(nodeId.NamespaceIndex);
                        expandedNodeIdCheck = new ExpandedNodeId(nodeId.Identifier, nodeId.NamespaceIndex, namespaceUri, 0);
                    }
                    if (nodeId == null)
                    {
                        nodeIdCheck = new NodeId(expandedNodeId.Identifier, (ushort)(_namespaceTable.GetIndex(expandedNodeId.NamespaceUri)));
                    }
                }

                // if it is already published, we do nothing, else we create a new monitored event item
                if (!IsEventNodePublishedInSessionInternal(nodeIdCheck, expandedNodeIdCheck))
                {
                    OpcMonitoredItem opcMonitoredItem = null;
                    EncryptedNetworkCredential encryptedCredentials = null;

                    if (publishEventsMethodData.OpcAuthenticationMode == OpcAuthenticationMode.UsernamePassword)
                    {
                        if (string.IsNullOrWhiteSpace(publishEventsMethodData.UserName) && string.IsNullOrWhiteSpace(publishEventsMethodData.Password))
                        {
                            throw new ArgumentException($"If {nameof(publishEventsMethodData.OpcAuthenticationMode)} is set to '{OpcAuthenticationMode.UsernamePassword}', you have to specify '{nameof(publishEventsMethodData.UserName)}' and/or '{nameof(publishEventsMethodData.Password)}'.");
                        }

                        encryptedCredentials = await EncryptedNetworkCredential.FromPlainCredential(publishEventsMethodData.UserName, publishEventsMethodData.Password);
                    }                 

                    // add a new item to monitor
                    if (expandedNodeId == null)
                    {
                        opcMonitoredItem = new OpcMonitoredItem(new EventConfigurationModel(publishEventsMethodData.EndpointId, publishEventsMethodData.EndpointName, publishEventsMethodData.EndpointUrl, publishEventsMethodData.UseSecurity, publishEventsMethodData.OpcAuthenticationMode ?? OpcAuthenticationMode.Anonymous, encryptedCredentials,
                            publishEventsMethodData.OpcEvents[0].Id, publishEventsMethodData.OpcEvents[0].DisplayName, publishEventsMethodData.OpcEvents[0].SelectClauses,
                            publishEventsMethodData.OpcEvents[0].WhereClause, publishEventsMethodData.OpcEvents[0].IotCentralEventPublishMode), EndpointId, EndpointUrl); ;
                    }
                    else
                    {
                        opcMonitoredItem = new OpcMonitoredItem(new EventConfigurationModel(publishEventsMethodData.EndpointId, publishEventsMethodData.EndpointName, publishEventsMethodData.EndpointUrl, publishEventsMethodData.UseSecurity, publishEventsMethodData.OpcAuthenticationMode ?? OpcAuthenticationMode.Anonymous, encryptedCredentials,
                            expandedNodeId.ToString(), publishEventsMethodData.OpcEvents[0].DisplayName, publishEventsMethodData.OpcEvents[0].SelectClauses,
                            publishEventsMethodData.OpcEvents[0].WhereClause, publishEventsMethodData.OpcEvents[0].IotCentralEventPublishMode), EndpointId, EndpointUrl);
                    }
                    opcEventSubscription.OpcMonitoredItems.Add(opcMonitoredItem);
                    Interlocked.Increment(ref NodeConfigVersion);
                    Logger.Debug($"{logPrefix} Added event item with nodeId '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' for monitoring.");

                    // trigger the actual OPC communication with the server to be done
                    ConnectAndMonitorSession.Set();
                    return HttpStatusCode.Accepted;
                }
                else
                {
                    Logger.Debug($"{logPrefix} Node with Id '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' is already monitored.");
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"{logPrefix} Exception while trying to add node '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' for monitoring.");
                return HttpStatusCode.InternalServerError;
            }
            finally
            {
                if (sessionLocked)
                {
                    ReleaseSession();
                }
            }
            return HttpStatusCode.OK;
        }

        
        /// <summary>
        /// Monitoring for an event source starts if it is required.
        /// </summary>
        public async Task MonitorEventsAsync(CancellationToken ct)
        {
            bool sessionLocked = false;
            try
            {
                try
                {
                    sessionLocked = await LockSessionAsync().ConfigureAwait(false);

                    // if the session is not connected or shutdown in progress, return
                    if (!sessionLocked || ct.IsCancellationRequested || State != SessionState.Connected)
                    {
                        return;
                    }
                }
                catch (Exception)
                {
                    throw;
                }

                // ensure all nodes in all subscriptions of this session are monitored.
                foreach (var opcEventSubscription in OpcEventSubscriptions)
                {
                    // create the subscription, if it is not yet there.
                    if (opcEventSubscription.OpcUaClientSubscription == null)
                    {
                        opcEventSubscription.OpcUaClientSubscription = CreateSubscription(opcEventSubscription.RequestedPublishingInterval, out int revisedPublishingInterval);
                        opcEventSubscription.PublishingInterval = revisedPublishingInterval;
                        Logger.Information($"Create Event subscription on endpoint '{EndpointUrl}' requested OPC publishing interval is {opcEventSubscription.RequestedPublishingInterval} ms. (revised: {revisedPublishingInterval} ms)");
                    }

                    // process all unmonitored events.
                    var unmonitoredEvents = opcEventSubscription.OpcMonitoredItems.Where(i => (i.State == OpcMonitoredItemState.Unmonitored || i.State == OpcMonitoredItemState.UnmonitoredNamespaceUpdateRequested));
                    int additionalMonitoredEventsCount = 0;
                    int monitoredEventsCount = 0;
                    bool haveUnmonitoredEvents = false;
                    if (unmonitoredEvents.Count() != 0)
                    {
                        haveUnmonitoredEvents = true;
                        monitoredEventsCount = opcEventSubscription.OpcMonitoredItems.Count(i => (i.State == OpcMonitoredItemState.Monitored));
                        Logger.Information($"Start monitoring events on endpoint '{EndpointUrl}'. Currently monitoring {monitoredEventsCount} events.");
                    }

                    // init perf data
                    Stopwatch stopWatch = new Stopwatch();
                    stopWatch.Start();
                    foreach (var unmonitoredEvent in unmonitoredEvents)
                    {
                        // if the session is not connected or a shutdown is in progress, we stop trying and wait for the next cycle
                        if (ct.IsCancellationRequested || State != SessionState.Connected)
                        {
                            break;
                        }

                        NodeId currentNodeId = null;
                        try
                        {
                            // update the namespace of the node if requested. there are two cases where this is requested:
                            // 1) publishing requests via the OPC server method are raised using a NodeId format. for those
                            //    the NodeId format is converted into an ExpandedNodeId format
                            // 2) ExpandedNodeId configuration file entries do not have at parsing time a session to get
                            //    the namespace index. this is set now.
                            if (unmonitoredEvent.State == OpcMonitoredItemState.UnmonitoredNamespaceUpdateRequested)
                            {
                                if (unmonitoredEvent.ConfigType == OpcMonitoredItemConfigurationType.ExpandedNodeId)
                                {
                                    ExpandedNodeId expandedNodeId = ExpandedNodeId.Parse(unmonitoredEvent.Id);
                                    int namespaceIndex = _namespaceTable.GetIndex(expandedNodeId.NamespaceUri);
                                    if (namespaceIndex < 0)
                                    {
                                        Logger.Information($"The namespace URI of node '{expandedNodeId.ToString()}' can be not mapped to a namespace index.");
                                    }
                                    else
                                    {
                                        //unmonitoredEvent.IdAsExpandedNodeId = new ExpandedNodeId(expandedNodeId.Identifier, (ushort)namespaceIndex, expandedNodeId.NamespaceUri, 0);
                                        unmonitoredEvent.IdAsExpandedNodeId = expandedNodeId;
                                    }
                                }
                                if (unmonitoredEvent.ConfigType == OpcMonitoredItemConfigurationType.NodeId)
                                {
                                    NodeId nodeId = NodeId.Parse(unmonitoredEvent.Id);
                                    string namespaceUri = _namespaceTable.ToArray().ElementAtOrDefault(nodeId.NamespaceIndex);
                                    if (string.IsNullOrEmpty(namespaceUri))
                                    {
                                        Logger.Information($"The namespace index of node '{nodeId.ToString()}' is invalid and the node format can not be updated.");
                                    }
                                    else
                                    {
                                        unmonitoredEvent.IdAsExpandedNodeId = new ExpandedNodeId(nodeId.Identifier, nodeId.NamespaceIndex, namespaceUri, 0);
                                        unmonitoredEvent.ConfigType = OpcMonitoredItemConfigurationType.ExpandedNodeId;
                                    }
                                }
                                unmonitoredEvent.State = OpcMonitoredItemState.Unmonitored;
                            }

                            // lookup namespace index if ExpandedNodeId format has been used and build NodeId identifier.
                            if (unmonitoredEvent.ConfigType == OpcMonitoredItemConfigurationType.ExpandedNodeId)
                            {
                                ExpandedNodeId expandedNodeId = ExpandedNodeId.Parse(unmonitoredEvent.Id);
                                int namespaceIndex = _namespaceTable.GetIndex(expandedNodeId.NamespaceUri);
                                if (namespaceIndex < 0)
                                {
                                    Logger.Warning($"Syntax or namespace URI of ExpandedNodeId '{expandedNodeId.ToString()}' is invalid and will be ignored.");
                                    continue;
                                }
                                unmonitoredEvent.IdAsNodeId = new NodeId(expandedNodeId.Identifier, expandedNodeId.NamespaceIndex);
                                currentNodeId = unmonitoredEvent.IdAsNodeId;
                            }
                            else
                            {
                                NodeId nodeId = NodeId.Parse(unmonitoredEvent.Id);
                                string namespaceUri = _namespaceTable.ToArray().ElementAtOrDefault(nodeId.NamespaceIndex);
                                if (string.IsNullOrEmpty(namespaceUri))
                                {
                                    Logger.Information($"The namespace index of node '{nodeId.ToString()}' is invalid and the node format can not be updated.");
                                }
                                else
                                {
                                    unmonitoredEvent.IdAsExpandedNodeId = new ExpandedNodeId(nodeId.Identifier, nodeId.NamespaceIndex, namespaceUri, 0);
                                    currentNodeId = nodeId;
                                }
                            }

                            // if configured, get the DisplayName for the node, otherwise use the nodeId
                            Node node;
                            if (string.IsNullOrEmpty(unmonitoredEvent.DisplayName))
                            {
                                if (FetchOpcNodeDisplayName == true)
                                {
                                    node = OpcUaClientSession.ReadNode(currentNodeId);
                                    unmonitoredEvent.DisplayName = node.DisplayName.Text ?? currentNodeId.ToString();
                                }
                                else
                                {
                                    unmonitoredEvent.DisplayName = currentNodeId.ToString();
                                }
                            }

                            // resolve all node and namespace references in the select and where clauses
                            EventFilter eventFilter = new EventFilter();
                            foreach (var selectClause in unmonitoredEvent.EventConfiguration.SelectClauses)
                            {
                                SimpleAttributeOperand simpleAttributeOperand = new SimpleAttributeOperand();
                                simpleAttributeOperand.AttributeId = selectClause.AttributeId.ResolveAttributeId();
                                simpleAttributeOperand.IndexRange = selectClause.IndexRange;
                                NodeId typeId = selectClause.TypeId.ToNodeId(_namespaceTable);
                                simpleAttributeOperand.TypeDefinitionId = new NodeId(typeId);
                                QualifiedNameCollection browsePaths = new QualifiedNameCollection();
                                foreach (var browsePath in selectClause.BrowsePaths)
                                {
                                    browsePaths.Add(QualifiedName.Parse(browsePath));
                                }
                                simpleAttributeOperand.BrowsePath = browsePaths;
                                eventFilter.SelectClauses.Add(simpleAttributeOperand);
                            }
                            foreach (var whereClauseElement in unmonitoredEvent.EventConfiguration.WhereClause)
                            {
                                ContentFilterElement contentFilterElement = new ContentFilterElement();
                                contentFilterElement.FilterOperator = whereClauseElement.Operator.ResolveFilterOperator();
                                switch (contentFilterElement.FilterOperator)
                                {
                                    case FilterOperator.OfType:
                                    case FilterOperator.InView:
                                        if (whereClauseElement.Operands.Count != 1)
                                        {
                                            Logger.Error($"The where clause element '{whereClauseElement.ToString()}' must contain 1 operands.");
                                            continue;
                                        }
                                        FilterOperand[] filterOperands = new FilterOperand[1];
                                        TypeInfo typeInfo = new TypeInfo(BuiltInType.NodeId, ValueRanks.Scalar);
                                        filterOperands[0] = whereClauseElement.Operands[0].GetOperand(typeInfo);
                                        //filterOperands[0] = whereClauseElement.Operands[0].GetOperand(DataTypeIds.NodeId);
                                        eventFilter.WhereClause.Push(contentFilterElement.FilterOperator, filterOperands);
                                        break;
                                    case FilterOperator.Equals:
                                    case FilterOperator.IsNull:
                                    case FilterOperator.GreaterThan:
                                    case FilterOperator.LessThan:
                                    case FilterOperator.GreaterThanOrEqual:
                                    case FilterOperator.LessThanOrEqual:
                                    case FilterOperator.Like:
                                    case FilterOperator.Not:
                                    case FilterOperator.Between:
                                    case FilterOperator.InList:
                                    case FilterOperator.And:
                                    case FilterOperator.Or:
                                    case FilterOperator.Cast:
                                    case FilterOperator.BitwiseAnd:
                                    case FilterOperator.BitwiseOr:
                                    //case FilterOperator.InView:
                                    //case FilterOperator.OfType:
                                    case FilterOperator.RelatedTo:
                                    default:
                                        Logger.Error($"The operator '{contentFilterElement.FilterOperator.ToString()}' is not supported.");
                                        break;
                                }
                            }

                            // add the new monitored event.
                            IOpcUaMonitoredItem monitoredItem = new OpcUaMonitoredItem()
                            {
                                StartNodeId = currentNodeId,
                                AttributeId = Attributes.EventNotifier,
                                DisplayName = unmonitoredEvent.DisplayName,
                                MonitoringMode = unmonitoredEvent.MonitoringMode,
                                SamplingInterval = 0,
                                QueueSize = unmonitoredEvent.QueueSize,
                                DiscardOldest = unmonitoredEvent.DiscardOldest,
                                Filter = eventFilter
                            };

                            monitoredItem.Notification += unmonitoredEvent.NotificationEventHandler;
                            opcEventSubscription.OpcUaClientSubscription.AddItem(monitoredItem);
                            unmonitoredEvent.OpcUaClientMonitoredItem = monitoredItem;
                            unmonitoredEvent.State = OpcMonitoredItemState.Monitored;
                            unmonitoredEvent.EndpointUrl = EndpointUrl;
                            Logger.Verbose($"Created monitored event for node '{currentNodeId.ToString()}' in subscription with id '{opcEventSubscription.OpcUaClientSubscription.Id}' on endpoint '{EndpointUrl}' (version: {NodeConfigVersion:X8})");
                            if (unmonitoredEvent.RequestedSamplingInterval != monitoredItem.SamplingInterval)
                            {
                                Logger.Information($"Sampling interval: requested: {unmonitoredEvent.RequestedSamplingInterval}; revised: {monitoredItem.SamplingInterval}");
                                unmonitoredEvent.SamplingInterval = monitoredItem.SamplingInterval;
                            }
                            if (additionalMonitoredEventsCount % 10000 == 0)
                            {
                                Logger.Information($"Now monitoring {monitoredEventsCount + additionalMonitoredEventsCount} events in subscription with id '{opcEventSubscription.OpcUaClientSubscription.Id}'");
                            }
                        }
                        catch (Exception e) when (e.GetType() == typeof(ServiceResultException))
                        {
                            ServiceResultException sre = (ServiceResultException)e;
                            switch ((uint)sre.Result.StatusCode)
                            {
                                case StatusCodes.BadSessionIdInvalid:
                                    {
                                        Logger.Information($"Session with Id {OpcUaClientSession.SessionId} is no longer available on endpoint '{EndpointUrl}'. Cleaning up.");
                                        // clean up the session
                                        InternalDisconnect();
                                        break;
                                    }
                                case StatusCodes.BadNodeIdInvalid:
                                case StatusCodes.BadNodeIdUnknown:
                                    {
                                        Logger.Error($"Failed to monitor node '{currentNodeId}' on endpoint '{EndpointUrl}'.");
                                        Logger.Error($"OPC UA ServiceResultException is '{sre.Result}'. Please check your publisher configuration for this node.");
                                        break;
                                    }
                                default:
                                    {
                                        Logger.Error($"Unhandled OPC UA ServiceResultException '{sre.Result}' when monitoring node '{currentNodeId}' on endpoint '{EndpointUrl}'. Continue.");
                                        break;
                                    }
                            }
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, $"Failed to monitor node '{currentNodeId}' on endpoint '{EndpointUrl}'");
                        }
                    }
                    opcEventSubscription.OpcUaClientSubscription.SetPublishingMode(true);
                    opcEventSubscription.OpcUaClientSubscription.ApplyChanges();
                    stopWatch.Stop();
                    if (haveUnmonitoredEvents == true)
                    {
                        monitoredEventsCount = opcEventSubscription.OpcMonitoredItems.Count(i => (i.State == OpcMonitoredItemState.Monitored));
                        Logger.Information($"Done processing unmonitored events on endpoint '{EndpointUrl}' took {stopWatch.ElapsedMilliseconds} msec. Now monitoring {monitoredEventsCount} events in subscription with id '{opcEventSubscription.OpcUaClientSubscription.Id}'.");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Exception");
            }
            finally
            {
                if (sessionLocked)
                {
                    ReleaseSession();
                }
            }
        }

        public async Task<HttpStatusCode> RequestEventNodeRemovalAsync(NodeId nodeId, ExpandedNodeId expandedNodeId, CancellationToken ct, bool takeLock = true)
        {
            HttpStatusCode result = HttpStatusCode.Gone;
            bool sessionLocked = false;
            try
            {
                if (takeLock)
                {
                    sessionLocked = await LockSessionAsync().ConfigureAwait(false);

                    if (!sessionLocked || ct.IsCancellationRequested)
                    {
                        return HttpStatusCode.Gone;
                    }
                }

                // create objects for publish check
                ExpandedNodeId expandedNodeIdCheck = expandedNodeId;
                NodeId nodeIdCheck = nodeId;
                if (State == SessionState.Connected)
                {
                    if (expandedNodeId == null)
                    {
                        string namespaceUri = _namespaceTable.ToArray().ElementAtOrDefault(nodeId.NamespaceIndex);
                        expandedNodeIdCheck = new ExpandedNodeId(nodeIdCheck, namespaceUri, 0);
                    }
                    if (nodeId == null)
                    {
                        nodeIdCheck = new NodeId(expandedNodeId.Identifier, (ushort)(_namespaceTable.GetIndex(expandedNodeId.NamespaceUri)));
                    }

                }

                // if node is not published return success
                if (!IsNodePublishedInSessionInternal(nodeIdCheck, expandedNodeIdCheck))
                {
                    Logger.Information($"RequestEventNodeRemovalAsync: Node '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' is not monitored.");
                    return HttpStatusCode.OK;
                }

                // tag all monitored items with nodeId to stop monitoring.
                // if the node to tag is specified as NodeId, it will also tag nodes configured in ExpandedNodeId format.
                foreach (var opcEventSubscription in OpcEventSubscriptions)
                {
                    var opcMonitoredItems = opcEventSubscription.OpcMonitoredItems.Where(m => { return m.IsMonitoringThisNode(nodeIdCheck, expandedNodeIdCheck, _namespaceTable); });
                    foreach (var opcMonitoredItem in opcMonitoredItems)
                    {
                        // tag it for removal.
                        opcMonitoredItem.State = OpcMonitoredItemState.RemovalRequested;
                        Logger.Information($"RequestEventNodeRemovalAsync: Node with id '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' tagged to stop monitoring.");
                        result = HttpStatusCode.Accepted;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"RequestEventNodeRemovalAsync: Exception while trying to tag node '{(expandedNodeId == null ? nodeId.ToString() : expandedNodeId.ToString())}' to stop monitoring.");
                result = HttpStatusCode.InternalServerError;
            }
            finally
            {
                if (sessionLocked)
                {
                    ReleaseSession();
                }
            }
            return result;
        }

        
        public async Task StopMonitoringEventsAsync(CancellationToken ct)
        {
            bool sessionLocked = false;
            try
            {
                try
                {
                    sessionLocked = await LockSessionAsync().ConfigureAwait(false);

                    // if shutdown is in progress, return
                    if (!sessionLocked || ct.IsCancellationRequested)
                    {
                        return;
                    }
                }
                catch (Exception)
                {
                    throw;
                }

                foreach (var opcEventSubscription in OpcEventSubscriptions)
                {
                    // remove items tagged to stop in the stack
                    var itemsToRemove = opcEventSubscription.OpcMonitoredItems.Where(i => i.State == OpcMonitoredItemState.RemovalRequested);
                    if (itemsToRemove.Any())
                    {
                        try
                        {
                            Logger.Information($"Remove monitored items in subscription with id {opcEventSubscription.OpcUaClientSubscription.Id} on endpoint '{EndpointUrl}'");
                            SavePublishedNodes = true;
                            opcEventSubscription.OpcUaClientSubscription.RemoveItems(itemsToRemove.Select(i => i.OpcUaClientMonitoredItem));
                            Logger.Information($"There are now {opcEventSubscription.OpcUaClientSubscription.MonitoredItemCount} monitored items in this subscription.");
                        }
                        catch
                        {
                            // nodes may be tagged for stop before they are monitored, just continue
                        }
                        // stop heartbeat timer for all items to remove
                        foreach (var itemToRemove in itemsToRemove)
                        {
                            itemToRemove?.HeartbeatSendTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                        }
                        // remove them in our data structure
                        opcEventSubscription.OpcMonitoredItems.RemoveAll(i => i.State == OpcMonitoredItemState.RemovalRequested);
                        Interlocked.Increment(ref NodeConfigVersion);
                        Logger.Information($"There are now {opcEventSubscription.OpcMonitoredItems.Count} monitored items for this subscription. (version: {NodeConfigVersion:X8})");
                    }
                }
            }
            finally
            {
                if (sessionLocked)
                {
                    ReleaseSession();
                }
            }
        }


        /// <summary>
        /// Checks if the event node specified by either the given NodeId or ExpandedNodeId on the given endpoint is published in the session. Caller to take session semaphore.
        /// </summary>
        private bool IsEventNodePublishedInSessionInternal(NodeId nodeId, ExpandedNodeId expandedNodeId)
        {
            try
            {
                foreach (var opcSubscription in OpcEventSubscriptions)
                {
                    if (opcSubscription.OpcMonitoredItems.Any(m => { return m.IsMonitoringThisNode(nodeId, expandedNodeId, _namespaceTable); }))
                    {
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, "Exception");
            }
            return false;
        }
    }
}
