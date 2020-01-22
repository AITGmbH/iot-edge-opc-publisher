using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Opc.Ua;
using OpcPublisher.AIT;
using Serilog.Core;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpcPublisher
{
    using Microsoft.Azure.Devices.Client;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Globalization;
    using System.Net;
    using System.Reflection;
    using System.Text;
    using System.Web;


    /// <summary>
    /// Class to encapsulate the IoTHub device/module client interface.
    /// </summary>
    public class HubClient : IHubClient, IDisposable
    {
        // Iot Central specific values for status property
        private const string IoTCentral_ReportedPropertyStatusPending = "pending";
        private const string IoTCentral_ReportedPropertyStatusFailed = "failed";
        private const string IoTCentral_ReportedPropertyStatusCompleted = "completed";

        /// <summary>
        /// Stores custom product information that will be appended to the user agent string that is sent to IoT Hub.
        /// </summary>
        public string ProductInfo
        {
            get
            {
                if (_iotHubClient == null)
                {
                    return _edgeHubClient.ProductInfo;
                }
                return _iotHubClient.ProductInfo;
            }
            set
            {
                if (_iotHubClient == null)
                {
                    _edgeHubClient.ProductInfo = value;
                    return;
                }
                _iotHubClient.ProductInfo = value;
            }
        }

        public static Dictionary<string, string> MonitoredSettingsCollection { get; private set; }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient(Logger logger)
        {
            MonitoredSettingsCollection = new Dictionary<string, string>();
            _logger = logger;
        }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient(DeviceClient iotHubClient, Logger logger)
        {
            _iotHubClient = iotHubClient;
            MonitoredSettingsCollection = new Dictionary<string, string>();
            _iotHubClient.SetDesiredPropertyUpdateCallbackAsync(HandleSettingChanged, null);
            _iotHubClient.SetMethodHandlerAsync("DesiredPropertiesUpdated", DesiredPropertiesUpdated, null);
            _logger = logger;
        }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient(ModuleClient edgeHubClient, Logger logger)
        {
            _edgeHubClient = edgeHubClient;
            MonitoredSettingsCollection = new Dictionary<string, string>();
            _edgeHubClient.SetDesiredPropertyUpdateCallbackAsync(HandleSettingChanged, null);
            _edgeHubClient.SetMethodHandlerAsync("DesiredPropertiesUpdated", DesiredPropertiesUpdated, null);
            _logger = logger;
        }

        /// <summary>
        /// Ctor for the class.
        /// </summary>
        public HubClient()
        {
        }

        /// <summary>
        /// Implement IDisposable.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_iotHubClient == null)
                {
                    _edgeHubClient.Dispose();
                    return;
                }
                _iotHubClient.Dispose();
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
        /// Create DeviceClient from the specified connection string using the specified transport type
        /// </summary>
        public static IHubClient CreateDeviceClientFromConnectionString(string connectionString, TransportType transportType, Logger logger)
        {
            return new HubClient(DeviceClient.CreateFromConnectionString(connectionString, transportType), logger);
        }

        /// <summary>
        /// Create ModuleClient from the specified connection string using the specified transport type
        /// </summary>
        public static IHubClient CreateModuleClientFromEnvironment(TransportType transportType, Logger logger)
        {
            return new HubClient(ModuleClient.CreateFromEnvironmentAsync(transportType).Result, logger);
        }

        /// <summary>
        /// Close the client instance
        /// </summary>
        public Task CloseAsync()
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.CloseAsync();
            }
            return _iotHubClient.CloseAsync();
        }

        /// <summary>
        /// Sets the retry policy used in the operation retries.
        /// </summary>
        public void SetRetryPolicy(IRetryPolicy retryPolicy)
        {
            if (_iotHubClient == null)
            {
                _edgeHubClient.SetRetryPolicy(retryPolicy);
                return;
            }
            _iotHubClient.SetRetryPolicy(retryPolicy);
        }

        /// <summary>
        /// Registers a new delegate for the connection status changed callback. If a delegate is already associated, 
        /// it will be replaced with the new delegate.
        /// </summary>
        public void SetConnectionStatusChangesHandler(ConnectionStatusChangesHandler statusChangesHandler)
        {
            if (_iotHubClient == null)
            {
                _edgeHubClient.SetConnectionStatusChangesHandler(statusChangesHandler);
                return;
            }
            _iotHubClient.SetConnectionStatusChangesHandler(statusChangesHandler);
        }

        /// <summary>
        /// Explicitly open the DeviceClient instance.
        /// </summary>
        public Task OpenAsync()
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.OpenAsync();
            }
            return _iotHubClient.OpenAsync();
        }

        /// <summary>
        /// Registers a new delegate for the named method. If a delegate is already associated with
        /// the named method, it will be replaced with the new delegate.
        /// </summary>
        public Task SetMethodHandlerAsync(string methodName, MethodCallback methodHandler)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SetMethodHandlerAsync(methodName, methodHandler, _edgeHubClient);
            }
            return _iotHubClient.SetMethodHandlerAsync(methodName, methodHandler, _iotHubClient);
        }

        /// <summary>
        /// Registers a new delegate that is called for a method that doesn't have a delegate registered for its name. 
        /// If a default delegate is already registered it will replace with the new delegate.
        /// </summary>
        public Task SetMethodDefaultHandlerAsync(MethodCallback methodHandler)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SetMethodDefaultHandlerAsync(methodHandler, _edgeHubClient);
            }
            return _iotHubClient.SetMethodDefaultHandlerAsync(methodHandler, _iotHubClient);
        }

        /// <summary>
        /// Sends an event to device hub
        /// </summary>
        public Task SendEventAsync(Message message)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SendEventAsync(message);
            }
            return _iotHubClient.SendEventAsync(message);
        }

        public async Task SendPropertyAsync(MessageData message, CancellationToken ct)
        {
            TwinCollection reportedPropertiesEdge = new TwinCollection();
            if (message.EventMessageData != null && message.EventMessageData.EventValues.Count > 0)
            {
                foreach (var eventValue in message.EventMessageData.EventValues)
                {
                    reportedPropertiesEdge[eventValue.Name] = eventValue.Value;
                }
            }
            else
            {
                reportedPropertiesEdge[message.DataChangeMessageData.Key] = message.DataChangeMessageData.Value;
            }

            var eventMessage = new Message(Encoding.UTF8.GetBytes(reportedPropertiesEdge.ToJson()));
            eventMessage.Properties["x-reported-properties"] = "true";

            if (_iotHubClient == null)
            {
                await _edgeHubClient.UpdateReportedPropertiesAsync(reportedPropertiesEdge, ct).ConfigureAwait(false);
                await _edgeHubClient.SendEventAsync(eventMessage).ConfigureAwait(false);
            }
            else
            {
                await _iotHubClient.UpdateReportedPropertiesAsync(reportedPropertiesEdge, ct).ConfigureAwait(false);
                await _iotHubClient.SendEventAsync(eventMessage).ConfigureAwait(false);
            }
        }

        public async Task SendSettingAsync(MessageData message, CancellationToken ct)
        {
            TwinCollection reportedPropertiesEdge = new TwinCollection();
            if (message.EventMessageData != null && message.EventMessageData.EventValues.Count > 0)
            {
                foreach (var eventValue in message.EventMessageData.EventValues)
                {
                    reportedPropertiesEdge[eventValue.Name] = new JObject();
                    reportedPropertiesEdge[eventValue.Name]["value"] = eventValue.Value;
                    reportedPropertiesEdge[eventValue.Name]["status"] = IoTCentral_ReportedPropertyStatusCompleted;
                }
            }
            else
            {
                reportedPropertiesEdge[message.DataChangeMessageData.Key] = new JObject();
                reportedPropertiesEdge[message.DataChangeMessageData.Key]["value"] = message.DataChangeMessageData.Value;
                reportedPropertiesEdge[message.DataChangeMessageData.Key]["status"] = IoTCentral_ReportedPropertyStatusCompleted;
            }

            var eventMessage = new Message(Encoding.UTF8.GetBytes(reportedPropertiesEdge.ToJson()));
            eventMessage.Properties["x-reported-properties"] = "true";

            if (_iotHubClient == null)
            {
                await _edgeHubClient.UpdateReportedPropertiesAsync(reportedPropertiesEdge, ct).ConfigureAwait(false);
                await _edgeHubClient.SendEventAsync(eventMessage).ConfigureAwait(false);
            }
            else
            {
                await _iotHubClient.UpdateReportedPropertiesAsync(reportedPropertiesEdge, ct).ConfigureAwait(false);
                await _iotHubClient.SendEventAsync(eventMessage).ConfigureAwait(false);
            }
        }

        public Task SendIoTCEventAsync(Message message, CancellationToken ct)
        {
            if (_iotHubClient == null)
            {
                return _edgeHubClient.SendEventAsync(message);
            }
            return _iotHubClient.SendEventAsync(message);
        }

        private async Task HandleSettingChanged(TwinCollection desiredProperties, object userContext)
        {
            var reportedProperties = new TwinCollection();
            var opcSessions = Program.NodeConfiguration.OpcSessions;
            foreach (var opcSession in opcSessions)
            {
                foreach (var opcSubscription in opcSession.OpcSubscriptions)
                {
                    foreach (var opcMonitoredItem in opcSubscription.OpcMonitoredItems)
                    {
                        var key = opcMonitoredItem.Key;
                        try
                        {
                            if (!desiredProperties.Contains(key))
                            {
                                continue;
                            }

                            //Handle OPC UA Property overwrite and acknowledge setting change
                            //Get JSON Value of desired property which is reported by setting change from IoT Central
                            var jsonValue = new Newtonsoft.Json.Linq.JObject(desiredProperties[key])
                                .GetValue("value").ToString();

                            var session = opcSession.OpcUaClientSession.GetSession();

                            var nodesToRead = new ReadValueIdCollection { new ReadValueId() { NodeId = opcMonitoredItem.ConfigNodeId, AttributeId = Attributes.DataType } };
                            session.Read(
                                null,
                                0,
                                TimestampsToReturn.Neither,
                                nodesToRead,
                                out DataValueCollection attributeResults,
                                out DiagnosticInfoCollection attributeDiagnostics);

                            ClientBase.ValidateResponse(attributeResults, nodesToRead);
                            ClientBase.ValidateDiagnosticInfos(attributeDiagnostics, nodesToRead);

                            var dataType = (uint)((NodeId)attributeResults.First().Value).Identifier;
                            var namespaceIndex = ((NodeId)attributeResults.First().Value).NamespaceIndex;

                            object value = null;

                            try
                            {
                                if (namespaceIndex == 0)
                                {
                                    switch (dataType)
                                    {
                                        case DataTypes.Boolean: value = Convert.ToBoolean(jsonValue); break;
                                        case DataTypes.Byte: value = Convert.ToByte(jsonValue); break;
                                        case DataTypes.DateTime: value = Convert.ToDateTime(jsonValue); break;
                                        case DataTypes.Double: value = Convert.ToDouble(jsonValue); break;
                                        case DataTypes.Float: value = float.Parse(jsonValue); break;
                                        case DataTypes.Guid: value = new Guid(jsonValue); break;
                                        case DataTypes.Int16: value = Convert.ToInt16(jsonValue); break;
                                        case DataTypes.Int32: value = Convert.ToInt32(jsonValue); break;
                                        case DataTypes.Int64: value = Convert.ToInt64(jsonValue); break;
                                        case DataTypes.Integer: value = Convert.ToInt32(jsonValue); break;
                                        case DataTypes.Number: value = Convert.ToInt32(jsonValue); break;
                                        case DataTypes.SByte: value = Convert.ToSByte(jsonValue); break;
                                        case DataTypes.String: value = Convert.ToString(jsonValue); break;
                                        case DataTypes.UInt16: value = Convert.ToUInt16(jsonValue); break;
                                        case DataTypes.UInt32: value = Convert.ToUInt32(jsonValue); break;
                                        case DataTypes.UInt64: value = Convert.ToUInt64(jsonValue); break;
                                        case DataTypes.UInteger: value = Convert.ToUInt32(jsonValue); break;
                                        default: throw new NotSupportedException("Data type is not supported.");
                                    }
                                }
                                else
                                {
                                    // custom data type - hope for the best
                                    value = Convert.ToInt32(jsonValue);
                                    _logger.Warning("Conversion into a custom data type ({DataType}). Int32 assumed.", dataType);
                                }
                            }
                            catch (Exception ex)
                            {
                                var dataTypeName = GetDataTypeName(dataType) ?? dataType.ToString();
                                string errorMessage = "Update of reported properties failed.";
                                if (ex is NotSupportedException)
                                {
                                    errorMessage = $"Data type '{dataTypeName}' is not supported.";
                                }
                                else if (ex is FormatException || ex is OverflowException)
                                {
                                    errorMessage = $"Conversion can not be executed. '{jsonValue}' is not of data type '{dataTypeName}'.";
                                }

                                _logger.Error(ex, $"{nameof(HandleSettingChanged)}: {errorMessage}");

                                reportedProperties[key] = new
                                {
                                    value = desiredProperties[key]["value"],
                                    desiredVersion = desiredProperties["$version"],
                                    message = errorMessage,
                                    status = "failed"
                                };
                                await UpdateReportedPropertiesAsync(reportedProperties);

                                continue;
                            }

                            //Create a new WriteValueCollection to write the new information to OPC UA Server
                            var valuesToWrite = new WriteValueCollection();
                            var newValueToWrite = new WriteValue
                            {
                                NodeId = opcMonitoredItem.ConfigNodeId,
                                AttributeId = opcMonitoredItem.AttributeId,
                                Value = new DataValue
                                {
                                    Value = value,
                                    ServerTimestamp = DateTime.MinValue,
                                    SourceTimestamp = DateTime.MinValue
                                }
                            };
                            valuesToWrite.Add(newValueToWrite);

                            opcSubscription.OpcUaClientSubscription.Subscription.Session.Write(
                                null,
                                valuesToWrite,
                                out var results,
                                out var diagnosticInfos);
                            ClientBase.ValidateResponse(results, valuesToWrite);
                            ClientBase.ValidateDiagnosticInfos(diagnosticInfos, valuesToWrite);

                            string status, message;
                            if (StatusCode.IsBad(results[0]))
                            {
                                _logger.Error($"[{results[0].ToString()}]: Cannot write Setting value of Monitored Item with NodeId {opcMonitoredItem.Id} and Key {opcMonitoredItem.Key}");
                                status = IoTCentral_ReportedPropertyStatusFailed;
                                message = $"Failure during synchronizing OPC UA Values, Reason: {results[0].ToString()}";
                            }
                            else
                            {
                                status = IoTCentral_ReportedPropertyStatusCompleted;
                                message = string.Empty;
                            }


                            reportedProperties[key] = new
                            {
                                value = desiredProperties[key]["value"],
                                status,
                                desiredVersion = desiredProperties["$version"],
                                message
                            };

                            await UpdateReportedPropertiesAsync(reportedProperties);
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, "Error while updating reported Setting.");

                            reportedProperties[key] = new
                            {
                                value = desiredProperties[key]["value"],
                                desiredVersion = desiredProperties["$version"],
                                status = IoTCentral_ReportedPropertyStatusFailed,
                                message = $"Failure during synchronizing OPC UA Values, Reason: {e.Message}"
                            };

                            await UpdateReportedPropertiesAsync(reportedProperties);
                        }
                    }
                }
            }
        }

        public async Task<MethodResponse> DefaultCommandHandlerAsync(MethodRequest methodRequest, object userContext)
        {
            string logPrefix = "DefaultCommandHandlerAsync:";
            string message = $"Method '{methodRequest.Name}' successfully received. Started to handle Command.";
            _logger.Information($"{logPrefix} {message}");
            string resultString = null;
            HttpStatusCode resultStatusCode = HttpStatusCode.NoContent;

            var opcSessions = Program.NodeConfiguration.OpcSessions;
            foreach (var opcSession in opcSessions)
            {
                foreach (var opcSubscription in opcSession.OpcSubscriptions)
                {
                    foreach (var opcMonitoredItem in opcSubscription.OpcMonitoredItems)
                    {
                        if (opcMonitoredItem.Key == methodRequest.Name)
                        {
                            var inputArguments = new List<object>();
                            var parameterDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(methodRequest.DataAsJson);
                            var session = opcSession.OpcUaClientSession.GetSession();

                            var references = session.FetchReferences(opcMonitoredItem.ConfigNodeId);
                            var nodeValue = session.ReadValue(references.Where(w => w.DisplayName == "InputArguments").SingleOrDefault().NodeId.ToString());

                            foreach (var param in parameterDictionary)
                            {
                                foreach (var inputArgument in ((Opc.Ua.ExtensionObject[])nodeValue.Value))
                                {
                                    var paramKey = HttpUtility.UrlDecode(param.Key);

                                    var opcUaArgument = (Argument)inputArgument.Body;
                                    if (string.Compare(paramKey, opcUaArgument.Name, true, CultureInfo.InvariantCulture) != 0)
                                    {
                                        continue;
                                    }

                                    if (opcUaArgument.DataType == DataTypeIds.UInt16)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToUInt16(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.UInt32)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToUInt32(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.UInt64)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToUInt64(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Int16)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt16(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Int32 ||
                                        opcUaArgument.DataType == DataTypeIds.Integer ||
                                        opcUaArgument.DataType == DataTypeIds.Number)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt32(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Int64)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt64(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Float)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToSingle(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.Double)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToDouble(param.Value)));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.String ||
                                        opcUaArgument.DataType == DataTypeIds.LocalizedText ||
                                        opcUaArgument.DataType == DataTypeIds.XmlElement ||
                                        opcUaArgument.DataType == DataTypeIds.QualifiedName ||
                                        opcUaArgument.DataType == DataTypeIds.DateTime)
                                    {
                                        inputArguments.Add(new Variant(param.Value));
                                    }
                                    else if (opcUaArgument.DataType == DataTypeIds.ServerState ||
                                        opcUaArgument.DataType == DataTypeIds.RedundancySupport ||
                                        opcUaArgument.DataType == DataTypeIds.NamingRuleType ||
                                        opcUaArgument.DataType == DataTypeIds.IdType ||
                                        opcUaArgument.DataType == DataTypeIds.NodeClass)
                                    {
                                        inputArguments.Add(new Variant(Convert.ToInt32(param.Value)));
                                    }
                                    else
                                    {
                                        var errorMessage = $"{logPrefix}: DataType {opcUaArgument.DataType.ToString()} is not implemented as input parameter yet. " +
                                            $"It will be added as normal string, if your method call fails it is currently not supported.";
                                        _logger.Error(errorMessage);
                                        resultString = errorMessage;
                                        resultStatusCode = HttpStatusCode.NotImplemented;
                                        inputArguments.Add(new Variant(param.Value));
                                    }
                                }
                            }

                            if (string.IsNullOrEmpty(resultString))
                            {
                                try
                                {
                                    opcSubscription.OpcUaClientSubscription.Subscription.Session.Browse(
                                        null,
                                        null,
                                        opcMonitoredItem.ConfigNodeId,
                                        0u,
                                        BrowseDirection.Inverse,
                                        ReferenceTypeIds.HierarchicalReferences,
                                        true,
                                        (uint)NodeClass.Object,
                                        out _,
                                        out var parentNodes);

                                    var parentNode = parentNodes.FirstOrDefault();
                                    if (parentNode == null)
                                    {
                                        resultString = "Failed to execute method";
                                        resultStatusCode = HttpStatusCode.InternalServerError;
                                    }
                                    else
                                    {
                                        var methodResult = opcSubscription.OpcUaClientSubscription.Subscription.Session.Call(
                                            new NodeId(parentNode.NodeId.Identifier, parentNode.NodeId.NamespaceIndex),
                                            opcMonitoredItem.ConfigNodeId, inputArguments.ToArray());

                                        var methodResultString = string.Join(Environment.NewLine,
                                            methodResult.Select(r => r.ToString()));

                                        resultString = $"Successfully executed method {methodRequest.Name}{Environment.NewLine}Result:{Environment.NewLine}{methodResultString}";
                                        resultStatusCode = HttpStatusCode.OK;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    resultString = ex.Message;
                                    resultStatusCode = HttpStatusCode.InternalServerError;
                                }
                            }
                        }
                    }
                }
            }

            // Response messages in IoT Central must be sent via reported properties
            var reportedProperties = new TwinCollection();
            reportedProperties[methodRequest.Name] = new
            {
                value = resultString
            };
            await UpdateReportedPropertiesAsync(reportedProperties);

            return new MethodResponse((int)resultStatusCode);
        }

        private async Task UpdateReportedPropertiesAsync(TwinCollection reportedProperties)
        {
            var eventMessage = new Message(Encoding.UTF8.GetBytes(reportedProperties.ToJson()));
            eventMessage.Properties["x-reported-properties"] = "true";

            if (_iotHubClient == null)
            {
                await _edgeHubClient.UpdateReportedPropertiesAsync(reportedProperties).ConfigureAwait(false);
                await _edgeHubClient.SendEventAsync(eventMessage).ConfigureAwait(false);
            }
            else
            {
                await _iotHubClient.UpdateReportedPropertiesAsync(reportedProperties).ConfigureAwait(false);
                await _iotHubClient.SendEventAsync(eventMessage).ConfigureAwait(false);
            }
        }

        private async Task<MethodResponse> DesiredPropertiesUpdated(MethodRequest methodRequest, object userContext)
        {
            await HandleSettingChanged(new TwinCollection(methodRequest.DataAsJson), null);
            return new MethodResponse((int)HttpStatusCode.OK);
        }

        private string GetDataTypeName(uint dataTypeValue)
        {
            var typeName = typeof(DataTypes).GetFields(BindingFlags.Static | BindingFlags.Public)
                                            .SingleOrDefault(field => (uint)field.GetValue(null) == dataTypeValue)
                                            ?.Name;

            return typeName;
        }

        private readonly Logger _logger;
        private static DeviceClient _iotHubClient;
        private static ModuleClient _edgeHubClient;
    }
}
