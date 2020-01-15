using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using Serilog;

namespace OpcPublisher
{

    public class IoTCEventsProcessor : IDisposable
    {
        private const string CONTENT_TYPE_OPCUAJSON = "application/opcua+uajson";
        private const string CONTENT_ENCODING_UTF8 = "UTF-8";
        private readonly ILogger _logger;
        private readonly IHubClient _hubClient;
        private static BlockingCollection<MessageData> _monitoredIoTcEventDataQueue;
        private uint _hubMessageSize;
        private uint _hubMessageSizeMax;
        private int _timeout;
        private CancellationToken _shutdownToken;
        private int _enqueueFailureCount;
        private int _enqueueCount;
        private string _jsonMessage;
        private bool _needToBufferMessage;
        private int _jsonMessageSize;
        private uint _hubMessageBufferSize;
        private MemoryStream _hubMessage;
        private bool _singleMessageSend;

        /// <summary>
        /// Number of times we were not able to make the IoTCentral send interval, because too high load.
        /// </summary>
        public static long MissedIoTcSendIntervalCount { get; set; }

        /// <summary>
        /// Number of times we were not able to sent the event message as IoT Central event to the cloud.
        /// </summary>
        public static long FailedIoTcMessages { get; set; }

        /// <summary>
        /// Number of payload bytes we sent to the cloud.
        /// </summary>
        public static long SentIoTcBytes { get; set; }

        /// <summary>
        /// Number of properties we sent to the cloud using deviceTwin
        /// </summary>
        public static long SentIoTcEvents { get; set; }

        /// <summary>
        /// Number of times the isze fo the event payload was too large for a telemetry message.
        /// </summary>
        public static long TooLargeCount { get; set; }

        /// <summary>
        /// Specifies the queue capacity for monitored iot central events.
        /// </summary>
        public static int MonitoredSettingsIoTcEventCapacity { get; set; } = 8192;

        public IoTCEventsProcessor(ILogger logger, IHubClient hubClient, uint hubMessageSize, uint hubMessageSizeMax, int timeout, CancellationToken shutdownToken)
        {
            _logger = logger;
            _hubClient = hubClient;
            _hubMessageSize = hubMessageSize;
            _hubMessageSizeMax = hubMessageSizeMax;
            _timeout = timeout;
            _shutdownToken = shutdownToken;
            _monitoredIoTcEventDataQueue = new BlockingCollection<MessageData>(MonitoredSettingsIoTcEventCapacity);

            Initialize();
        }

        public void EnqueueEvent(MessageData message)
        {
            Interlocked.Increment(ref _enqueueCount);
            if (_monitoredIoTcEventDataQueue.TryAdd(message))
                return;
            Interlocked.Increment(ref _enqueueFailureCount);
            if (_enqueueFailureCount % 10000 == 0)
            {
                _logger.Information($"The internal monitored setting message queue is above its capacity of {_monitoredIoTcEventDataQueue.BoundedCapacity}. We have already lost {_enqueueFailureCount} monitored item notifications:(");
            }
        }

        public async Task MonitoredIoTCEventsProcessorAsync()
        {
            DateTime nextSendTime = DateTime.UtcNow + TimeSpan.FromSeconds(_timeout);
            double millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
            EventMessageData eventMessageData = null;

            while (!_shutdownToken.IsCancellationRequested)
            {
                try
                {
                    // sanity check the send interval, compute the timeout and get the next monitored item message
                    if (_timeout > 0)
                    {
                        millisecondsTillNextSend = nextSendTime.Subtract(DateTime.UtcNow).TotalMilliseconds;
                        if (millisecondsTillNextSend < 0)
                        {
                            // do not wait if we missed the send interval
                            millisecondsTillNextSend = 0;
                        }
                    }
                    else
                    {
                        // if we are in shutdown do not wait, else wait infinite if send interval is not set
                        millisecondsTillNextSend = _shutdownToken.IsCancellationRequested ? 0 : Timeout.Infinite;
                    }

                    var gotItem = _monitoredIoTcEventDataQueue.TryTake(out MessageData messageData, (int)millisecondsTillNextSend, _shutdownToken);

                    // the two commandline parameter --ms (message size) and --si (send interval) control when data is sent to IoTHub/EdgeHub
                    // pls see detailed comments on performance and memory consumption at https://github.com/Azure/iot-edge-opc-publisher

                    // check if we got an item or if we hit the timeout or got canceled
                    if (gotItem)
                    {
                        eventMessageData = messageData?.EventMessageData;

                        if (eventMessageData != null)
                        {
                            // for IoTCentral we send simple key/value pairs. key is the DisplayName, value the value.
                            _jsonMessage = await CreateIoTCentralJsonForEventChangeAsync(eventMessageData, _shutdownToken).ConfigureAwait(false);

                            _jsonMessageSize =
                                Encoding.UTF8.GetByteCount(_jsonMessage.ToString(CultureInfo.InvariantCulture));

                            // sanity check that the user has set a large enough messages size
                            if ((_hubMessageSize > 0 && _jsonMessageSize > _hubMessageSize) ||
                                (_hubMessageSize == 0 && _jsonMessageSize > _hubMessageBufferSize))
                            {
                                _logger.Error(
                                    $"There is a IoT Central event message (size: {_jsonMessageSize}), which will not fit into an hub message (max size: {_hubMessageBufferSize}].");
                                _logger.Error(
                                    $"Please check your hub message size settings. The IoT Central event message will be discarded silently. Sorry:(");
                                TooLargeCount++;
                                continue;
                            }

                            // if batching is requested or we need to send at intervals, batch it otherwise send it right away
                            _needToBufferMessage = false;
                            if (_hubMessageSize > 0 || (_hubMessageSize == 0 && _timeout > 0))
                            {
                                // if there is still space to batch, do it. otherwise send the buffer and flag the message for later buffering
                                if (_hubMessage.Position + _jsonMessageSize + 1 <= _hubMessage.Capacity)
                                {
                                    // add the message and a comma to the buffer
                                    _hubMessage.Write(
                                        Encoding.UTF8.GetBytes(_jsonMessage.ToString(CultureInfo.InvariantCulture)),
                                        0, _jsonMessageSize);
                                    _hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                                    _logger.Debug(
                                        $"Added new IoT Central event message with size {_jsonMessageSize} to hub message (size is now {(_hubMessage.Position - 1)}).");
                                    continue;
                                }
                                else
                                {
                                    _needToBufferMessage = true;
                                }
                            }
                        }
                        else
                        {
                            _logger.Error("EventMessageData is null, there is a problem getting OPC UA Event data.");
                        }
                    }
                    else
                    {
                        // if we got no message, we either reached the interval or we are in shutdown and have processed all messages
                        if (_shutdownToken.IsCancellationRequested)
                        {
                            _logger.Information($"Cancellation requested.");
                            _monitoredIoTcEventDataQueue.CompleteAdding();
                            _monitoredIoTcEventDataQueue.Dispose();
                            return;
                        }
                    }

                    // the batching is completed or we reached the send interval or got a cancelation request
                    try
                    {
                        Microsoft.Azure.Devices.Client.Message encodedhubMessage = null;

                        // if we reached the send interval, but have nothing to send (only the opening square bracket is there), we continue
                        if (!gotItem && _hubMessage.Position == 1)
                        {
                            nextSendTime += TimeSpan.FromSeconds(_timeout);
                            _hubMessage.Position = 0;
                            _hubMessage.SetLength(0);
                            if (!_singleMessageSend)
                            {
                                _hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                            }
                            continue;
                        }

                        // if there is no batching and no send interval configured, we send the JSON message we just got, otherwise we send the buffer
                        if (_singleMessageSend)
                        {
                            // create the message without brackets
                            encodedhubMessage = new Message(Encoding.UTF8.GetBytes(_jsonMessage.ToString(CultureInfo.InvariantCulture)));
                        }
                        else
                        {
                            // remove the trailing comma and add a closing square bracket
                            _hubMessage.SetLength(_hubMessage.Length - 1);
                            _hubMessage.Write(Encoding.UTF8.GetBytes("]"), 0, 1);
                            encodedhubMessage = new Message(_hubMessage.ToArray());
                        }
                        if (_hubClient != null)
                        {
                            encodedhubMessage.ContentType = CONTENT_TYPE_OPCUAJSON;
                            encodedhubMessage.ContentEncoding = CONTENT_ENCODING_UTF8;
                            encodedhubMessage.Properties[HubCommunicationBase.MessageSchemaPropertyName] = HubCommunicationBase.MessageSchemaIotCentral;

                            nextSendTime += TimeSpan.FromSeconds(_timeout);
                            try
                            {
                                encodedhubMessage.Properties["endpointId"] = eventMessageData.EndpointId;
                                SentIoTcBytes += encodedhubMessage.GetBytes().Length;
                                await _hubClient.SendEventAsync(encodedhubMessage).ConfigureAwait(false);
                                SentIoTcEvents++;
                                _logger.Debug($"Sending {encodedhubMessage.BodyStream.Length} bytes to hub.");
                                _logger.Debug($"Message sent was: {_jsonMessage}");
                            }
                            catch
                            {
                                FailedIoTcMessages++;
                            }

                            // reset the messaage
                            _hubMessage.Position = 0;
                            _hubMessage.SetLength(0);
                            if (!_singleMessageSend)
                            {
                                _hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
                            }

                            // if we had not yet buffered the last message because there was not enough space, buffer it now
                            if (_needToBufferMessage)
                            {
                                // add the message and a comma to the buffer
                                _hubMessage.Write(Encoding.UTF8.GetBytes(_jsonMessage.ToString(CultureInfo.InvariantCulture)), 0, _jsonMessageSize);
                                _hubMessage.Write(Encoding.UTF8.GetBytes(","), 0, 1);
                            }
                        }
                        else
                        {
                            _logger.Information("No hub client available. Dropping messages...");
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, "Exception while sending message to hub. Dropping message...");
                    }
                }
                catch (Exception e)
                {
                    if (!(e is OperationCanceledException))
                    {
                        _logger.Error(e, "Error while processing monitored item messages.");
                    }
                }
            }
        }

        public void Dispose()
        {
            _hubMessage?.Dispose();
        }

        private void Initialize()
        {
            uint jsonSquareBracketLength = 2;
            Message tempMsg = new Message();
            // the system properties are MessageId (max 128 byte), Sequence number (ulong), ExpiryTime (DateTime) and more. ideally we get that from the client.
            int systemPropertyLength = 128 + sizeof(ulong) + tempMsg.ExpiryTimeUtc.ToString(CultureInfo.InvariantCulture).Length;
            int applicationPropertyLength = Encoding.UTF8.GetByteCount($"iothub-content-type={CONTENT_TYPE_OPCUAJSON}") + Encoding.UTF8.GetByteCount($"iothub-content-encoding={CONTENT_ENCODING_UTF8}");
            // if batching is requested the buffer will have the requested size, otherwise we reserve the max size

            var reservedBufferSize = (uint)systemPropertyLength + jsonSquareBracketLength + (uint)applicationPropertyLength;

            if (_hubMessageSize > 0 && _hubMessageSize < reservedBufferSize)
            {
                throw new ArgumentException($"value cannot be smaller than {reservedBufferSize}", nameof(_hubMessageSize));
            }

            if (_hubMessageSize <= 0 && _hubMessageSizeMax < reservedBufferSize)
            {
                throw new ArgumentException($"value cannot be smaller than {reservedBufferSize}", nameof(_hubMessageSizeMax));
            }

            _hubMessageBufferSize = (_hubMessageSize > 0 ? _hubMessageSize : _hubMessageSizeMax) - reservedBufferSize;

            byte[] hubMessageBuffer = new byte[_hubMessageBufferSize];
            _hubMessage = new MemoryStream(hubMessageBuffer);
            _singleMessageSend = _timeout == 0 && _hubMessageSize == 0;

            _jsonMessage = string.Empty;
            _needToBufferMessage = false;
            _jsonMessageSize = 0;

            _hubMessage.Position = 0;
            _hubMessage.SetLength(0);
            if (!_singleMessageSend)
            {
                _hubMessage.Write(Encoding.UTF8.GetBytes("["), 0, 1);
            }
        }

        /// <summary>
        /// Creates an IoTCentral JSON message for a event change notification, based on the event configuration for the endpoint.
        /// </summary>
        private async Task<string> CreateIoTCentralJsonForEventChangeAsync(EventMessageData messageData, CancellationToken shutdownToken)
        {
            try
            {
                // build the JSON message for IoTCentral
                StringBuilder jsonStringBuilder = new StringBuilder();
                StringWriter jsonStringWriter = new StringWriter(jsonStringBuilder);
                using (JsonWriter jsonWriter = new JsonTextWriter(jsonStringWriter))
                {
                    await jsonWriter.WriteStartObjectAsync(shutdownToken).ConfigureAwait(false);
                    await jsonWriter.WritePropertyNameAsync(messageData.Key, shutdownToken).ConfigureAwait(false);
                    var eventValues = string.Join(",", messageData.EventValues.Select(s => new {
                        s.Name,
                        s.Value
                    }));
                    await jsonWriter.WriteValueAsync(eventValues, shutdownToken).ConfigureAwait(false);
                    await jsonWriter.WritePropertyNameAsync("messageType", shutdownToken).ConfigureAwait(false);
                    await jsonWriter.WriteValueAsync("event", shutdownToken).ConfigureAwait(false);
                    await jsonWriter.WriteEndObjectAsync(shutdownToken).ConfigureAwait(false);
                    await jsonWriter.FlushAsync(shutdownToken).ConfigureAwait(false);
                }
                return jsonStringBuilder.ToString();
            }
            catch (Exception e)
            {
                _logger.Error(e, "Generation of IoTCentral JSON message failed.");
            }
            return string.Empty;
        }
    }
}