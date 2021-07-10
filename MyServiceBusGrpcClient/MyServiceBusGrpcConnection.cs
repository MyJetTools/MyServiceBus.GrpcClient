using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MyServiceBus.GrpcContracts;
using ProtoBuf.Grpc.Client;

namespace MyServiceBusGrpcClient
{
    public class MyServiceBusGrpcConnection
    {
        private readonly string _appName;
        private string _clientVersion;

        private readonly Func<string> _getEngineUrl;


        private MyServiceBusConnectionInstance _connectionInstance;

        private Action<Exception> _handleException;

        private Action<string> _connectionEvents;

        private readonly Dictionary<string, string> _topicsToCreateOnConnect = new Dictionary<string, string>();

        private readonly object _lockObject = new object();

        public int MaxPayloadSize { get; set; } = 1024 * 1024 * 4;


        private void PopulateVersion()
        {
            try
            {
                _clientVersion = GetType().Assembly.GetName().Version.ToString();
            }
            catch
            {
                _clientVersion = "Unknown";
            }
        }

        public MyServiceBusGrpcConnection(string appName, Func<string> getEngineUrl)
        {
            _appName = appName;
            _getEngineUrl = getEngineUrl;
            PopulateVersion();
        }

        public MyServiceBusGrpcConnection HandleException(Action<Exception> handleExceptionCallback)
        {
            _handleException = handleExceptionCallback;
            return this;
        }

        public MyServiceBusGrpcConnection SubscribeToConnectionEvents(Action<string> connectionEvents)
        {
            _connectionEvents = connectionEvents;
            return this;
        }

        public MyServiceBusGrpcConnection CreateTopicIfNotExists(string topicId)
        {
            _topicsToCreateOnConnect.Add(topicId, topicId);
            return this;
        }


        private Task _keepConnectionLoop;

        private async Task CreateTopicsAsync(MyServiceBusConnectionInstance instance)
        {
            foreach (var topicId in _topicsToCreateOnConnect.Keys)
            {
                await instance.MyServiceBusGrpcPublisher.CreateTopicIfNotExistsAsync(new CreateTopicGrpcContract
                {
                    SessionId = instance.SessionId,
                    TopicId = topicId
                });
            }
        }


        private void NotifyConnectionEvent(string message)
        {
            if (_connectionEvents == null)
            {
                Console.WriteLine(message);
            }
            else
            {
                _connectionEvents(message);
            }
        }


        private async Task ConnectionPingLoopAsync(MyServiceBusConnectionInstance instance)
        {
            while (true)
            {
                var pingResult = await instance.PingAsync();
                if (!pingResult)
                {
                    NotifyConnectionEvent($"Ping detected not connection to {instance.GrpcUrl} with SessionId {instance.SessionId}");
                    return;
                }

                await Task.Delay(3000);
            }
            
        }

        private async Task KeepConnectionLoop()
        {
            while (true)
            {

                try
                {
                    var grpcUrl = _getEngineUrl();
                    
                    var grpcSession = GrpcChannel
                        .ForAddress(grpcUrl)
                        .CreateGrpcService<IMyServiceBusGrpcSessions>();

                    var newSessionResult = await grpcSession.GreetingAsync(new GreetingGrpcRequest
                    {
                        Name = _appName,
                        ClientVersion = _clientVersion
                    });

                    NotifyConnectionEvent($"Connection is established to {grpcUrl} with SessionId {newSessionResult.SessionId}");
                    
                    var grpcPublisher = GrpcChannel
                        .ForAddress(grpcUrl)
                        .CreateGrpcService<IMyServiceBusGrpcPublisher>();

                    var instance =
                        new MyServiceBusConnectionInstance(grpcPublisher, grpcSession, newSessionResult.SessionId, grpcUrl);

                    await CreateTopicsAsync(instance);

                    _connectionInstance = instance;

                    await ConnectionPingLoopAsync(instance);
                }
                catch (Exception e)
                {
                    

                    if (_handleException == null)
                        Console.WriteLine(e);
                    else
                        _handleException(e);
                }
                finally
                {


                    if (_connectionInstance != null)
                    {
                        NotifyConnectionEvent($"Connection to {_connectionInstance.GrpcUrl} with SessionId {_connectionInstance.SessionId}");   
                    }
                    
                    _connectionInstance = null;

                    await Task.Delay(3000);
                }
            }

        }


        private static List<MessageHeaderGrpcModel> GetHeadersGrpcModel(Dictionary<string, string> headers)
        {
            if (headers == null)
            {
                return new List<MessageHeaderGrpcModel>();
            }
            
            return headers.Select(itm => new MessageHeaderGrpcModel
            {
                Key = itm.Key,
                Value = itm.Value
            }).ToList();
        }


        public async ValueTask PublishMessageAsync(string topicId, byte[] messageContent, Dictionary<string, string> headers = null, bool persistImmediately = false)
        {
            var connectionInstance = _connectionInstance;

            if (connectionInstance == null)
                throw new Exception("There is no connection to ServiceBus");

            var contract = new MessagesToPublishGrpcContract
            {
                TopicId = topicId,
                PersistImmediately = persistImmediately,
                Messages = new List<MessageContentGrpcModel>
                {
                    new MessageContentGrpcModel
                    {
                        Headers = GetHeadersGrpcModel(headers),
                        Content = messageContent
                    }
                }
            };

            await connectionInstance.MyServiceBusGrpcPublisher.PublishAsync(contract, MaxPayloadSize);
        }
        
        public async ValueTask PublishMessagesAsync(string topicId, IEnumerable<(byte[] content, Dictionary<string, string> headers)> messages, bool persistImmediately = false)
        {

            var connectionInstance = _connectionInstance;

            if (connectionInstance == null)
                throw new Exception("There is no connection to ServiceBus");


            var contract = new MessagesToPublishGrpcContract
            {
                TopicId = topicId,
                PersistImmediately = persistImmediately,
                Messages = messages.Select(itm => new MessageContentGrpcModel
                {
                    Content = itm.content,
                    Headers = GetHeadersGrpcModel(itm.headers)
                }).ToList()
            };

            await connectionInstance.MyServiceBusGrpcPublisher.PublishAsync(contract, MaxPayloadSize);
        }

        public void Start()
        {
            lock (_lockObject)
            {
                if (_keepConnectionLoop != null)
                    throw new Exception("Connection is already started");
                
                _keepConnectionLoop = KeepConnectionLoop();    
            }
 
        }
        
    }
}