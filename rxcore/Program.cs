using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
//reactive ref
using System.Reactive.Linq;

//mqtt lib and dependencies
using M2Mqtt;
using M2Mqtt.Messages;
using Newtonsoft.Json;
using System.Threading;
using System.Text;

namespace rxcore
{
    public class Program
    {
        #region Constants
        //host url
        const string MQTT_BROKER_ADDRESS = "cloud.makestro.com";
        //device id
        const string MQTT_ClientId = "SimulatorDevice1";
        //devices/{device_id}/messages/events/
        const string TopicPublish = "mifmasterz/simulateddevice/data";

        //devices/{device_id}/messages/devicebound/#
        const string TopicSubscribe = "mifmasterz/simulateddevice/data";
        //SharedAccessSignature sig={signature-string}&se={expiry}&sr={URL-encoded-resourceURI}
        const string MQTT_Pass = "123qweasd";

        //{iothubhostname}/{device_id}/api-version=2016-11-14
        const string MQTT_User = "mifmasterz";

        #endregion

        static MqttClient client;
        public static void Main(string[] args)
        {
            //setup mqtt client
            SetupMqtt();
            //run simulator in another thread
            Task simulatedSensor = new Task(() => LoopDeviceSimulator());
            simulatedSensor.Start();
            //filter if mqtt data is coming
            var FilteredSensorData = 
            from x in WhenDataReceived
            let node = JsonConvert.DeserializeObject<SensorData>(new string(Encoding.UTF8.GetChars(x.Message)))
            where node.Temp>50
            select x;
            //create window for 5 seconds
            IObservable<IObservable<MqttMsgPublishEventArgs>> WindowedData = FilteredSensorData
            .Window(() =>
            {
                IObservable<long> seqWindowControl = Observable.Interval(TimeSpan.FromSeconds(6));
                return seqWindowControl;
            });
            ConcurrentBag<int> TempRate = new ConcurrentBag<int>();
            //subscribe
            WindowedData
            .Subscribe(seqwindow => {
                Console.WriteLine($"Data from {DateTime.Now.AddSeconds(-5)} to {DateTime.Now}");
                if(TempRate.Count>0){
                    Console.WriteLine($"average temperature in 5 secs: {TempRate.Count} items at {TempRate.Average()}");
                    int someItem;
                    while (!TempRate.IsEmpty) 
                    {
                    TempRate.TryTake(out someItem);
                    }
                }
                seqwindow.Subscribe(e =>{
                var msg = new String(Encoding.UTF8.GetChars(e.Message));
                Console.WriteLine($"{e.Topic} -> {msg}");
                var node = JsonConvert.DeserializeObject<SensorData>(msg);
                TempRate.Add(node.Temp);
                });
            });
            //infinite delay
            Thread.Sleep(Timeout.Infinite);
            
        }

        static void LoopDeviceSimulator()
        {
            Random rnd = new Random(Environment.TickCount);
            while (true)
            {
                SensorData newData = new SensorData();
                newData.DeviceName = "SimulatorDevice1";
                newData.Humid = rnd.Next(100);
                newData.Temp = rnd.Next(100);
                newData.Light = rnd.Next(1000);
                newData.Gas = rnd.Next(100);
                newData.Created = DateTime.Now;
                Console.WriteLine($"send data - temp:{newData.Temp}");
                client.Publish(TopicPublish, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(newData)), MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE, false);
                Thread.Sleep(2000);
            }
        }

        //public static event EventHandler<MqttMsgPublishEventArgs> MqttEvent;
        static void SetupMqtt()
        {
            // create client instance
            client = new MqttClient(MQTT_BROKER_ADDRESS);

            // register to message received - don't need this
            //client.MqttMsgPublishReceived += client_MqttMsgPublishReceived;
            
            string clientId = "SIMULATED-DEVICE";
            client.Connect(clientId,MQTT_User,MQTT_Pass);

            client.Subscribe(new string[] { TopicSubscribe }, new byte[] { MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE });

        }
        //replace event with RX
        public static IObservable<MqttMsgPublishEventArgs> WhenDataReceived
        {
            get
            {
                return Observable.FromEventPattern<MqttClient.MqttMsgPublishEventHandler, MqttMsgPublishEventArgs>(
                        h => client.MqttMsgPublishReceived += h,
                        h => client.MqttMsgPublishReceived -= h)
                    .Select(x => x.EventArgs);
            }
        }
    }

    //class for storing sensor data
    public class SensorData
    {
        public string DeviceName { set; get; }
        public int Temp { set; get; }
        public int Humid { set; get; }
        public int Light { set; get; }
        public int Gas { set; get; }
        public DateTime Created { set; get; }
    }
}
