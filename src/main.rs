use rumqtt::{MqttClient, MqttOptions, QoS, Notification};
use std::{thread, time::Duration};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use dashmap::DashMap;

fn foo(payload: Arc<Vec<u8>>)
// fn foo(payload: String)
{
    println!("Got foo message: {:?}", payload);
}
fn bar(payload: Arc<Vec<u8>>)
// fn bar(payload: String)
{
    println!("Got bar message: {:?}", payload);
}

pub fn spawn_message_loop (callbacks: &Arc<RwLock<HashMap<String, CallbackType>>>, 
                           notifications: crossbeam_channel::Receiver<Notification>)
{
    let callbacks = callbacks.clone();
    thread::spawn(move || {
        for notification in notifications {
            let callbacks = callbacks.read().unwrap();
            match notification {
                Notification::Publish(msg) => {
                    let topic = msg.topic_name;
                    match callbacks.get(&topic)
                    {
                        Some(cb) => cb(msg.payload),
                        None => {
                            println!("Topic not in list of callbacks");
                            continue;
                        }
                    }
                }
                _ => println!("Ignoring notification")
            }
        }
    });
}
type CallbackType = Box<dyn Send + Sync +'static + Fn(Arc<Vec<u8>>)>;
struct Comm {
    client: MqttClient,
    callbacks: Arc<RwLock<HashMap<String, CallbackType>>>,
}
impl Comm {
    pub fn new() -> Self {
        let mqtt_options = MqttOptions::new("test-pubsub1", "localhost", 1883);
        let (mqtt_client, notifications) = MqttClient::start(mqtt_options).unwrap();
        let callbacks = Arc::new(RwLock::new(HashMap::new()));
        spawn_message_loop(&callbacks, notifications);
        Self {
            client: mqtt_client,
            callbacks: callbacks
        }
    }
    pub fn subscribe(&mut self, topic: String, callback: CallbackType)
    {
        println!("Subscribng to {}", topic);
        let callbacks = self.callbacks.clone();
        let mut callbacks = callbacks.write().unwrap();
        callbacks.insert(topic.clone(), callback);
        self.client.subscribe(topic, QoS::AtLeastOnce).unwrap();
    }
    
}
struct Application<'a> {
    comm: &'a mut Comm
}
impl<'a> Application<'a> {
    pub fn new(comm: &'a mut Comm) -> Self
    {
        Application {
            comm
        }
    }
    pub fn setup(&mut self)
    {
        self.comm.subscribe("hello/foo".to_string(), Box::new(& |payload| {
            foo(payload);
        }) );
        self.comm.subscribe("hello/bar".to_string(), Box::new(&bar));
    }
}
fn main() 
{
    let mut comm = Comm::new();
    let mut app = Application::new(&mut comm);
    app.setup();

    loop {
        thread::park();
    }
    // mqtt_client.subscribe("hello/foo", QoS::AtLeastOnce).unwrap();
    // mqtt_client.subscribe("hello/bar", QoS::AtLeastOnce).unwrap();
    // let sleep_time = Duration::from_secs(1);
    // thread::spawn(move || {
    //     for i in 0..100 {
    //         let payload = format!("publish {}", i);
    //         thread::sleep(sleep_time);
    //         mqtt_client.publish("hello/world", QoS::AtLeastOnce, false, payload).unwrap();
    //     }
    // });

    // let mut callbacks: HashMap<String, fn(Arc<Vec<u8>>) -> ()> = HashMap::new();
    // callbacks.insert("hello/foo".to_string(), foo);
    // callbacks.insert("hello/bar".to_string(), bar);

    // for notification in notifications {
    //     match notification {
    //         Notification::Publish(msg) => {
    //             let topic = msg.topic_name;
    //             // callbacks.get(topic).map(|cb| cb(msg.payload));
    //             match callbacks.get(&topic)
    //             {
    //                 Some(cb) => cb(msg.payload),
    //                 None => {
    //                     println!("Topic not in list of callbacks");
    //                     continue;
    //                 }
    //             }
    //         }
    //         _ => println!("Ignoring notification")
    //     }
    // }
}
// use boxfnonce::SendBoxFnOnce;
// // type  Callback = SendBoxFnOnce<'static, Arc<Vec<u8>>, ()>;
// // type  Callback = SendBoxFnOnce<'static, String, ()>;
// type Callback = Box<dyn Send + Sync + Fn(String) + 'static>;
// fn main() {
//     let callbacks = Arc::new(DashMap::<String, Callback>::new());
//     let cb = callbacks.clone();
//     thread::spawn(move || {
//         for i in 1..10
//         {
//             cb.get("hello/foo").map(|f| f("foo.bar".to_string()));
//             thread::sleep(Duration::from_millis(1000));
//         }
//     });

//     {
//         let cb2 = callbacks.clone();
//         callbacks.insert("hello/foo".to_string(), Box::new(foo));
//         callbacks.insert("hello/bar".to_string(), Box::new(bar));
//     }
// }