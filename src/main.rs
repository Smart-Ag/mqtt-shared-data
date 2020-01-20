use rumqtt::{MqttClient, MqttOptions, QoS, Notification};
use std::{thread, time::Duration};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use dashmap::DashMap;

fn foo(payload: Arc<Vec<u8>>, shared: i32)
// fn foo(payload: String)
{
    println!("Got foo message: {:?}, {:?}", payload, shared);
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
    comm: &'a mut Comm,
    shared_data: Arc<RwLock<i32>>
}
impl<'a> Application<'a> {
    pub fn new(comm: &'a mut Comm) -> Self
    {
        Application {
            comm,
            shared_data: Arc::new(RwLock::new(0))
        }
    }
    pub fn setup(&mut self)
    {
        let shared_ref = self.shared_data.clone();
        self.comm.subscribe("hello/foo".to_string(), Box::new(move |payload| {
            let shared = shared_ref.read().unwrap();
            foo(payload, *shared);
        }));

        let shared_ref = self.shared_data.clone();
        self.comm.subscribe("hello/bar".to_string(), Box::new(move |payload| {
            let mut shared = shared_ref.write().unwrap();
            *shared += 1;
            bar(payload);
        }));
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
}
