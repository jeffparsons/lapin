extern crate env_logger;
extern crate futures;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate log;
extern crate tokio_core;

use futures::Stream;
use futures::future::Future;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;

use lapin::types::FieldTable;
use lapin::client::ConnectionOptions;
use lapin::channel::{BasicConsumeOptions, BasicProperties, BasicPublishOptions, BasicQosOptions,
                     QueueDeclareOptions, QueueDeleteOptions, QueuePurgeOptions};

#[test]
fn connection() {
    env_logger::init();
    let mut core = Core::new().unwrap();

    let handle = core.handle();
    let addr = "127.0.0.1:5672".parse().unwrap();

    core.run(
        TcpStream::connect(&addr, &handle)
            .and_then(|stream| {
                lapin::client::Client::connect(stream, &ConnectionOptions::default())
            })
            .and_then(|(client, _)| {
                client
                    .create_channel()
                    .and_then(|channel| {
                        let id = channel.id;
                        info!("created channel with id: {}", id);

                        channel
                            .queue_declare(
                                "hello",
                                &QueueDeclareOptions::default(),
                                &FieldTable::new(),
                            )
                            .and_then(move |_| {
                                info!("channel {} declared queue {}", id, "hello");

                                channel
                                    .queue_purge("hello", &QueuePurgeOptions::default())
                                    .and_then(move |_| {
                                        channel.basic_publish(
                                            "",
                                            "hello",
                                            b"hello from tokio",
                                            &BasicPublishOptions::default(),
                                            BasicProperties::default(),
                                        )
                                    })
                            })
                    })
                    .and_then(move |_| client.create_channel())
                    .and_then(|channel| {
                        let id = channel.id;
                        info!("created channel with id: {}", id);

                        let ch1 = channel.clone();
                        let ch2 = channel.clone();
                        channel
                            .basic_qos(&BasicQosOptions {
                                prefetch_count: 16,
                                ..Default::default()
                            })
                            .and_then(move |_| {
                                info!("channel QoS specified");
                                channel
                                    .queue_declare(
                                        "hello",
                                        &QueueDeclareOptions::default(),
                                        &FieldTable::new(),
                                    )
                                    .map(move |()| channel)
                            })
                            .and_then(move |channel| {
                                info!("channel {} declared queue {}", id, "hello");

                                channel.basic_consume(
                                    "hello",
                                    "my_consumer",
                                    &BasicConsumeOptions::default(),
                                    &FieldTable::new(),
                                )
                            })
                            .and_then(move |stream| {
                                info!("got consumer stream");

                                stream
                                    .into_future()
                                    .map_err(|(err, _)| err)
                                    .and_then(move |(message, _)| {
                                        let msg = message.unwrap();
                                        info!("got message: {:?}", msg);
                                        assert_eq!(msg.data, b"hello from tokio");
                                        ch1.basic_ack(msg.delivery_tag)
                                    })
                                    .and_then(move |_| {
                                        ch2.queue_delete("hello", &QueueDeleteOptions::default())
                                    })
                            })
                    })
            }),
    ).unwrap();
}

#[test]
// TODO: There's probably a better name for this when we figure out exactly
// what the underlying problem is.
fn consumer_not_ready_polls_again() {
    use tokio_core::reactor::Timeout;
    use std::time::Duration;

    env_logger::init();
    let mut core = Core::new().unwrap();

    let handle = core.handle();
    let handle_for_timeout = handle.clone();
    let addr = "127.0.0.1:5672".parse().unwrap();

    core.run(
        TcpStream::connect(&addr, &handle)
            .and_then(|stream| {
                lapin::client::Client::connect(stream, &ConnectionOptions::default())
            })
            .and_then(|(client, _heartbeat_future_fn)| {
                client.create_channel().and_then(|consume_channel| {
                    let id = consume_channel.id;
                    println!("created channel with id: {}", id);
                    let ack_channel = consume_channel.clone();

                    consume_channel
                        .queue_declare(
                            "hello_not_ready_polls_again",
                            &QueueDeclareOptions::default(),
                            &FieldTable::new(),
                        )
                        .and_then(move |_| {
                            // Purge the queue so we don't end up with messages from previous test runs!
                            consume_channel
                                .queue_purge(
                                    "hello_not_ready_polls_again",
                                    &QueuePurgeOptions::default(),
                                )
                                .map(move |_| {
                                    println!(
                                        "channel {} declared queue {}",
                                        id, "hello_not_ready_polls_again"
                                    );

                                    handle.spawn(
                                        consume_channel
                                            .basic_consume(
                                                "hello_not_ready_polls_again",
                                                "my_consumer",
                                                &BasicConsumeOptions::default(),
                                                &FieldTable::new(),
                                            )
                                            .and_then(|stream| {
                                                println!("got consumer stream");

                                                // We've purged the queue, and we're only sending one message.
                                                stream.take(1).for_each(move |message| {
                                                    println!("got message: {:?}", message);
                                                    println!(
                                                        "decoded message: {:?}",
                                                        std::str::from_utf8(&message.data).unwrap()
                                                    );
                                                    ack_channel.basic_ack(message.delivery_tag)
                                                })
                                            })
                                            .map_err(|err| {
                                                println!("uh oh: {:?}", err);
                                                ()
                                            }),
                                    );
                                    ()
                                })
                        })
                        .and_then(move |_| {
                            client.create_channel().and_then(move |publish_channel| {
                                let id = publish_channel.id;
                                println!("created channel with id: {}", id);

                                // Wait a little while before sending the message, to make sure that we don't beat the consumer and make its job too easy.
                                // We want the consumer to have a chance to be "not ready" so we can assert that it actually polls again when the
                                // message eventually gets delivered.
                                Timeout::new(Duration::from_secs(1), &handle_for_timeout)
                                    .unwrap()
                                    .and_then(move |_| {
                                        publish_channel
                                            .basic_publish(
                                                "",
                                                "hello_not_ready_polls_again",
                                                b"please receive me",
                                                &BasicPublishOptions::default(),
                                                BasicProperties::default()
                                                    .with_user_id("guest".to_string())
                                                    .with_reply_to("foobar".to_string()),
                                            )
                                            .map(|confirmation| {
                                                println!(
                                                    "publish got confirmation: {:?}",
                                                    confirmation
                                                )
                                            })
                                    })
                            })
                        })
                        .and_then(move |_| futures::future::empty::<(), std::io::Error>())
                })
            }),
    ).unwrap();
}
