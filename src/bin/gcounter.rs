use eddy::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::StdoutLock,
    sync::{Arc, Mutex},
    time::Duration,
};

const KV: &str = "seq-kv";
const KEY: &str = "counter";
const READ_SLEEP_MS: u64 = 50;
const CAS_SLEEP_MS: u64 = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GCounterPayload {
    /// An RPC from a client to add `delta` to the `seq-kv` store
    Add {
        delta: usize,
    },
    /// An RPC response from this node stating the add was successful
    AddOk,
    /// Either:
    /// * An RPC from a client to return the current counter value
    /// * An RPC to the `seq-kv` store to return the current sum of deltas
    Read {
        key: Option<String>,
    },
    /// Either:
    /// * An RPC response from this node to a client returning the current counter value
    /// * An RPC response from the `seq-kv` store returning the current sum of deltas
    ReadOk {
        value: usize,
    },
    /// * An RPC to the `seq-kv` store write the current delta
    Write {
        key: String,
        value: usize,
    },
    /// * An RPC response from the `seq-kv` store stating the write RPC was successful
    WriteOk,
    /// * An RPC to the `seq-kv` store compare-and-set the current value from `from` to `to`
    #[serde(rename = "cas")]
    CompareAndSet {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: Option<bool>,
    },
    /// * An RPC response from the `seq-kv` store stating the compare-and-set RPC was successful
    #[serde(rename = "cas_ok")]
    CompareAndSetOk,
    Error {
        code: usize,
        text: String,
    },
}

enum InjectedPayload {
    ReadKVCounter,
    Cas { delta: usize },
}

#[derive(Debug)]
struct AddInstruction {
    delta: usize,
    rcvd_add_msg_id: usize,
    sent_cas_msg_id: Option<usize>,
}

#[derive(Debug)]
struct GCounterNode {
    node_id: String,
    _node_ids: Vec<String>,
    msg_id: usize,
    counter: usize,
    add_queue: Arc<Mutex<VecDeque<AddInstruction>>>,
}

impl Node<(), GCounterPayload, InjectedPayload> for GCounterNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<GCounterPayload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        let add_queue = Arc::new(Mutex::new(VecDeque::new()));

        let add_queue_thread = Arc::clone(&add_queue);

        let add_tx = tx.clone();

        // Thread to read the add_queue for add requests from the servers.
        // * First send read injection the current value from seq-kv and give time for the result to be returned
        // * Second send the CAS injection to swap the values on the server
        std::thread::spawn(move || {
            // generate read events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(READ_SLEEP_MS));

                let add_queue_lock = add_queue_thread
                    .lock()
                    .expect("add_queue thread should be able to get lock on mutex");

                if let Some(AddInstruction {
                    delta,
                    rcvd_add_msg_id: _add_msg_id,
                    sent_cas_msg_id: None,
                }) = add_queue_lock.iter().next()
                {
                    eprintln!("current add_queue: {:?}", add_queue_lock);

                    // first read
                    if add_tx
                        .send(Event::Injected(InjectedPayload::ReadKVCounter))
                        .is_err()
                    {
                        break;
                    }

                    std::thread::sleep(Duration::from_millis(CAS_SLEEP_MS));

                    // then cas

                    if add_tx
                        .send(Event::Injected(InjectedPayload::Cas { delta: *delta }))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });

        Ok(Self {
            node_id: init.node_id,
            _node_ids: init.node_ids.clone(),
            msg_id: 1,
            counter: 0,
            add_queue: Arc::clone(&add_queue),
        })
    }

    fn step(
        &mut self,
        input: Event<GCounterPayload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::ReadKVCounter => {
                    Message {
                        src: self.node_id.clone(),
                        dst: KV.to_string(),
                        body: Body {
                            msg_id: Some(self.msg_id),
                            in_reply_to: None,
                            payload: GCounterPayload::Read {
                                key: Some(KEY.to_string()),
                            },
                        },
                    }
                    .send(output, &mut self.msg_id)
                    .context("read from seq-kv")?;
                }
                InjectedPayload::Cas { delta } => {
                    let from = self.counter;
                    let to = self.counter + delta;

                    let msg_id = self.msg_id;

                    match self
                        .add_queue
                        .lock()
                        .expect("should receive mutex lock")
                        .iter_mut()
                        .next()
                    {
                        Some(&mut AddInstruction {
                            delta: _,
                            rcvd_add_msg_id: _,
                            ref mut sent_cas_msg_id,
                        }) => {
                            *sent_cas_msg_id = Some(msg_id);
                        }
                        _ => {
                            panic!("could not get mutex lock");
                        }
                    };

                    Message {
                        src: self.node_id.clone(),
                        dst: KV.to_string(),
                        body: Body {
                            msg_id: Some(msg_id),
                            in_reply_to: None,
                            payload: GCounterPayload::CompareAndSet {
                                key: KEY.to_string(),
                                from,
                                to,
                                create_if_not_exists: Some(true),
                            },
                        },
                    }
                    .send(output, &mut self.msg_id)
                    .context("cas seq-kv key: {} from: {} to: {}")?;
                }
            },
            Event::Message(message) => {
                let original_in_reply_to = message.body.in_reply_to;
                let mut reply = message.into_reply(&self.msg_id);
                match reply.body.payload {
                    GCounterPayload::Add { delta } => {
                        // self.add_queue
                        //     .lock()
                        //     .expect("add_queue thread should be able to get lock on mutex")
                        //     .push_back(AddInstruction {
                        //         delta,
                        //         rcvd_add_msg_id: reply
                        //             .body
                        //             .in_reply_to
                        //             .expect("an in_reply_to should be set by into_reply()"),
                        //         sent_cas_msg_id: None,
                        //     });

                        self.counter += delta;

                        reply.body.payload = GCounterPayload::Write {
                            key: KEY.to_string(),
                            value: self.counter,
                        };
                        reply
                            .send(output, &mut self.msg_id)
                            .context("sending `write`")?;
                    }
                    GCounterPayload::Read { .. } => {
                        reply.body.payload = GCounterPayload::ReadOk {
                            value: self.counter,
                        };
                        reply
                            .send(output, &mut self.msg_id)
                            .context("sending `read_ok`")?;
                    }
                    GCounterPayload::CompareAndSetOk => {
                        let mut add_queue = self
                            .add_queue
                            .lock()
                            .expect("add_queue thread should be able to get lock on mutex");

                        if let Some(AddInstruction {
                            delta: _,
                            rcvd_add_msg_id,
                            sent_cas_msg_id: Some(sent_cas_msg_id),
                        }) = add_queue.front()
                            && &original_in_reply_to.expect("original_in_reply_to should be set")
                                == sent_cas_msg_id
                        {
                            let in_reply_to = *rcvd_add_msg_id;

                            add_queue.pop_front();

                            reply.body.in_reply_to = Some(in_reply_to);
                            reply.body.payload = GCounterPayload::AddOk;
                            reply
                                .send(output, &mut self.msg_id)
                                .context("sending `add_ok`")?;
                        }
                    }
                    GCounterPayload::ReadOk { value } => {
                        self.counter = value;
                    }
                    GCounterPayload::AddOk => {
                        panic!("should not receive `add_ok`");
                    }
                    GCounterPayload::Write { .. } => {
                        panic!("should not receive `write`");
                    }
                    GCounterPayload::CompareAndSet { .. } => {
                        panic!("should not receive `cas`");
                    }
                    GCounterPayload::WriteOk => {}
                    GCounterPayload::Error { code, text } => {
                        eprintln!(
                            "Received Error: src={} in_reply_to={:?} code={} text={}",
                            reply.dst, original_in_reply_to, code, text
                        );

                        match code {
                            // `read`: key does not exist
                            20 => {
                                // write the kv store
                                reply.dst = KV.into();
                                reply.body.in_reply_to = None;
                                reply.body.payload = GCounterPayload::Write {
                                    key: KEY.into(),
                                    value: self.counter,
                                };
                                reply
                                    .send(output, &mut self.msg_id)
                                    .context("sending `cas` to `seq-kv`")?;
                            }
                            // `cas`: current value is not `from`
                            22 => {
                                // do nothing. the add thread will resend
                            }
                            _ => {
                                panic!("unknown error")
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, GCounterNode, _, _>(())
}
