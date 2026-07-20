use eddy::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    io::StdoutLock,
    time::Duration,
};

const GOSSIP_SLEEP_MS: u64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: BTreeSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: BTreeSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    _node_ids: Vec<String>,
    msg_id: usize,
    messages: BTreeSet<usize>,
    known: HashMap<String, BTreeSet<usize>>,
    neighborhood: Vec<String>,
}

impl Node<(), BroadcastPayload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<BroadcastPayload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        let gossip_tx = tx.clone();
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(GOSSIP_SLEEP_MS));
                if gossip_tx
                    .send(Event::Injected(InjectedPayload::Gossip))
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Self {
            node_id: init.node_id,
            _node_ids: init.node_ids.clone(),
            msg_id: 1,
            messages: BTreeSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|node_id| (node_id, BTreeSet::new()))
                .collect(),
            neighborhood: vec![],
        })
    }

    fn step(
        &mut self,
        input: Event<BroadcastPayload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known_by_n = &self.known[n];

                        let notify_of: BTreeSet<_> = self
                            .messages
                            .iter()
                            .filter(|m| !known_by_n.contains(m))
                            .copied()
                            .collect();

                        if !notify_of.is_empty() {
                            eprintln!("notify of {}/{}", notify_of.len(), self.messages.len());

                            Message {
                                src: self.node_id.clone(),
                                dst: n.clone(),
                                body: Body {
                                    msg_id: Some(self.msg_id),
                                    in_reply_to: None,
                                    payload: BroadcastPayload::Gossip { seen: notify_of },
                                },
                            }
                            .send(output, &mut self.msg_id)
                            .with_context(|| format!("gossip to {}", n))?;
                            self.msg_id += 1;
                        }
                    }
                }
            },
            Event::Message(message) => {
                let mut reply = message.into_reply(&self.msg_id);
                match reply.body.payload {
                    BroadcastPayload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.messages.extend(seen);
                    }
                    BroadcastPayload::Broadcast { message } => {
                        self.messages.insert(message);

                        reply.body.payload = BroadcastPayload::BroadcastOk;
                        reply
                            .send(output, &mut self.msg_id)
                            .context("reply to broadcast")?;
                    }

                    BroadcastPayload::Read => {
                        reply.body.payload = BroadcastPayload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply
                            .send(output, &mut self.msg_id)
                            .context("reply to read")?;
                    }

                    BroadcastPayload::Topology { mut topology } => {
                        self.neighborhood = topology.remove(&self.node_id).unwrap_or_else(|| {
                            panic!("no toplogy given for node {}", self.node_id)
                        });

                        reply.body.payload = BroadcastPayload::TopologyOk;
                        reply
                            .send(output, &mut self.msg_id)
                            .context("reply to topology")?;
                    }
                    BroadcastPayload::BroadcastOk
                    | BroadcastPayload::ReadOk { .. }
                    | BroadcastPayload::TopologyOk => {}
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
