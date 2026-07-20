use eddy::*;

use anyhow::Context;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    io::StdoutLock,
    time::Duration,
};

const GOSSIP_SLEEP_MS: u64 = 50;
const NUM_GOSSIP_RECEIVERS: usize = 2;

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
        unseen_by_receiver: BTreeSet<usize>,
    },
    GossipOk {
        received: BTreeSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    node_ids: Vec<String>,
    msg_id: usize,
    messages: BTreeSet<usize>,
    known: HashMap<String, BTreeSet<usize>>,
    topology: HashMap<String, Vec<String>>,
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
            node_ids: init.node_ids.clone(),
            msg_id: 1,
            messages: BTreeSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|node_id| (node_id, BTreeSet::new()))
                .collect(),
            topology: HashMap::new(),
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
                    let rng = &mut rand::rng();

                    for n in self.neighborhood.sample(rng, NUM_GOSSIP_RECEIVERS) {
                        let known_by_receiver = &self.known[n];

                        let unseen_by_receiver: BTreeSet<_> = self
                            .messages
                            .iter()
                            .filter(|m| !known_by_receiver.contains(m))
                            .copied()
                            .collect();

                        if !unseen_by_receiver.is_empty() {
                            eprintln!(
                                "notify of {}/{}",
                                unseen_by_receiver.len(),
                                self.messages.len()
                            );

                            Message {
                                src: self.node_id.clone(),
                                dst: n.clone(),
                                body: Body {
                                    msg_id: Some(self.msg_id),
                                    in_reply_to: None,
                                    payload: BroadcastPayload::Gossip { unseen_by_receiver },
                                },
                            }
                            .send(output, &mut self.msg_id)
                            .with_context(|| format!("gossip to {}", n))?;

                            eprintln!("sent gossip to {}", n);
                        }
                    }
                }
            },
            Event::Message(message) => {
                let mut reply = message.into_reply(&self.msg_id);
                match reply.body.payload {
                    BroadcastPayload::Gossip { unseen_by_receiver } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(unseen_by_receiver.iter().copied());
                        self.messages.extend(unseen_by_receiver.iter().copied());

                        reply.body.payload = BroadcastPayload::GossipOk {
                            received: unseen_by_receiver,
                        };
                        reply
                            .send(output, &mut self.msg_id)
                            .context("reply to gossip")?;
                    }
                    BroadcastPayload::GossipOk { received } => {
                        self.known
                            .get_mut(&reply.dst)
                            .expect("got gossip_ok from unknown node")
                            .extend(received.iter().copied());
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

                    BroadcastPayload::Topology { topology } => {
                        self.topology = topology;

                        // Neighborhood is all other nodes
                        self.neighborhood = self
                            .node_ids
                            .iter()
                            .filter(|n| n != &&self.node_id)
                            .cloned()
                            .collect();

                        eprintln!(
                            "neighborhood: len: {} nodes: {:?}",
                            self.neighborhood.len(),
                            self.neighborhood
                        );

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
