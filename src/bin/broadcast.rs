use eddy::*;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
};

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
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Init(eddy::Init),
    InitOk,
}

impl Payload for BroadcastPayload {
    fn extract_init(input: Self) -> Option<Init> {
        if let Self::Init(init) = input {
            return Some(init);
        }

        None
    }

    fn gen_init_ok() -> Self {
        Self::InitOk
    }
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    node_ids: Vec<String>,
    msg_id: usize,
    messages: HashSet<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl Node<Self, BroadcastPayload> for BroadcastNode {
    fn from_init(state: Self, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: init.node_id,
            node_ids: init.node_ids,

            msg_id: state.msg_id,
            messages: state.messages,
            topology: state.topology,
        })
    }

    fn step(
        &mut self,
        input: Message<BroadcastPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            BroadcastPayload::Init(init) => {
                self.node_id = init.node_id;

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::InitOk,
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to generate")?;
                output.write_all(b"\n").context("add newline")?;
            }

            BroadcastPayload::InitOk => bail!("received init_ok message"),
            BroadcastPayload::Broadcast { message } => {
                let is_new_message = self.messages.insert(message);

                // Only broadcast message out if it is the first time we've seen it.
                if is_new_message {
                    for dest_node_id in self
                        .topology
                        .get(&self.node_id)
                        .expect("topology should contain an entry for this node")
                        .iter()
                        .filter(|id| self.node_ids.contains(id) && **id != input.src)
                    {
                        let broadcast_msg = Message {
                            src: self.node_id.clone(),
                            dst: dest_node_id.clone(),
                            body: Body {
                                msg_id: Some(self.msg_id),
                                in_reply_to: None,
                                payload: BroadcastPayload::Broadcast { message },
                            },
                        };

                        serde_json::to_writer(&mut *output, &broadcast_msg)
                            .context("serialize message to broadcast")?;
                        output.write_all(b"\n").context("add newline")?;

                        self.msg_id += 1;
                    }
                }

                let broadcast_ok_msg = Message {
                    src: self.node_id.clone(),
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::BroadcastOk,
                    },
                };

                serde_json::to_writer(&mut *output, &broadcast_ok_msg)
                    .context("serialize message to broadcast")?;
                output.write_all(b"\n").context("add newline")?;
            }
            BroadcastPayload::BroadcastOk => {}
            BroadcastPayload::Read => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::ReadOk {
                            messages: self.messages.clone(),
                        },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to read")?;
                output.write_all(b"\n").context("add newline")?;
            }
            BroadcastPayload::ReadOk { .. } => {}
            BroadcastPayload::Topology { topology } => {
                self.topology = topology;

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::TopologyOk,
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to topology")?;
                output.write_all(b"\n").context("add newline")?;
            }
            BroadcastPayload::TopologyOk => bail!("received topology_ok message"),
        }

        // dbg!(&self);

        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let initial_state = BroadcastNode {
        node_id: String::new(),
        node_ids: vec![],
        msg_id: 1,
        messages: HashSet::new(),
        topology: HashMap::new(),
    };

    main_loop::<BroadcastNode, BroadcastNode, _>(initial_state)
}
