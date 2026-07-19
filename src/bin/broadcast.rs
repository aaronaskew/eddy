use eddy::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
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
        messages: BTreeSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    msg_id: usize,
    messages: BTreeSet<usize>,
    _known: HashMap<String, BTreeSet<usize>>,
    _msg_communicated: HashMap<usize, BTreeSet<usize>>,
    neighborhood: Vec<String>,
}

impl Node<(), BroadcastPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<BroadcastPayload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: init.node_id,
            msg_id: 1,
            messages: BTreeSet::new(),
            _known: init
                .node_ids
                .into_iter()
                .map(|node_id| (node_id, BTreeSet::new()))
                .collect(),
            _msg_communicated: HashMap::new(),
            neighborhood: vec![],
        })
    }

    fn step(
        &mut self,
        input: Event<BroadcastPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(message) => {
                let mut reply = message.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    BroadcastPayload::Broadcast { message } => {
                        self.messages.insert(message);

                        reply.body.payload = BroadcastPayload::BroadcastOk;
                        serde_json::to_writer(&mut *output, &reply)
                            .context("serialize message to broadcast")?;
                        output.write_all(b"\n").context("add newline")?;
                    }

                    BroadcastPayload::Read => {
                        reply.body.payload = BroadcastPayload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        serde_json::to_writer(&mut *output, &reply)
                            .context("serialize response to read")?;
                        output.write_all(b"\n").context("add newline")?;
                    }

                    BroadcastPayload::Topology { mut topology } => {
                        self.neighborhood = topology.remove(&self.node_id).unwrap_or_else(|| {
                            panic!("no toplogy given for node {}", self.node_id)
                        });

                        reply.body.payload = BroadcastPayload::TopologyOk;
                        serde_json::to_writer(&mut *output, &reply)
                            .context("serialize response to topology")?;
                        output.write_all(b"\n").context("add newline")?;
                    }
                    BroadcastPayload::BroadcastOk
                    | BroadcastPayload::ReadOk { .. }
                    | BroadcastPayload::TopologyOk => {}
                }
            }
            Event::Injected(_) => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
