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
    _node_id: String,
    _node_ids: Vec<String>,
    msg_id: usize,
    messages: BTreeSet<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl Node<Self, BroadcastPayload> for BroadcastNode {
    fn from_init(state: Self, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            _node_id: init.node_id,
            _node_ids: init.node_ids,

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
        let mut reply = input.into_reply(Some(&mut self.msg_id));
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

            BroadcastPayload::Topology { topology } => {
                self.topology = topology;

                reply.body.payload = BroadcastPayload::TopologyOk;
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to topology")?;
                output.write_all(b"\n").context("add newline")?;
            }
            BroadcastPayload::BroadcastOk
            | BroadcastPayload::ReadOk { .. }
            | BroadcastPayload::TopologyOk => {}
        }

        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let initial_state = BroadcastNode {
        _node_id: String::new(),
        _node_ids: vec![],
        msg_id: 1,
        messages: BTreeSet::new(),
        topology: HashMap::new(),
    };

    main_loop::<BroadcastNode, BroadcastNode, _>(initial_state)
}
