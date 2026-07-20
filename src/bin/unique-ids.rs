use eddy::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UniquePayload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    node_id: String,
    msg_id: usize,
}

impl Node<(), UniquePayload> for UniqueNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<UniquePayload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: init.node_id,
            msg_id: 1,
        })
    }

    fn step(&mut self, input: Event<UniquePayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(message) = input else {
            panic!("got injected event when there is no event injection");
        };

        let mut reply = message.into_reply(&self.msg_id);

        match reply.body.payload {
            UniquePayload::Generate => {
                let guid = format!("{}-{}", self.node_id, self.msg_id);

                reply.body.payload = UniquePayload::GenerateOk { guid };

                reply
                    .send(output, &mut self.msg_id)
                    .context("serialize response to echo")?;
            }

            UniquePayload::GenerateOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _, _>(())
}
