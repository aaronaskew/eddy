use eddy::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    msg_id: usize,
}

impl Node<(), EchoPayload> for EchoNode {
    fn from_init(
        _state: (),
        _init: Init,
        _tx: std::sync::mpsc::Sender<Event<EchoPayload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self { msg_id: 1 })
    }

    fn step(&mut self, input: Event<EchoPayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(message) = input else {
            panic!("got injected event when there is no event injection");
        };

        let mut reply = message.into_reply(Some(&mut self.msg_id));

        match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo")?;
                output.write_all(b"\n").context("add newline")?;
            }

            EchoPayload::EchoOk { .. } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _, _>(())
}
