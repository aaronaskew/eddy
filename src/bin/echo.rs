use eddy::*;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Init(eddy::Init),
    InitOk,
    Echo { echo: String },
    EchoOk { echo: String },
}

impl Payload for EchoPayload {
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

struct EchoNode {
    msg_id: usize,
}

impl Node<Self, EchoPayload> for EchoNode {
    fn from_init(state: Self, _init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            msg_id: state.msg_id,
        })
    }

    fn step(&mut self, input: Message<EchoPayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            EchoPayload::Init { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: EchoPayload::InitOk,
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo")?;
                output.write_all(b"\n").context("add newline")?;
            }
            EchoPayload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo")?;
                output.write_all(b"\n").context("add newline")?;
            }
            EchoPayload::InitOk => bail!("received init_ok message"),
            EchoPayload::EchoOk { .. } => {}
        }

        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode, EchoNode, _>(EchoNode { msg_id: 1 })
}
