use eddy::*;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UniquePayload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
    Init(eddy::Init),
    InitOk,
}

impl Payload for UniquePayload {
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

struct UniqueNode {
    node_id: String,
    msg_id: usize,
}

impl Node<Self, UniquePayload> for UniqueNode {
    fn from_init(state: Self, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node_id: init.node_id,
            msg_id: state.msg_id,
        })
    }

    fn step(
        &mut self,
        input: Message<UniquePayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            UniquePayload::Init(init) => {
                self.node_id = init.node_id;

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: UniquePayload::InitOk,
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to generate")?;
                output.write_all(b"\n").context("add newline")?;
            }
            UniquePayload::Generate => {
                let guid = format!("{}-{}", self.node_id, self.msg_id);

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: UniquePayload::GenerateOk { guid },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo")?;
                output.write_all(b"\n").context("add newline")?;
            }
            UniquePayload::InitOk => bail!("received init_ok message"),
            UniquePayload::GenerateOk { .. } => {}
        }

        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<UniqueNode, UniqueNode, _>(UniqueNode {
        msg_id: 1,
        node_id: String::new(),
    })
}
