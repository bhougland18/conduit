wit_bindgen::generate!({
    path: "../../wit",
    world: "conduit-node",
});

use exports::conduit::batch::batch::{BatchError, Guest, Payload, PortBatch};

struct UppercaseGuest;

impl Guest for UppercaseGuest {
    fn invoke(inputs: Vec<PortBatch>) -> Result<Vec<PortBatch>, BatchError> {
        let mut packets = Vec::new();

        for input in inputs {
            for mut packet in input.packets {
                let Payload::Bytes(bytes) = packet.payload else {
                    return Err(BatchError::UnsupportedPayload(
                        "uppercase guest accepts only bytes payloads".to_owned(),
                    ));
                };
                packet.payload = Payload::Bytes(uppercase_ascii(bytes));
                packets.push(packet);
            }
        }

        Ok(vec![PortBatch {
            port_id: "out".to_owned(),
            packets,
        }])
    }
}

fn uppercase_ascii(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in &mut bytes {
        *byte = byte.to_ascii_uppercase();
    }
    bytes
}

export!(UppercaseGuest);
