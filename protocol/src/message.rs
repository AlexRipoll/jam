use std::io::{self, Error, ErrorKind};

#[derive(Debug, PartialEq, Eq)]
pub struct Message {
    pub message_id: MessageId,
    pub payload: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MessageId {
    KeepAlive = -1,
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    Bitfield = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
    Port = 9,
}

impl From<u8> for MessageId {
    fn from(value: u8) -> Self {
        match value {
            0 => MessageId::Choke,
            1 => MessageId::Unchoke,
            2 => MessageId::Interested,
            3 => MessageId::NotInterested,
            4 => MessageId::Have,
            5 => MessageId::Bitfield,
            6 => MessageId::Request,
            7 => MessageId::Piece,
            8 => MessageId::Cancel,
            9 => MessageId::Port,
            _ => panic!("Unknown MessageId"),
        }
    }
}

impl Message {
    pub fn new(message_id: MessageId, payload: Option<Vec<u8>>) -> Message {
        Message {
            message_id,
            payload,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        // Determine payload length (0 if None)
        let payload_length = self.payload.as_ref().map_or(0, |p| p.len());

        // Preallocate the buffer with the capacity needed
        let mut bytes = Vec::with_capacity(4 + 1 + payload_length);

        // a Message has the following format <length prefix><message ID><payload>.
        //
        // Write the length of the message (payload size + 1 for message ID)
        bytes.extend_from_slice(&(payload_length as u32 + 1).to_be_bytes());
        // Write the message ID as a single byte
        bytes.push(self.message_id as u8);
        // Write the payload, if it exists
        if let Some(payload) = &self.payload {
            bytes.extend_from_slice(payload);
        }

        bytes
    }

    pub fn deserialize(buffer: &[u8]) -> io::Result<Message> {
        if buffer.len() < 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Buffer too short for length prefix",
            ));
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&buffer[..4]);
        let length_prefix = u32::from_be_bytes(length_bytes);

        if length_prefix == 0 {
            return Ok(Message::new(MessageId::KeepAlive, None));
        }

        let message_id = MessageId::from(buffer[4]);

        let payload = if length_prefix > 1 {
            Some(buffer[5..length_prefix as usize + 5 - 1].to_vec())
        } else {
            None
        };

        Ok(Message::new(message_id, payload))
    }
}

// struct sued for Request and Cancel message payloads
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransferPayload {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl TransferPayload {
    pub fn new(index: u32, begin: u32, length: u32) -> Self {
        Self {
            index,
            begin,
            length,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + 4 + 4);

        // the Request and Cancel payload has the following format <index><begin><length>
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&self.begin.to_be_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());

        bytes
    }
}

pub struct PiecePayload {
    pub index: u32,
    pub begin: u32,
    pub block: Vec<u8>,
}

impl PiecePayload {
    pub fn new(index: u32, begin: u32, block: Vec<u8>) -> Self {
        Self {
            index,
            begin,
            block,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + 4 + 4);

        // the Request and Cancel payload has the following format <index><begin><length>
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&self.begin.to_be_bytes());
        bytes.extend_from_slice(&self.block);

        bytes
    }

    pub fn deserialize(payload: &[u8]) -> io::Result<Self> {
        // Check that the payload is at least 8 bytes for `index` and `begin`
        if payload.len() < 8 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Payload too short for Piece deserialization",
            ));
        }

        // Extract the `index` (first 4 bytes) and `begin` (next 4 bytes)
        let mut index = [0u8; 4];
        index.copy_from_slice(&payload[..4]);
        let index = u32::from_be_bytes(index);
        let mut begin = [0u8; 4];
        begin.copy_from_slice(&payload[4..8]);
        let begin = u32::from_be_bytes(begin);

        // The remaining bytes correspond to the `block`
        let block = payload[8..].to_vec();

        Ok(PiecePayload::new(index, begin, block))
    }
}

#[cfg(test)]
mod test {
    use super::Message;

    #[test]
    fn test_bitfield_message_deserialize() {
        let bytes = vec![
            0, 0, 0, 10, 5, 255, 255, 255, 255, 255, 255, 255, 255, 240, 0, 0, 0, 0, 0,
        ];
        let expected = Message {
            message_id: super::MessageId::Bitfield,
            payload: Some(vec![255, 255, 255, 255, 255, 255, 255, 255, 240]),
        };
        assert_eq!(expected, Message::deserialize(&bytes).unwrap());
    }
}
