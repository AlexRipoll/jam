#[derive(Debug)]
pub struct Message {
    message_id: MessageId,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum MessageId {
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
    pub fn new(message_id: MessageId, payload: Vec<u8>) -> Message {
        Message {
            message_id,
            payload,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        // Preallocate the buffer with the exact capacity needed
        let mut bytes = Vec::with_capacity(4 + 1 + self.payload.len());

        // a Message has the following format <length prefix><message ID><payload>.
        //
        // Write the length of the message (payload size + 1 for message ID)
        bytes.extend_from_slice(&(self.payload.len() as u32 + 1).to_be_bytes());
        // Write the message ID as a single byte
        bytes.push(self.message_id as u8);
        // Write the payload
        bytes.extend_from_slice(&self.payload);

        bytes
    }

    pub fn deserialize(buffer: Vec<u8>) -> Message {
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&buffer[..4]);
        let length_prefix = u32::from_be_bytes(length_bytes);

        let message_id = MessageId::from(buffer[5]);
        let payload = buffer[6..length_prefix as usize + 6].to_vec();

        Message {
            message_id,
            payload,
        }
    }
}

struct Bitfield {
    bytes: Vec<u8>,
}

impl Bitfield {
    pub fn new(bitfield: Vec<u8>) -> Self {
        Self { bytes: bitfield }
    }

    pub fn has_piece(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index >= self.bytes.len() {
            return false;
        }

        (&self.bytes[byte_index] & (1 << (7 - bit_index))) != 0
    }

    pub fn set_piece(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index < self.bytes.len() {
            self.bytes[byte_index] |= 1 << (7 - bit_index);
        }
    }
}

// struct sued for Request and Cancel message payloads
struct Transfer {
    index: u32,
    begin: u32,
    length: u32,
}

impl Transfer {
    fn new(index: u32, begin: u32, block: u32) -> Self {
        Self {
            index,
            begin,
            length: block,
        }
    }

    fn serialize_payload(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + 4 + 4);

        // the Request and Cancel payload has the following format <index><begin><length>
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&self.begin.to_be_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());

        bytes
    }
}

#[cfg(test)]
mod test {
    use super::Bitfield;

    #[test]
    fn test_has_piece() {
        let bitfield = Bitfield::new(vec![0b0, 0b0, 0b00001000, 0b0]);
        //check that it has the only piece available at index 20
        assert!(bitfield.has_piece(20));
    }

    #[test]
    fn test_has_piece_out_of_range() {
        let bitfield = Bitfield::new(vec![0b0, 0b0, 0b00001000, 0b0]);
        // should return false when checking for a piece index out of range
        assert!(!bitfield.has_piece(50));
    }

    #[test]
    fn test_set_piece() {
        let mut bitfield = Bitfield::new(vec![0b0, 0b0, 0b0, 0b0]);
        bitfield.set_piece(20);

        assert_eq!(bitfield.bytes, vec![0b0, 0b0, 0b00001000, 0b0]);
    }

    #[test]
    fn test_set_piece_out_of_range() {
        let mut bitfield = Bitfield::new(vec![0b0, 0b0, 0b0, 0b0]);
        // out of range
        bitfield.set_piece(50);

        // no change in bitfield
        assert_eq!(bitfield.bytes, vec![0b0, 0b0, 0b0, 0b0]);
    }
}
