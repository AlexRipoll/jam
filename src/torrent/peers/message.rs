use std::fmt::{self, Display};

/// Represents a BitTorrent peer wire protocol message.
///
/// Each message has a message ID and an optional payload.
/// The protocol specifies a set of standard message types defined in `MessageId`.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Message {
    /// The type of message
    pub message_id: MessageId,
    /// Optional payload data associated with the message
    pub payload: Option<Vec<u8>>,
}

/// Identifies the type of BitTorrent peer wire protocol message.
///
/// The BitTorrent protocol defines standard message types with specific IDs.
/// KeepAlive is a special case with no message ID in the wire format.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MessageId {
    /// Keep-alive message: <len=0000>
    KeepAlive = -1,
    /// Choke message: <len=0001><id=0>
    Choke = 0,
    /// Unchoke message: <len=0001><id=1>
    Unchoke = 1,
    /// Interested message: <len=0001><id=2>
    Interested = 2,
    /// Not Interested message: <len=0001><id=3>
    NotInterested = 3,
    /// Have message: <len=0005><id=4><piece index>
    Have = 4,
    /// Bitfield message: <len=0001+X><id=5><bitfield>
    Bitfield = 5,
    /// Request message: <len=0013><id=6><index><begin><length>
    Request = 6,
    /// Piece message: <len=0009+X><id=7><index><begin><block>
    Piece = 7,
    /// Cancel message: <len=0013><id=8><index><begin><length>
    Cancel = 8,
    /// Port message: <len=0003><id=9><listen-port>
    Port = 9,
}

impl TryFrom<u8> for MessageId {
    type Error = MessageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageId::Choke),
            1 => Ok(MessageId::Unchoke),
            2 => Ok(MessageId::Interested),
            3 => Ok(MessageId::NotInterested),
            4 => Ok(MessageId::Have),
            5 => Ok(MessageId::Bitfield),
            6 => Ok(MessageId::Request),
            7 => Ok(MessageId::Piece),
            8 => Ok(MessageId::Cancel),
            9 => Ok(MessageId::Port),
            _ => Err(MessageError::InvalidMessageId(value)),
        }
    }
}

impl Message {
    /// Creates a new BitTorrent protocol message.
    ///
    /// # Arguments
    /// * `message_id` - The type of message
    /// * `payload` - Optional payload data
    pub fn new(message_id: MessageId, payload: Option<Vec<u8>>) -> Self {
        Self {
            message_id,
            payload,
        }
    }

    /// Serializes the message into a byte vector according to the BitTorrent protocol.
    ///
    /// Format: <length prefix (4 bytes)><message ID (1 byte)><payload (variable)>
    /// The length prefix is the length of the message ID (1 byte) plus the length of the payload.
    /// For keep-alive messages, only the length prefix (0) is sent.
    ///
    /// # Returns
    /// A vector of bytes representing the serialized message
    pub fn serialize(&self) -> Vec<u8> {
        match self.message_id {
            MessageId::KeepAlive => {
                // Keep-alive is just 4 bytes of zeros
                vec![0, 0, 0, 0]
            }
            _ => {
                // Determine payload length (0 if None)
                let payload_length = self.payload.as_ref().map_or(0, |p| p.len());

                // Calculate the total message length (message ID byte + payload)
                let msg_length = 1 + payload_length;

                // Preallocate the buffer with the capacity needed
                let mut bytes = Vec::with_capacity(4 + msg_length);

                // Write the length prefix (4 bytes)
                bytes.extend_from_slice(&(msg_length as u32).to_be_bytes());

                // Write the message ID (1 byte)
                bytes.push(self.message_id as u8);

                // Write the payload, if it exists
                if let Some(payload) = &self.payload {
                    bytes.extend_from_slice(payload);
                }

                bytes
            }
        }
    }

    /// Deserializes a byte buffer into a Message and returns the consumed bytes.
    ///
    /// # Arguments
    /// * `buffer` - The buffer containing message data
    ///
    /// # Returns
    /// A Result containing the deserialized Message and the number of bytes consumed,
    /// or a MessageError if deserialization fails
    pub fn deserialize(buffer: &[u8]) -> Result<Message, MessageError> {
        if buffer.len() < 4 {
            return Err(MessageError::BufferTooShort);
        }

        // Extract the length prefix (first 4 bytes)
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&buffer[..4]);
        let length_prefix = u32::from_be_bytes(length_bytes) as usize;

        // Keep-alive message has length prefix 0
        if length_prefix == 0 {
            return Ok(Self::new(MessageId::KeepAlive, None));
        }

        // Check if buffer contains the full message
        let total_length = 4 + length_prefix;
        if buffer.len() < total_length {
            return Err(MessageError::BufferTooShort);
        }

        // Extract and validate message ID
        let message_id = MessageId::try_from(buffer[4])?;

        // Extract payload if present
        let payload = if length_prefix > 1 {
            Some(buffer[5..4 + length_prefix].to_vec())
        } else {
            None
        };

        Ok(Self::new(message_id, payload))
    }

    /// Creates a new Have message with the given piece index.
    ///
    /// # Arguments
    /// * `piece_index` - The index of the piece being announced
    ///
    /// # Returns
    /// A Message of type Have with the piece index as payload
    pub fn have(piece_index: u32) -> Self {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&piece_index.to_be_bytes());
        Self::new(MessageId::Have, Some(payload))
    }

    /// Creates a new Bitfield message with the given bitfield.
    ///
    /// # Arguments
    /// * `bitfield` - The bitfield representing pieces the peer has
    ///
    /// # Returns
    /// A Message of type Bitfield with the bitfield as payload
    pub fn bitfield(bitfield: Vec<u8>) -> Self {
        Self::new(MessageId::Bitfield, Some(bitfield))
    }

    /// Creates a new Request message.
    ///
    /// # Arguments
    /// * `transfer_payload` - The TransferPayload containing index, begin, and length
    ///
    /// # Returns
    /// A Message of type Request with the transfer payload serialized
    pub fn request(transfer_payload: &TransferPayload) -> Self {
        Self::new(MessageId::Request, Some(transfer_payload.serialize()))
    }

    /// Creates a new Cancel message.
    ///
    /// # Arguments
    /// * `transfer_payload` - The TransferPayload containing index, begin, and length
    ///
    /// # Returns
    /// A Message of type Cancel with the transfer payload serialized
    pub fn cancel(transfer_payload: &TransferPayload) -> Self {
        Self::new(MessageId::Cancel, Some(transfer_payload.serialize()))
    }

    /// Creates a new Piece message.
    ///
    /// # Arguments
    /// * `piece_payload` - The PiecePayload containing index, begin, and block data
    ///
    /// # Returns
    /// A Message of type Piece with the piece payload serialized
    pub fn piece(piece_payload: &PiecePayload) -> Self {
        Self::new(MessageId::Piece, Some(piece_payload.serialize()))
    }

    /// Extracts a Have message's piece index from the payload.
    ///
    /// # Returns
    /// A Result containing the piece index, or a MessageError if extraction fails
    pub fn extract_have_index(&self) -> Result<u32, MessageError> {
        if self.message_id != MessageId::Have {
            return Err(MessageError::DeserializationError);
        }

        let payload = self.payload.as_ref().ok_or(MessageError::PayloadTooShort)?;
        if payload.len() < 4 {
            return Err(MessageError::PayloadTooShort);
        }

        let mut index_bytes = [0u8; 4];
        index_bytes.copy_from_slice(&payload[..4]);
        Ok(u32::from_be_bytes(index_bytes))
    }

    /// Extracts a TransferPayload from a Request or Cancel message.
    ///
    /// # Returns
    /// A Result containing the TransferPayload, or a MessageError if extraction fails
    pub fn extract_transfer_payload(&self) -> Result<TransferPayload, MessageError> {
        if self.message_id != MessageId::Request && self.message_id != MessageId::Cancel {
            return Err(MessageError::DeserializationError);
        }

        let payload = self.payload.as_ref().ok_or(MessageError::PayloadTooShort)?;
        TransferPayload::try_from(payload.as_slice())
    }

    /// Extracts a PiecePayload from a Piece message.
    ///
    /// # Returns
    /// A Result containing the PiecePayload, or a MessageError if extraction fails
    pub fn extract_piece_payload(&self) -> Result<PiecePayload, MessageError> {
        if self.message_id != MessageId::Piece {
            return Err(MessageError::DeserializationError);
        }

        let payload = self.payload.as_ref().ok_or(MessageError::PayloadTooShort)?;
        PiecePayload::try_from(payload.as_slice())
    }
}

/// Represents the payload structure for Request and Cancel messages.
///
/// Contains index, begin, and length fields as per the BitTorrent specification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransferPayload {
    /// The zero-based piece index
    pub index: u32,
    /// The zero-based byte offset within the piece
    pub begin: u32,
    /// The requested length in bytes
    pub length: u32,
}

impl TransferPayload {
    /// Creates a new TransferPayload with the specified parameters.
    ///
    /// # Arguments
    /// * `index` - The zero-based piece index
    /// * `begin` - The zero-based byte offset within the piece
    /// * `length` - The requested length in bytes
    ///
    /// # Returns
    /// A new TransferPayload instance
    pub fn new(index: u32, begin: u32, length: u32) -> Self {
        Self {
            index,
            begin,
            length,
        }
    }

    /// Serializes the TransferPayload into a byte vector.
    ///
    /// Format: <index (4 bytes)><begin (4 bytes)><length (4 bytes)>
    ///
    /// # Returns
    /// A vector of bytes representing the serialized payload
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&self.begin.to_be_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());

        bytes
    }
}

impl TryFrom<&[u8]> for TransferPayload {
    type Error = MessageError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 12 {
            return Err(MessageError::PayloadTooShort);
        }

        let mut index_bytes = [0u8; 4];
        let mut begin_bytes = [0u8; 4];
        let mut length_bytes = [0u8; 4];

        index_bytes.copy_from_slice(&bytes[0..4]);
        begin_bytes.copy_from_slice(&bytes[4..8]);
        length_bytes.copy_from_slice(&bytes[8..12]);

        Ok(Self {
            index: u32::from_be_bytes(index_bytes),
            begin: u32::from_be_bytes(begin_bytes),
            length: u32::from_be_bytes(length_bytes),
        })
    }
}

/// Represents the payload structure for Piece messages.
///
/// Contains index, begin, and block data as per the BitTorrent specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiecePayload {
    /// The zero-based piece index
    pub index: u32,
    /// The zero-based byte offset within the piece
    pub begin: u32,
    /// The actual block data
    pub block: Vec<u8>,
}

impl PiecePayload {
    /// Creates a new PiecePayload with the specified parameters.
    ///
    /// # Arguments
    /// * `index` - The zero-based piece index
    /// * `begin` - The zero-based byte offset within the piece
    /// * `block` - The actual block data
    ///
    /// # Returns
    /// A new PiecePayload instance
    pub fn new(index: u32, begin: u32, block: Vec<u8>) -> Self {
        Self {
            index,
            begin,
            block,
        }
    }

    /// Serializes the PiecePayload into a byte vector.
    ///
    /// Format: <index (4 bytes)><begin (4 bytes)><block (variable)>
    ///
    /// # Returns
    /// A vector of bytes representing the serialized payload
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + self.block.len());
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes.extend_from_slice(&self.begin.to_be_bytes());
        bytes.extend_from_slice(&self.block);
        bytes
    }
}

impl TryFrom<&[u8]> for PiecePayload {
    type Error = MessageError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 8 {
            return Err(MessageError::PayloadTooShort);
        }

        let mut index_bytes = [0u8; 4];
        let mut begin_bytes = [0u8; 4];

        index_bytes.copy_from_slice(&bytes[0..4]);
        begin_bytes.copy_from_slice(&bytes[4..8]);

        let block = bytes[8..].to_vec();

        Ok(Self {
            index: u32::from_be_bytes(index_bytes),
            begin: u32::from_be_bytes(begin_bytes),
            block,
        })
    }
}

/// Errors that can occur when working with BitTorrent protocol messages.
#[derive(Debug, PartialEq, Eq)]
pub enum MessageError {
    /// Buffer is too short to contain a complete message
    BufferTooShort,
    /// Payload is too short to deserialize properly
    PayloadTooShort,
    /// The message ID value is not recognized
    InvalidMessageId(u8),
    /// The message length is invalid
    InvalidLength(usize),
    /// Generic deserialization error
    DeserializationError,
}

impl Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooShort => write!(f, "Buffer too short for complete message"),
            Self::PayloadTooShort => write!(f, "Payload too short for deserialization"),
            Self::InvalidMessageId(id) => write!(f, "Invalid message ID: {}", id),
            Self::InvalidLength(len) => write!(f, "Invalid message length: {}", len),
            Self::DeserializationError => write!(f, "Failed to deserialize message"),
        }
    }
}

impl std::error::Error for MessageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitfield_message_deserialize() {
        let bytes = vec![0, 0, 0, 10, 5, 255, 255, 255, 255, 255, 255, 255, 255, 240];
        let message = Message::deserialize(&bytes).unwrap();

        assert_eq!(message.message_id, MessageId::Bitfield);
        assert_eq!(
            message.payload,
            Some(vec![255, 255, 255, 255, 255, 255, 255, 255, 240])
        );
    }

    #[test]
    fn test_keep_alive_message() {
        let bytes = vec![0, 0, 0, 0];
        let message = Message::deserialize(&bytes).unwrap();

        assert_eq!(message.message_id, MessageId::KeepAlive);
        assert_eq!(message.payload, None);

        // Test serialization
        let serialized = message.serialize();
        assert_eq!(serialized, bytes);
    }

    #[test]
    fn test_choke_message() {
        let bytes = vec![0, 0, 0, 1, 0];
        let message = Message::deserialize(&bytes).unwrap();

        assert_eq!(message.message_id, MessageId::Choke);
        assert_eq!(message.payload, None);

        // Test serialization
        let serialized = message.serialize();
        assert_eq!(serialized, bytes);
    }

    #[test]
    fn test_have_message() {
        let piece_index = 12345u32;
        let have_message = Message::have(piece_index);

        assert_eq!(have_message.message_id, MessageId::Have);
        assert!(have_message.payload.is_some());

        let serialized = have_message.serialize();
        let deserialized = Message::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.message_id, MessageId::Have);
        assert_eq!(deserialized.extract_have_index().unwrap(), piece_index);
    }

    #[test]
    fn test_transfer_payload() {
        let transfer_payload = TransferPayload::new(1, 2, 16384);
        let serialized = transfer_payload.serialize();

        let deserialized = TransferPayload::try_from(serialized.as_slice()).unwrap();
        assert_eq!(deserialized, transfer_payload);

        // Test with Request message
        let request_message = Message::request(&transfer_payload);
        let serialized_request = request_message.serialize();
        let deserialized_request = Message::deserialize(&serialized_request).unwrap();

        assert_eq!(deserialized_request.message_id, MessageId::Request);
        let extracted_payload = deserialized_request.extract_transfer_payload().unwrap();
        assert_eq!(extracted_payload, transfer_payload);
    }

    #[test]
    fn test_piece_payload() {
        let block_data = vec![1, 2, 3, 4, 5];
        let piece_payload = PiecePayload::new(1, 2, block_data.clone());
        let serialized = piece_payload.serialize();

        let deserialized = PiecePayload::try_from(serialized.as_slice()).unwrap();
        assert_eq!(deserialized.index, 1);
        assert_eq!(deserialized.begin, 2);
        assert_eq!(deserialized.block, block_data);

        // Test with Piece message
        let piece_message = Message::piece(&piece_payload);
        let serialized_piece = piece_message.serialize();
        let deserialized_piece = Message::deserialize(&serialized_piece).unwrap();

        assert_eq!(deserialized_piece.message_id, MessageId::Piece);
        let extracted_payload = deserialized_piece.extract_piece_payload().unwrap();
        assert_eq!(extracted_payload.index, piece_payload.index);
        assert_eq!(extracted_payload.begin, piece_payload.begin);
        assert_eq!(extracted_payload.block, piece_payload.block);
    }

    #[test]
    fn test_invalid_message_id() {
        let bytes = vec![0, 0, 0, 1, 123]; // Invalid message ID 123
        let result = Message::deserialize(&bytes);
        assert!(matches!(result, Err(MessageError::InvalidMessageId(123))));
    }

    #[test]
    fn test_buffer_too_short() {
        let bytes = vec![0, 0, 0]; // Too short for length prefix
        let result = Message::deserialize(&bytes);
        assert!(matches!(result, Err(MessageError::BufferTooShort)));

        let bytes = vec![0, 0, 0, 10, 5]; // Claims 10 byte payload but only has 1
        let result = Message::deserialize(&bytes);
        assert!(matches!(result, Err(MessageError::BufferTooShort)));
    }
}
