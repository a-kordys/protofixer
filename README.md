# protofixer
A small library in Rust that takes a protobuf serialized message and sorts fields inside, so that the field order becomes deterministic.

## Example:
```rust
use prost::Message;
use protofixer::sort_protobuf_message;

fn encode_canonically(object: &MyData) -> Vec<u8> {
    let msg = Message::encode_to_vec(object);
    let canonical = sort_protobuf_message(&msg).expect("bad output of Message::encode_to_vec()");
    canonical.to_owned()
}
```