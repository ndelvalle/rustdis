use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Increment the string representing a floating point number stored at key by the specified
/// increment. By using a negative increment value, the result is that the value stored at the key
/// is decremented (by the obvious properties of addition). If the key does not exist, it is set to
/// 0 before performing the operation.
///
/// Ref: <https://redis.io/docs/latest/commands/incrbyfloat/>
///
/// TODO:
/// * Handle overflow errors.
/// * The precision of the output is fixed at 17 digits after the decimal point regardless of the
///   actual internal precision of the computation.
/// * Both the value already contained in the string key and the increment argument can be
///   optionally provided in exponential notation.

#[derive(Debug, PartialEq)]
pub struct IncrByFloat {
    pub key: String,
    pub increment: f64,
}

impl Executable for IncrByFloat {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let mut store = store.lock();
        let res = store.incr_by::<f64, String>(&self.key, self.increment);
        match res {
            Ok(res) => Ok(Frame::Bulk(res.into())),
            Err(msg) => Ok(Frame::Error(msg.to_string())),
        }
    }
}

impl TryFrom<&mut CommandParser> for IncrByFloat {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let increment = parser.next_float()?;

        Ok(Self { key, increment })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::commands::Command;

    #[tokio::test]
    async fn existing_key() {
        let store = Store::new();
        store.lock().set(String::from("key1"), Bytes::from("10.50"));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBYFLOAT")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("0.1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrByFloat(IncrByFloat {
                key: "key1".to_string(),
                increment: 0.1,
            })
        );

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Bulk(Bytes::from("10.59999999999999964")));
        assert_eq!(
            store.lock().get("key1"),
            Some(Bytes::from("10.59999999999999964"))
        );
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBYFLOAT")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrByFloat(IncrByFloat {
                key: "key1".to_string(),
                increment: 10.00,
            })
        );

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Bulk(Bytes::from("10")));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("10")));
    }

    #[tokio::test]
    async fn invalid_key_type() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBYFLOAT")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrByFloat(IncrByFloat {
                key: "key1".to_string(),
                increment: 10.00,
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("value"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not an integer or out of range".to_string())
        );
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("value")));
    }
}
