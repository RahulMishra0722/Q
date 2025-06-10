*New Queue frame
* Frame Structure:
* [0-1]   : Message Type (2 bytes, uint16)
* [2-10]  : Retry Delay (8 bytes, uint64)
* [10-12] : Retry Limit (2 bytes, uint16)
* [12-14] : Queue Key Length (2 bytes, uint16)
* [14:14 + Key Length] Data
* [14 + Key Length:14 + Key Length ]   : Queue Key (variable length)
*           - Durable Flag (1 byte)
*           - Auto Delete Flag (1 byte)
*           - Exclusive Flag (1 byte)
*           - NoWait Flag (1 byte)
 */
// Parameters:
//   - name: The name/key of the queue to be created
//   - RetryLimit: Maximum number of retry attempts for queue creation
//   - RetryDelay: Duration to wait between retry attempts
//   - isDurable: Indicates if the queue should persist between server restarts
//   - autoDelete: Determines if the queue should be automatically deleted when no longer in use
//   - exclusive: Specifies if the queue is exclusive to the current connection
//   - noWait: Indicates whether the method should wait for a response from the server


/*
 *  Byte[0:2]   = Type
 *               - 2 bytes representing the type of message (e.g., IncomingMessage).
 *
 *  Byte[2:10]  = TTL (Time-to-Live as time.Duration)
 *               - 8 bytes for the TTL, stored as a uint64.
 *
 *  Byte[10:34] = Message Created At (time.Time)
 *               - 8 bytes for the Unix timestamp (seconds since epoch).
 *               - 4 bytes for the nanoseconds part of the time.
 *               - 4 bytes for the timezone offset (in seconds).
 *               - Total: 24 bytes.
 *
 *  Byte[34:36] = Key Length
 *               - 2 bytes representing the length of the key/name associated with the queue.
 *
 *  Byte[36:36 + Key_Length] = Key/Name
 *               - Variable-length field representing the key, starting at byte 36 and continuing
 *                 for `Key_Length` bytes. Truncated to a maximum of 40 bytes.
 *
 *  Byte[36 + Key_Length] = Mandatory flag
 *               - 1 byte indicating whether the message is mandatory (1 for true, 0 for false).
 *
 *  Byte[36 + Key_Length + 1] = Immediate flag
 *               - 1 byte indicating whether the message requires immediate delivery (1 for true, 0 for false).
 *
 *  Byte[36 + Key_Length + 2] = Persistent Delivery Mode flag
 *               - 1 byte indicating whether the message has persistent delivery mode enabled (1 for true, 0 for false).

 *  Byte[36 + Key_Length + 3: 36 + Key_Length + 5] = Message Priority
 *               - Next 2 bytes contain the priority of the message

 *  Byte[36 + Key_Length + 6:36 + Key_Length + 10] = Data Length
 *               - 4 bytes representing the length of the `data` field as a uint32.
 *
 *  Byte[36 + Key_Length + 10:end] = Data
 *               - The actual message data, starting at `36 + Key_Length + 10` and continuing to the end of the buffer.
 */
