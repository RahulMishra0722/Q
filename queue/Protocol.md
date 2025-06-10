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
