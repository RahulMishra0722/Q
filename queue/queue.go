package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"time"
)

//!Priority Todo
//! Identifier for consumer and producer should be sent as one of the args so we can set up listners for consumer to listen for the messages sent by producer via tcp

// Todo Set up a listner it looks at how many messages it has a it keeps listning for em

// * we set up a ticker that ticks every say 2 secs and what it also checks if the message queue length == 0 if if it is shut down the listner as soons the new message comes appead the message array and check if the listner is not already running if not start it so itll send the message to the consumer[✅]
// * have a boot state that tells u that the listner thats reading from the queue is running or not[✅]!Done

//Todo
//* Consumer also have to create the queue with the same values that the producer used but if the queue for that name is already there return that queue else create

// * Better errors
// * current flow Producer -> Tcp -> ConsumerChan || What i want => Producer -> QueueMan -> ConsumerTcp -> ConsumerChan
const DEFAULT_RUNTIME_FOR_CONSUMER_LISTNER_BEFORE_IT_SHUTS_DOWN = 5

type QueueMan struct {
	MainServerConn           net.Conn
	Queues                   map[string]MQ
	ConsumerIdTrack          uint16
	Consumers                map[string][]Consumer // key is the consumer id
	ConsumerChan             map[string]chan Message
	IdMu                     sync.RWMutex
	Port                     uint
	MessageQueue             []Message
	IsQueueListnerRunning    bool
	errorChan                chan error
	CurrentMessageId         uint
	ProccessedMessageRecord  map[uint]bool
	IsConsumerListnerRunning map[string]bool
	LastRecivedFromConsumer  map[string]time.Time
	messageMutex             sync.Mutex
	consumerMutex            sync.Mutex
}

type TransportType uint16

const (
	NewQueue         TransportType = 0x00ff
	IncommingMessage TransportType = 0x0001
)

type Consumer struct {
	ConsumerId uint16
	Conn       net.Conn
}

func NewConsumer(ConsumerId uint16, Conn net.Conn) Consumer {
	return Consumer{
		ConsumerId,
		Conn,
	}
}

type MQ struct {
	ConsumerIdTrack uint16
	Name            string
	Durable         bool // Queue survives server restarts
	AutoDelete      bool // Queue is deleted when no consumers are connected
	Exclusive       bool // Queue is used by only one connection and deleted when the connection closes
	NoWait          bool // Do not wait for a server response
}

func (qm *QueueMan) NewMQ(name string, isdurable, autoDelete, exclusive, noWait bool) MQ {
	queue := MQ{
		0,    //id
		name, // name
		isdurable,
		autoDelete,
		exclusive,
		noWait,
		// t, //! we'll get to this when im done with basic structure
	}
	qm.Queues[name] = queue
	return queue
}

type Message struct {
	isProducer bool
	Id         uint
	Key        string // The routing key that determines where the message will be delivered.
	Mandatory, // If true, the broker will return an error if the message cannot be routed.
	Immediate, // If true, the broker will return an error if no consumer is ready to receive the message.
	PersistentDeliveryMode bool
	Body      []byte
	CreatedAt time.Time
	TTL       time.Duration
	Priority  uint //! (1 - 3) 1 = low, 2 = mid, 3 = high i wanna make it more complex for now lets keep it simple
}

/*
*New Queue frame
!Byte 1 Message type
!Byte 2 Padding
!Byte 2 - 42 // Ill restrict the name to 40 bytes so 2 - 42 is the NAME
!Byte 43 [IsDurable]
!Byte 44 [AutoDelete]
!Byte 45 [Exclusive]
!Byte 46 [NoWait]
*/

func (mq *QueueMan) FrameNewQueueRequest(name string, isDurable, autoDelete, exclusive, noWait bool) ([]byte, error) {
	dataBuffLen := 50
	buff := make([]byte, dataBuffLen)
	binary.BigEndian.PutUint16(buff[0:2], uint16(NewQueue))
	fmt.Printf("whats being sent is %d\n", uint16(NewQueue))
	copy(buff[2:42], name) // copy the name
	if isDurable {
		buff[43] = 1
	} else {
		buff[43] = 0
	}
	if autoDelete {
		buff[44] = 1
	} else {
		buff[44] = 0
	}
	if exclusive {
		buff[45] = 1
	} else {
		buff[45] = 0
	}
	if noWait {
		buff[46] = 1
	} else {
		buff[46] = 0
	}

	return buff, nil
}
func LogMessageContents(msg Message) {
	fmt.Println("Message successfully unframed:")
	fmt.Printf("Key: %s\n", msg.Key)
	fmt.Printf("Mandatory: %v\n", msg.Mandatory)
	fmt.Printf("Immediate: %v\n", msg.Immediate)
	fmt.Printf("PersistentDeliveryMode: %v\n", msg.PersistentDeliveryMode)
	fmt.Printf("Body: %s\n", string(msg.Body))
	fmt.Printf("was created at: %v\n", msg.CreatedAt)
	fmt.Printf("TTL is : %v\n", msg.TTL)
	fmt.Printf("Priority is : %v\n", msg.Priority)

}
func (qm *QueueMan) MessageQueueProcessor() error {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			qm.messageMutex.Lock()
			if len(qm.MessageQueue) == 0 {
				qm.messageMutex.Unlock()
				fmt.Println("shutting down message processor, all messages processed...")
				qm.IsQueueListnerRunning = false
				return nil
			}
			msg := qm.MessageQueue[0]
			qm.MessageQueue = qm.MessageQueue[1:]
			qm.messageMutex.Unlock()

			data := qm.FrameIncomingMessage(
				msg.Key,
				msg.isProducer,
				msg.TTL,
				msg.CreatedAt,
				msg.Mandatory,
				msg.Immediate,
				msg.PersistentDeliveryMode,
				msg.Body,
			)

			var wg sync.WaitGroup
			for _, consumer := range qm.Consumers[msg.Key] {
				wg.Add(1)
				go func(cons Consumer, msgData []byte) {
					defer wg.Done()

					qm.consumerMutex.Lock()
					if !qm.IsConsumerListnerRunning[msg.Key] {
						go qm.SetUpListenerForConsumer(cons.Conn, msg.Key, time.Now())
					}
					qm.consumerMutex.Unlock()

					qm.SendToAllTheConsumersOfThisQueue(msg.Key, cons.Conn, msgData)
				}(consumer, data)
			}

			wg.Wait()

			qm.messageMutex.Lock()
			qm.ProccessedMessageRecord[msg.Id] = true
			qm.RemoveMessage(msg.Id)
			qm.messageMutex.Unlock()

		default:

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (qm *QueueMan) SendToAllTheConsumersOfThisQueue(queueName string, conn net.Conn, data []byte) error {
	var n int
	var err error
	fmt.Println("i was called")
	if n, err = conn.Write(data); err != nil {
		return fmt.Errorf("error writing data to consumer %v", err)
	}

	fmt.Printf("wrote %d bytes\n", n)

	return nil
}
func (qm *QueueMan) UnFrameNewQueueRequest(reader io.Reader) (MQ, error) {
	header := make([]byte, 47)
	if _, err := io.ReadFull(reader, header); err != nil {
		return MQ{}, fmt.Errorf("error reading the header %v", err)
	}
	MType := binary.BigEndian.Uint16(header[0:2])
	fmt.Println(MType)
	name := string(header[2:42])

	isDurable := header[43] == 1
	AutoDelete := header[44] == 1
	Exclusive := header[45] == 1
	NoWait := header[46] == 1

	q := qm.NewMQ(name, isDurable, AutoDelete, Exclusive, NoWait)

	return q, nil
}

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

func (qm *QueueMan) FrameIncomingMessage(
	name string,
	isProducer bool,
	TTL time.Duration,
	createdAt time.Time,
	mandatory, immediate, PersistentDeliveryMode bool,
	data []byte,
) []byte {
	const (
		TTL_LENGTH_IN_BYTES = 8
		CREATED_AT_IN_BYTES = 24
		MAX_KEY_LENGTH      = 40
	)

	// Truncate key if too long
	key := []byte(name)
	if len(key) > MAX_KEY_LENGTH {
		key = key[:MAX_KEY_LENGTH]
	}

	totalLen := 2 + // Message Type (uint16)
		1 + // isProducer (byte)
		TTL_LENGTH_IN_BYTES + // TTL
		CREATED_AT_IN_BYTES + // CreatedAt
		2 + // Key Length
		len(key) + // Key
		3 + // Flags (mandatory, immediate, PersistentDeliveryMode)
		2 + // Message Priority
		4 + // Data length
		len(data) // Actual data

	buff := make([]byte, totalLen)

	// Message Type
	binary.BigEndian.PutUint16(buff[0:2], uint16(IncommingMessage))

	if isProducer {
		buff[2] = 1
	}

	binary.BigEndian.PutUint64(buff[3:11], uint64(TTL))

	wallTime := createdAt.Unix()
	nanos := createdAt.Nanosecond()
	_, offset := createdAt.Zone()

	binary.BigEndian.PutUint64(buff[11:19], uint64(wallTime))
	binary.BigEndian.PutUint32(buff[19:23], uint32(nanos))
	binary.BigEndian.PutUint32(buff[23:27], uint32(offset))

	binary.BigEndian.PutUint16(buff[27:29], uint16(len(key)))
	copy(buff[29:29+len(key)], key)

	flagOffset := 29 + len(key)
	if mandatory {
		buff[flagOffset] = 1
	}
	if immediate {
		buff[flagOffset+1] = 1
	}
	if PersistentDeliveryMode {
		buff[flagOffset+2] = 1
	}

	binary.BigEndian.PutUint16(buff[flagOffset+3:flagOffset+5], 3)

	binary.BigEndian.PutUint32(buff[flagOffset+5:flagOffset+9], uint32(len(data)))

	copy(buff[flagOffset+9:], data)

	return buff
}

func (qm *QueueMan) UnFrameIncomingMessage(reader io.Reader) (Message, error) {

	typeBuf := make([]byte, 2)
	if _, err := io.ReadFull(reader, typeBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read message type: %v", err)
	}
	messageType := binary.BigEndian.Uint16(typeBuf)

	if messageType != uint16(IncommingMessage) {
		return Message{}, fmt.Errorf("unexpected message type: %d (0x%x)", messageType, messageType)
	}

	producerBuf := make([]byte, 1)
	if _, err := io.ReadFull(reader, producerBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read producer flag: %v", err)
	}
	value := producerBuf[0] == 1
	fmt.Println(value)
	ttlAndCreatedAtBuffer := make([]byte, 24)
	if _, err := io.ReadFull(reader, ttlAndCreatedAtBuffer); err != nil {
		return Message{}, fmt.Errorf("failed to read TTL and CreatedAt: %v", err)
	}

	TTL := time.Duration(binary.BigEndian.Uint64(ttlAndCreatedAtBuffer[0:8]))
	seconds := binary.BigEndian.Uint64(ttlAndCreatedAtBuffer[8:16])
	nanos := binary.BigEndian.Uint32(ttlAndCreatedAtBuffer[16:20])
	offSet := binary.BigEndian.Uint32(ttlAndCreatedAtBuffer[20:24])

	createdAt := time.Unix(int64(seconds), int64(nanos)).In(time.FixedZone("", int(offSet)))

	lenBuf := make([]byte, 2)
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read key length: %v", err)
	}
	keyLength := binary.BigEndian.Uint16(lenBuf)

	key := make([]byte, keyLength)
	if _, err := io.ReadFull(reader, key); err != nil {
		return Message{}, fmt.Errorf("failed to read key: %v", err)
	}

	flags := make([]byte, 3)
	if _, err := io.ReadFull(reader, flags); err != nil {
		return Message{}, fmt.Errorf("failed to read message flags: %v", err)
	}

	mandatory := flags[0] == 1
	immediate := flags[1] == 1
	persistentDeliveryMode := flags[2] == 1

	prioritySizeByte := make([]byte, 2)
	if _, err := io.ReadFull(reader, prioritySizeByte); err != nil {
		return Message{}, fmt.Errorf("failed to read priority: %v", err)
	}
	priority := binary.BigEndian.Uint16(prioritySizeByte)

	// Read Data Length
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read data length: %v", err)
	}
	dataLen := binary.BigEndian.Uint32(lengthBuf)

	// Read Data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(reader, data); err != nil {
		return Message{}, fmt.Errorf("failed to read data: %v", err)
	}

	m := Message{
		Id:                     qm.GenNewMessageId(),
		isProducer:             value,
		Key:                    string(key),
		TTL:                    TTL,
		CreatedAt:              createdAt,
		Mandatory:              mandatory,
		Immediate:              immediate,
		PersistentDeliveryMode: persistentDeliveryMode,
		Body:                   data,
		Priority:               uint(priority),
	}
	LogMessageContents(m)
	return m, nil
}
func (qm *QueueMan) RemoveMessage(id uint) {
	qm.IdMu.Lock()
	defer qm.IdMu.Unlock()
	for i := 0; i < len(qm.MessageQueue); i++ {
		if qm.MessageQueue[i].Id == id {
			qm.MessageQueue = append(qm.MessageQueue[:i], qm.MessageQueue[i+1:]...)
		}
	}
}
func (qm *QueueMan) SortBasedOnPriority() {
	sort.Slice(qm.MessageQueue, func(i, j int) bool {
		return qm.MessageQueue[i].Priority > qm.MessageQueue[j].Priority
	})
}
func (mq *QueueMan) SendMessageToConsumerChan(ch chan Message, msg Message) error {
	if ch == nil {
		log.Println("Consumer channel is nil, cannot send message")
		return fmt.Errorf("consumer channel is nil")
	}
	select {
	case ch <- msg:
		log.Printf("Message sent to consumer channel: %+v", msg)
	default:
		log.Println("Consumer channel is full, dropping message")
		return fmt.Errorf("consumer channel is full")
	}
	return nil
}

func (mq *QueueMan) EjectMessageOnceTTLExpires(ttl time.Duration, msgId uint) error {
	t := time.NewTicker(ttl)
	defer t.Stop()

	for {
		mq.IdMu.RLock()
		processed := mq.ProccessedMessageRecord[msgId]
		mq.IdMu.RUnlock()

		if processed {
			fmt.Printf("Exiting TTL listener for message ID: %d as it has been processed.\n", msgId)
			return nil
		}

		select {
		case <-t.C:
			mq.IdMu.RLock()
			processed := mq.ProccessedMessageRecord[msgId]
			mq.IdMu.RUnlock()

			if !processed {
				fmt.Printf("TTL expired. Ejecting the message with ID: %d...\n", msgId)
				mq.RemoveMessage(msgId)
				fmt.Printf("The length of the message queue is now %d.\n", len(mq.MessageQueue))
				return nil
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (mq *QueueMan) HandleIncommingMessage(reader io.Reader) error {
	msg, err := mq.UnFrameIncomingMessage(reader)
	if err != nil {
		fmt.Println("Error in HandleIncommingMessage:", err)
		return err
	}
	mq.MessageQueue = append(mq.MessageQueue, msg)
	go mq.SortBasedOnPriority()
	go mq.EjectMessageOnceTTLExpires(msg.TTL, msg.Id)
	if !mq.IsQueueListnerRunning {
		go mq.MessageQueueProcessor()
		mq.IsQueueListnerRunning = true
	}
	return nil
}

func (mq *QueueMan) handleListner(conn net.Conn) error {
	reader := bufio.NewReader(conn)
	for {
		typeBuf := make([]byte, 2)
		_, err := io.ReadFull(reader, typeBuf)
		if err != nil {
			return err
		}

		mType := binary.BigEndian.Uint16(typeBuf)
		//fmt.Printf("Received message type: %d (0x%x)\n", mType, mType)

		isProducerBuf := make([]byte, 1)

		_, err = io.ReadFull(reader, isProducerBuf)
		if err != nil {
			return err
		}
		resetBytes := append(typeBuf, isProducerBuf...)
		reader = bufio.NewReader(io.MultiReader(bytes.NewReader(resetBytes), reader))
		isProducer := isProducerBuf[0] != 0
		fmt.Printf("is producer %v\n", isProducer)
		if !isProducer {

			msg, err := mq.UnFrameIncomingMessage(reader)
			if err != nil {
				return fmt.Errorf("error trying to unframe New Queue Request: %v", err)
			}
			consumer := NewConsumer(uint16(msg.Id), conn)

			mq.Consumers[msg.Key] = append(mq.Consumers[msg.Key], consumer)
			fmt.Printf("the length of the consumer is %d\n", len(mq.Consumers))
			go func() {
				err = mq.SetUpListenerForConsumer(conn, msg.Key, time.Now())
				if err != nil {
					fmt.Println(err)
				}
			}()
			fmt.Println("closing handler for consumer since we dont need it")
			return nil
		} else {

			fmt.Printf("is producer: %v\n", isProducer)

			switch TransportType(mType) {
			case NewQueue:
				fmt.Println("Handling New Queue Request")
				_, err := mq.UnFrameNewQueueRequest(reader)
				if err != nil {
					return fmt.Errorf("error trying to unframe New Queue Request: %v", err)
				}
			case IncommingMessage:
				fmt.Println("Handling Incoming Message")
				err := mq.HandleIncommingMessage(reader)
				if err != nil {
					return fmt.Errorf("error trying to unframe the incoming message: %v", err)
				}
			default:
				return fmt.Errorf("unknown message type: %v", mType)
			}

		}

	}
}

func (mq *QueueMan) SetUpListenerForConsumer(conn net.Conn, queueName string, startTime time.Time) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	fmt.Println("started SetUpListenerForConsumer")
	reader := bufio.NewReader(conn)

	mq.IsConsumerListnerRunning[queueName] = true
	defer func() {
		mq.IsConsumerListnerRunning[queueName] = false
	}()

	for time.Since(startTime) < time.Minute*DEFAULT_RUNTIME_FOR_CONSUMER_LISTNER_BEFORE_IT_SHUTS_DOWN {
		fmt.Printf("im running for queue %s\n", queueName)
		msg, err := mq.UnFrameIncomingMessage(reader)

		LogMessageContents(msg)
		fmt.Printf("im after the message unframe %s\n", queueName)

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error unframing incoming message: %v", err)
		}

		if err := mq.SendMessageToConsumerChan(mq.ConsumerChan[queueName], msg); err != nil {
			log.Printf("Failed to send message to consumer channel: %v", err)
		}
	}
	return nil
}
func (mq *QueueMan) RegisterConsumer(QueueName string, ConsumerConn net.Conn) {
	if mq.ConsumerChan[QueueName] == nil {
		mq.ConsumerChan[QueueName] = make(chan Message, 100)
	}
	data := mq.FrameIncomingMessage(QueueName, false, 2*time.Minute, time.Now(), false, false, false, []byte{})
	if _, err := ConsumerConn.Write(data); err != nil {
		fmt.Printf("error occured trying to send fake data to let the server know we are consumer")
	}

	go func() {
		mq.SetUpListenerForConsumer(ConsumerConn, QueueName, time.Now())
		mq.IsConsumerListnerRunning[QueueName] = true
	}()
}

func (mq *QueueMan) RegisterProducer(name string, Durable, AutoDelete, Exclusive, NoWait bool) {
	queueData, err := mq.FrameNewQueueRequest(name, Durable, AutoDelete, Exclusive, NoWait)

	mq.ErrorMustNotBeThere(err)
	mq.TryDeliverToBroker(queueData)
}
func (mq *QueueMan) Consume(QueueName string) <-chan Message {
	ch := mq.ConsumerChan[QueueName]
	return ch
}

func (qm *QueueMan) Dial(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp4", addr)
	if err != nil {
		return &net.TCPConn{}, err
	}
	return conn, nil
}
func (mq *QueueMan) Produce(name string, mandatory, immidiate, PersistentDeliveryMode bool, data []byte, ttl time.Duration, createdAt time.Time) error {

	IncommingMessageInbytes := mq.FrameIncomingMessage(name, true, ttl, createdAt, mandatory, immidiate, PersistentDeliveryMode, data)

	fmt.Printf("Framed Message Bytes (Length: %d): %v\n", len(IncommingMessageInbytes), IncommingMessageInbytes)

	err := mq.TryDeliverToBroker(IncommingMessageInbytes)
	return err
}

// To Test
// start the server
// register the producer by creating a queue by calling RegisterProducer
// register the consumer by calling RegisterConsumer
// consume by reading from the chan thats returned by consume func

func (mq *QueueMan) Start() error {
	address := fmt.Sprintf(":%d", mq.Port)
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		return fmt.Errorf("could not start listener on port %d: %v", mq.Port, err)
	}
	defer listener.Close()

	fmt.Printf("Server listening on port %d\n", mq.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %v", err)
		}
		go mq.handleListner(conn)

	}
}

func (qm *QueueMan) TryDeliverToBroker(data []byte) error {
	address := fmt.Sprintf("localhost:%d", qm.Port)
	conn, err := net.Dial("tcp4", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write(data)
	if err != nil {
		return err
	}
	return nil
}
func (qm *QueueMan) ErrorMustNotBeThere(err error) bool {
	if err != nil {
		qm.errorChan <- err

		return true
	}
	return false
}

func (m *QueueMan) GetConsumerId() uint16 {

	m.ConsumerIdTrack++

	return m.ConsumerIdTrack
}
func (qm *QueueMan) GenNewMessageId() uint {

	qm.CurrentMessageId++

	return qm.CurrentMessageId
}
func main() {
	queueManager := &QueueMan{
		Queues:                   make(map[string]MQ),
		Consumers:                make(map[string][]Consumer),
		ConsumerChan:             make(map[string]chan Message),
		ProccessedMessageRecord:  make(map[uint]bool),
		IsConsumerListnerRunning: map[string]bool{},
		Port:                     8081,
		IsQueueListnerRunning:    false,
	}

	queueManager.ConsumerChan["testqueue"] = make(chan Message, 100)

	serverErrChan := make(chan error, 1)
	go func() {
		err := queueManager.Start()
		if err != nil {
			serverErrChan <- fmt.Errorf("error starting server: %v", err)
		}
	}()

	time.Sleep(time.Second)

	select {
	case err := <-serverErrChan:
		fmt.Println(err)
		return
	default:
	}

	queueManager.RegisterProducer("testqueue", true, false, false, false)

	consumerConn, err := net.Dial("tcp4", "localhost:8081")
	if err != nil {
		fmt.Println("Error connecting consumer:", err)
		return
	}
	defer consumerConn.Close()

	queueManager.RegisterConsumer("testqueue", consumerConn)

	err = queueManager.Produce("testqueue", true, false, true, []byte("Hello, World!"), time.Duration(time.Minute)*10, time.Now())
	if err != nil {
		fmt.Println("Error producing message:", err)
		return
	}

	messageChan := queueManager.Consume("testqueue")
	for msg := range messageChan {
		fmt.Println("Received message: in the consumer reader chan!")
		LogMessageContents(msg)
	}
}
