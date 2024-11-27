package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type QueueMan struct {
	MainServerConn  net.Conn
	Queues          map[string]MQ
	ConsumerIdTrack uint16
	Consumers       map[string][]Consumer // key is the consumer id
	ConsumerChan    map[string]chan Message
	IdMu            sync.RWMutex
	Port            uint // Add a port field
}

type TransportType uint16

const (
	NewQueue         TransportType = 0x00ff
	IncommingMessage TransportType = 0x0001
)

type TransportMessage struct {
	Id               uint16
	ProducerQueueKey string
}
type Consumer struct {
	ConsumerId   uint16
	Conn         net.Conn
	ListningChan chan TransportMessage
}
type Table struct { // new queue that will be created for retry if the message fails
	TTL                  time.Duration
	ShouldRetryOnFailure bool
	Name                 string // use the same name as the que so its easy to find the consumers for this queue so we can send the message after checks
}

func ErrorMustNotBeThere(err error) {
	if err != nil {
		panic(err)
	}
}
func NewConsumer(ConsumerId uint16, Conn net.Conn, ProducerQueueKey string) Consumer {
	return Consumer{
		ConsumerId,
		Conn,
		make(chan TransportMessage),
	}
}
func (m *QueueMan) GetConsumerId() uint16 {
	m.IdMu.Lock()
	m.ConsumerIdTrack++
	m.IdMu.Unlock()
	return m.ConsumerIdTrack
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
	Key        string // The routing key that determines where the message will be delivered.
	Mandatory, // If true, the broker will return an error if the message cannot be routed.
	Immediate, // If true, the broker will return an error if no consumer is ready to receive the message.
	PersistentDeliveryMode bool
	Body      []byte
	CreatedAt time.Time
	TTL       time.Duration
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
*    Byte[2:4] = Key Length [Context: 2 till 4 is the length of the key of the queue]
*    Byte[4: 4 + Key_Length] = key/name [Context: Since the key length ends at 4th byte we start from 4th byte and go till the length of the key]
*    Byte[4 + Key_Length] = mandatory   [Context: wherevere the key length eneded we can store the config values by adding 1 to it or 4 + length of the key till however long of a byte u want given its under the buff size we accuired]
*    Byte[4 + Key_Length + 1] = immediate,
*    Byte[4 + Key_Length + 2] = PersistentDeliveryMode [Context: "" same as above 1 + 1 = 2 ]
*    Byte[4 + Key_Length + 3: 4 + Key_Length + 7] Data Length [Context same as above  1 + 1 + 1 = 3 + 4  + keylength + 3 + 4 [4 is the next 4 bytes that we are aquring to set the length of the data]= index of where we currently are]
*    Byte[4 + Key_Length + 3 + 4 + Data Length]Data 1 + 1 + 1 =  [3 + 4] 7 + keylength : till however long the data is
 */
/*
*    Byte[0:2] = Type
*    Byte[2:10] = TTL (Time-to-Live as time.Duration): 8 bytes
*    Byte[10:34] = Message Created At (time.Time): 24 bytes
*    Byte[34:36] = Key Length [Context: 2 bytes representing the length of the queue key]
*    Byte[36: 36 + Key_Length] = Key/Name [Context: Starts at byte 36 and continues for the length of the key]
*    Byte[36 + Key_Length] = Mandatory flag [Context: Indicates if the message is mandatory]
*    Byte[36 + Key_Length + 1] = Immediate flag
*    Byte[36 + Key_Length + 2] = Persistent Delivery Mode flag
*    Byte[36 + Key_Length + 3: 36 + Key_Length + 7] = Data Length [Context: 4 bytes representing the length of the data]
*    Byte[36 + Key_Length + 7: end] = Data [Context: Actual message data, starting after data length]
 */
func (qm *QueueMan) FrameIncommingMessage(name string, TTL time.Duration, createdAt time.Time, mandatory, immediate, PersistentDeliveryMode bool, data []byte) []byte {
	TTL_LENGTH_IN_BYTES := 8
	CREATED_AT_IN_BYTES := 24
	key := []byte(name)
	if len(key) > 40 {
		key = key[:40]
	}

	totalLen := 2 + //Type
		2 + //Key Length
		len(key) + //Key
		3 + // (mandator) + (immediate)+ (PersistentDeliveryMode)
		4 + // Data length
		len(data) // data

	buff := make([]byte, totalLen)
	Key_Length := len(key)
	binary.BigEndian.PutUint16(buff[0:2], uint16(IncommingMessage)) //set bytes

	binary.BigEndian.PutUint16(buff[2:4], uint16(len(key))) // set key length

	copy(buff[4:4+Key_Length], key) //  copy key

	flagOffset := 4 + Key_Length // its the index of where we left of after copying the key
	buff[flagOffset] = 0
	buff[flagOffset+1] = 0
	buff[flagOffset+2] = 0

	if mandatory {
		buff[flagOffset] = 1
	}
	if immediate {
		buff[flagOffset+1] = 1
	}
	if PersistentDeliveryMode {
		buff[flagOffset+2] = 1
	}

	binary.BigEndian.PutUint32(buff[flagOffset+3:flagOffset+7], uint32(len(data)))

	copy(buff[flagOffset+7:], data)

	return buff
}

func (mq *QueueMan) UnFrameIncommingMessage(reader io.Reader) (Message, error) {

	fmt.Println("Starting UnFrameIncommingMessage")

	typeBuf := make([]byte, 2)
	if _, err := io.ReadFull(reader, typeBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read message type: %v", err)
	}
	messageType := binary.BigEndian.Uint16(typeBuf)

	fmt.Printf("Unframing message - Type received: %d (0x%x)\n", messageType, messageType)

	if messageType != uint16(IncommingMessage) {
		return Message{}, fmt.Errorf("unexpected message type: %d (0x%x)", messageType, messageType)
	}

	lenBuf := make([]byte, 2)
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read key length: %v", err)
	}
	keyLength := binary.BigEndian.Uint16(lenBuf)

	fmt.Printf("Key length: %d\n", keyLength)

	key := make([]byte, keyLength)
	if _, err := io.ReadFull(reader, key); err != nil {
		return Message{}, fmt.Errorf("failed to read key: %v", err)
	}

	fmt.Printf("Key: %s\n", string(key))

	// Read flags
	flags := make([]byte, 3)
	if _, err := io.ReadFull(reader, flags); err != nil {
		return Message{}, fmt.Errorf("failed to read message flags: %v", err)
	}

	fmt.Printf("Flags: %v\n", flags)

	mandatory := flags[0] == 1
	immediate := flags[1] == 1
	persistentDeliveryMode := flags[2] == 1

	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBuf); err != nil {
		return Message{}, fmt.Errorf("failed to read data length: %v", err)
	}
	dataLen := binary.BigEndian.Uint32(lengthBuf)

	fmt.Printf("Data length: %d\n", dataLen)

	// Read data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(reader, data); err != nil {
		return Message{}, fmt.Errorf("failed to read data: %v", err)
	}

	fmt.Printf("Data: %s\n", string(data))

	return Message{
		Key:                    string(key),
		Mandatory:              mandatory,
		Immediate:              immediate,
		PersistentDeliveryMode: persistentDeliveryMode,
		Body:                   data,
	}, nil
}

func (mq *QueueMan) HandleIncommingMessage(reader io.Reader) error {
	msg, err := mq.UnFrameIncommingMessage(reader)
	if err != nil {
		fmt.Println("Error in HandleIncommingMessage:", err)
		return err
	}

	fmt.Println("Message successfully unframed:")
	fmt.Printf("Key: %s\n", msg.Key)
	fmt.Printf("Mandatory: %v\n", msg.Mandatory)
	fmt.Printf("Immediate: %v\n", msg.Immediate)
	fmt.Printf("PersistentDeliveryMode: %v\n", msg.PersistentDeliveryMode)
	fmt.Printf("Body: %s\n", string(msg.Body))

	ch := mq.ConsumerChan[msg.Key]
	ch <- msg
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
		fmt.Printf("Received message type: %d (0x%x)\n", mType, mType)

		// Reset the reader to include the type bytes we just read
		reader = bufio.NewReader(io.MultiReader(bytes.NewReader(typeBuf), reader))

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

func (mq *QueueMan) SetUpListnerForConsumer(conn net.Conn, queueName string) error {
	reader := bufio.NewReader(conn)
	for {
		msg, err := mq.UnFrameIncommingMessage(reader)
		if err != nil {
			return fmt.Errorf("error unframing the incomming message : %v", err)
		}
		ch := mq.ConsumerChan[queueName]
		ch <- msg
	}
}
func (mq *QueueMan) RegisterConsumer(QueueName string, ConsumerConn net.Conn) {
	consumer := NewConsumer(mq.GetConsumerId(), ConsumerConn, QueueName)
	mq.Consumers[QueueName] = append(mq.Consumers[QueueName], consumer)
	ch := make(chan Message)
	mq.ConsumerChan[QueueName] = ch
	go func() {
		mq.handleListner(ConsumerConn)
	}()
}
func (mq *QueueMan) RegisterProducer(name string, Durable, AutoDelete, Exclusive, NoWait bool) {
	queueData, err := mq.FrameNewQueueRequest(name, Durable, AutoDelete, Exclusive, NoWait)
	ErrorMustNotBeThere(err)
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
func (mq *QueueMan) Produce(name string, mandatory, immidiate, PersistentDeliveryMode bool, data []byte) error {
	fmt.Printf("Producing Message - Debug Input:\n")
	fmt.Printf("Queue Name: %s\n", name)
	fmt.Printf("Mandatory: %v\n", mandatory)
	fmt.Printf("Immediate: %v\n", immidiate)
	fmt.Printf("PersistentDeliveryMode: %v\n", PersistentDeliveryMode)
	fmt.Printf("Data: %s\n", string(data))

	IncommingMessageInbytes := mq.FrameIncommingMessage(name, mandatory, immidiate, PersistentDeliveryMode, data)

	// Debug the framed bytes
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

func main() {
	queueManager := &QueueMan{
		Queues:       make(map[string]MQ),
		Consumers:    make(map[string][]Consumer),
		ConsumerChan: make(map[string]chan Message),
		Port:         8081,
	}

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

	err = queueManager.Produce("testqueue", true, false, true, []byte("Hello, World!"))
	if err != nil {
		fmt.Println("Error producing message:", err)
		return
	}

	messageChan := queueManager.Consume("testqueue")
	for msg := range messageChan {
		fmt.Println("Received message: in the consumer reader chan!")
		fmt.Printf("Key: %s\n", msg.Key)
		fmt.Printf("Mandatory: %v\n", msg.Mandatory)
		fmt.Printf("Immediate: %v\n", msg.Immediate)
		fmt.Printf("PersistentDeliveryMode: %v\n", msg.PersistentDeliveryMode)
		fmt.Printf("Body: %s\n", string(msg.Body))
	}
}
