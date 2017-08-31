# coding: utf-8
import os, sys, datetime
from confluent_kafka import Producer, Consumer, TopicPartition


def string2int(s):
    return int(s)


def string2string(s):
    return str(s)


class Option:
    def __init__(self, name, tag, default, conv=None, help=None):
        if conv is None:
            if type(default) == type(""):
                conv = string2string
            else:
                conv = string2int
        self.__info = (name, tag, default, conv, help)

    @property
    def name(self):
        return self.__info[0]

    @property
    def tag(self):
        return self.__info[1]

    @property
    def default(self):
        return self.__info[2]

    @property
    def conv(self):
        return self.__info[3]

    @property
    def help(self):
        return self.__info[4]


class Options:
    options = (
        Option("host", "-h", "127.0.0.1"),
        Option("port", "-p", 9092),
        Option("record_num", "-r", 10000),
        Option("topic", "-t", "__benchmark"),
        Option("partition", "-P", 0),
        Option("file", "-f", os.path.abspath(__file__).split(".")[0] + ".py"),
        Option("record_length", "-l", 0))

    def __init__(self, options=None):
        if options is None:
            options = Options.options
        self.options = options
        self.__options = {item.tag: item for item in options}
        assert len(self.__options) == len(options)
        self.__values = {}

    def parse_option(self):
        i = 1
        while i < len(sys.argv):
            option = self.__options.get(sys.argv[i])
            if option is not None:
                i += 1
                if i >= len(sys.argv):
                    print(self.__usage(sys.argv[0]))
                    return False
                self.__values[option.name] = option.conv(sys.argv[i])
            else:
                print(self.__usage(sys.argv[0]))
                return False
            i += 1
        return True

    def __usage(self, name):
        temp = "python " + name
        for option in self.options:
            temp += " [%s %s]" % (option.tag, option.name)
        return temp

    def set(self, name, value):
        self.__values[name] = value

    def get(self, name, default=None):
        value = self.__values.get(name)
        if value is not None:
            return value
        if default:
            return default
        for option in self.__options.values():
            if option.name == name:
                return option.default
        return None

    def __str__(self):
        temp = ""
        for option in self.options:
            temp += " %s: %s\r\n" % (option.name, str(self.get(option.name)))
        return temp


class KafkaMsg:
    def __init__(self, options):
        self.options = options
        length = options.get("length", 0)
        if length > 0:
            prefix = "-" * (length - 1)
            self.lines = ["%s%d" % (prefix, i) for i in range(10)]
        else:
            with open(options.get("file"), "r") as fp:
                self.lines = fp.readlines()
        self.size = len(self.lines)
        self.key = options.get("tag", str(datetime.datetime.now())) + " "

    def hook_get_key_and_value(self, index):
        return (self.key + str(index), self.lines[index % self.size])


if __name__ == "__main__":
    # "usage python confluent_kafka_test.py -h 127.0.0.1 -r 100005"
    # dist-packages/confluent_kafka-0.11.0-py3.5-linux-x86_64.egg/confluent_kafka
    # dist-packages/confluent_kafka, 0.9
    # librdkafka maybe the last version,
    options = Options()
    options.set("port", 9092)
    options.set("length", 10)
    if options.parse_option() is False:
        exit(100)
    # print(options)

    server = "%s:%d" % (options.get("host"), options.get("port"))
    topic = options.get("topic")
    partition = options.get("partition")
    size = options.get("record_num")

    producer = Producer({'bootstrap.servers': server,
                         'socket.blocking.max.ms': 10})
    consumer = Consumer({'bootstrap.servers': server,
                         'socket.blocking.max.ms': 10,
                         'group.id': 'client_kafka_benchmark',
                         'default.topic.config': {'auto.offset.reset': 'largest'}})
    consumer.subscribe([topic])

    msg_generator = KafkaMsg(options)
    start_offset = 0

    # consumer offset to end
    while True:
        msg = consumer.poll()
        if msg.error():
            start_offset = msg.offset()
            break

    print("produce...., start offset: " + str(start_offset))
    i = 0
    while i < size:
        try:
            k, v = msg_generator.hook_get_key_and_value(i)
            producer.produce(topic, v)
        except:
            producer.flush()
        i += 1
    producer.flush()

    i = 0
    while i < size:
        k, v = msg_generator.hook_get_key_and_value(i)
        msg = consumer.poll()
        if msg.error():
            print("msg.error: %s, offset: %d" % (str(msg.error()), msg.offset()))
            # error KafkaError{code=_PARTITION_EOF,val=-191,str="Broker: No more messages"}
            # but this is more when use consumer.assign()
            break
        if msg.value().decode() != v:
            print("mismatch, index:%d, offset:%d" % (i, msg.offset()))
            print("mine:%s, theirs:%s" % (v, msg.value().decode()))
            break
        i += 1

    if i < size:  # error, print 10 records
        offset = start_offset + max(0, i - 5)
        # python3.5 Segmentation fault (core dumped) when destruct consumer
        consumer_temp = Consumer({'bootstrap.servers': server,
                                  'socket.blocking.max.ms': 10,
                                  'group.id': 'client_kafka_benchmark',
                                  'default.topic.config': {'auto.offset.reset': 'largest'}})
        consumer_temp.assign([TopicPartition(topic, partition, offset)])
        for i in range(10):
            msg = consumer_temp.poll(1)
            if msg is None:
                break
            print("--%d %s" % (msg.offset(), msg.value().decode()))


def kafka_server_config():
    """
    version = "kafka_2.11-0.10.2.1"
    broker.id=0
    delete.topic.enable=true
    listeners=PLAINTEXT://169.0.1.198:9092
    num.network.threads=3
    num.io.threads=8
    socket.send.buffer.bytes=102400
    socket.receive.buffer.bytes=102400
    socket.request.max.bytes=104857600
    log.dirs=/tmp/kafka-logs
    num.partitions=1
    num.recovery.threads.per.data.dir=1
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    zookeeper.connect=localhost:2181
    zookeeper.connection.timeout.ms=6000
    """
    pass

def my_test_data():
    """
    # python  confluent_kafka_test.py -h 169.0.1.198 -r 200000
    produce...., start offset: 0
    mismatch, index:100000, offset:100000
    mine:---------0, theirs:---------1
    --99995 ---------5
    --99996 ---------6
    --99997 ---------7
    --99998 ---------8
    --99999 ---------9
    --100000 ---------1
    --100001 ---------2
    --100002 ---------3
    --100003 ---------4
    --100004 ---------5
    # python  confluent_kafka_test.py -h 169.0.1.198 -r 200000
    produce...., start offset: 199999
    msg.error: KafkaError{code=_PARTITION_EOF,val=-191,str="Broker: No more messages"}, offset: 294618
    --294613 ---------4
    --294614 ---------5
    --294615 ---------6
    --294616 ---------7
    --294617 ---------8
    --294618 ---------9
    --294619 ---------0
    --294620 ---------1
    --294621 ---------2
    --294622 ---------3
    # python  confluent_kafka_test.py -h 169.0.1.198 -r 200000
    produce...., start offset: 399998
    msg.error: KafkaError{code=_PARTITION_EOF,val=-191,str="Broker: No more messages"}, offset: 494598
    --494593 ---------5
    --494594 ---------6
    --494595 ---------7
    --494596 ---------8
    --494597 ---------9
    --494598 ---------0
    --494599 ---------1
    --494600 ---------2
    --494601 ---------3
    --494602 ---------4
    # 
    """
    pass