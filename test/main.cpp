#include "KafkaDefine.h"
#include "KafkaHandle.h"
#include "KafkaProducer.h"


#ifdef TEST_PERFORMANCE

extern UInt64 t_start;

extern Int32 msgs;
extern Int32 bytes;

#endif

int main(int32_t argc, char* argv[])
{
#ifdef TEST_PERFORMANCE
	if(argc != 3)
	{
        std::cout << "error args! format is: msgcnt[100], msgsize[500000]" << std::endl;
		return 0;
	}
	int msgsize = atoi(argv[1]);
	int msgcnt = atoi(argv[2]);
    std::string msgpattern = "this is a message";
	int32_t len = msgpattern.size();

	char *sbuf = (char*)malloc(msgsize);
	int32_t ret = 0;
	while(ret < msgsize)
	{
		memcpy(sbuf+ret, msgpattern.c_str(), len);
		ret += len;
	}
#endif

    librdkafka::KafkaHandleStatType kafkaHandleStatType;
    kafkaHandleStatType = librdkafka::KafkaHandle::Instance()->Init("./rdkafkaConfSetFile.conf");
    switch(kafkaHandleStatType)
    {
        case librdkafka::FAILED_TO_OPEN_CONFIG_FILE: std::cout << "Failed to open config file" << std::endl; exit(0);break;
        case librdkafka::UNKNOWN_CONFIG_KEY: std::cout << "Unknown config key" << std::endl; exit(0);break;
        case librdkafka::INVALID_CONFIG_VALUE: std::cout << "Invalid config value" << std::endl; exit(0);break;
        case librdkafka::FAILED_TO_CREATE_KAFKA_HANDLE: std::cout << "Failed to create kafka handle" << std::endl; exit(0); break;
        default: std::cout << "Success to init kafkaHandle" << std::endl;break;
    }

    librdkafka::InitKafkaProducerStatType initKafkaTopicStatType;
    librdkafka::KafkaProducer producer;
    initKafkaTopicStatType = producer.InitKafkaProducer("", "test");
    switch (initKafkaTopicStatType)
    {
        case librdkafka::FAILED_TO_OPEN_TOPIC_CONFIG_FILE: std::cout << "Failed to open topic config file" << std::endl; exit(0);break;
        case librdkafka::UNKNOWN_TOPIC_CONFIG_KEY: std::cout << "Unknown topic config key" << std::endl; exit(0); break;
        case librdkafka::INVALID_TOPIC_CONFIG_VALUE: std::cout << "Invalid topic config name" << std::endl; exit(0); break;
        case librdkafka::FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE: std::cout << "Failed to create new topic" << std::endl; exit(0); break;
        default: std::cout << "Success to init kafkaProducer" << std::endl;break;
    }

#ifdef TEST_PERFORMANCE
    librdkafka::SendMsgStatType sendMsgStatType;
	t_start = librdkafka::getCurrentTime();
	while(true && (msgcnt == -1 || msgs < msgcnt))
	{
        std::string msg = sbuf;
        sendMsgStatType = producer.SendMessageToKafka(msg);
        switch(sendMsgStatType)
        {
            case librdkafka::UNKNOWN_TOPIC_OR_PARTITION: std::cout << "Topic or partition is error" << std::endl; exit(0); break;
            case librdkafka::MSG_SIZE_TOO_LARGE: std::cout << "Msg is larger than configured max size" << std::endl; exit(0); break;
            default: break;
        }

		++msgs;
		bytes += msgsize;
	}
    producer.WaitMsgDeliver();
    librdkafka::print_state();

    sendMsgStatType = producer.OnTick();
    switch(sendMsgStatType)
    {
        case librdkafka::UNKNOWN_TOPIC_OR_PARTITION: std::cout << "Topic or partition is error" << std::endl; exit(0); break;
        case librdkafka::MSG_SIZE_TOO_LARGE: std::cout << "Msg is larger than configured max size" << std::endl; exit(0); break;
        default: std::cout << "All msg delivered" << std::endl; break;
    }
    librdkafka::print_state();

#endif

#ifdef NORMAL_STATE
    librdkafka::SendMsgStatType sendMsgStatType;
    std::string msg;
	while(getline(std::cin, msg))
	{
        sendMsgStatType = producer.SendMessageToKafka(msg);
        switch(sendMsgStatType)
        {
            case librdkafka::UNKNOWN_TOPIC_OR_PARTITION: std::cout << "Topic or partition is error" << std::endl; exit(0); break;
            case librdkafka::MSG_SIZE_TOO_LARGE: std::cout << "Msg is larger than configured max size" << std::endl; exit(0); break;
            default: break;
        }
        producer.WaitMsgDeliver();
	}
#endif

    return 0;
}
