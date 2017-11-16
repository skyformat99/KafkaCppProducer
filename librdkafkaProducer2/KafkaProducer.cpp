//#include "librdkafka/KafkaProducer.h"
#include "KafkaProducer.h"
#include "KafkaHandle.h"
#include "KafkaTopicHandle.h"

#ifdef TEST_PERFORMANCE

extern Int32 tx_err;

#endif


namespace librdkafka
{

KafkaProducer::KafkaProducer()
{
	srand(time(NULL));
}

KafkaProducer::~KafkaProducer()
{
    rd_kafka_topic_destroy(m_Rkt);
    rd_kafka_metadata_destroy(m_Metadata);
	rd_kafka_destroy(m_Rk);
}

InitKafkaProducerStatType KafkaProducer::Init(const std::string& kafkaConfigFilePath, const std::string& kafkaTopicConfFilePath, const std::string& kafkaTopic)
{
    InitKafkaProducerStatType initType;

    KafkaHandle kafkaHandle;
    std::string configFilePath = kafkaConfigFilePath;
    initType = kafkaHandle.Init(configFilePath);
    switch(initType)
    {
        case librdkafka::FAILED_TO_OPEN_CONFIG_FILE: std::cout << "Failed to open config file" << std::endl; exit(0);break;
        case librdkafka::UNKNOWN_CONFIG_KEY: std::cout << "Unknown config key" << std::endl; exit(0);break;
        case librdkafka::INVALID_CONFIG_VALUE: std::cout << "Invalid config value" << std::endl; exit(0);break;
        case librdkafka::FAILED_TO_CREATE_KAFKA_HANDLE: std::cout << "Failed to create kafka handle" << std::endl; exit(0); break;
        default: m_Rk = kafkaHandle.GetKafkaHandle();break;
    }

    KafkaTopicHandle kafkaTopicHandle(m_Rk);
    std::string topicConfigFilePath = kafkaTopicConfFilePath;
    std::string topic = kafkaTopic;

    initType = kafkaTopicHandle.Init(topicConfigFilePath, topic);
    switch(initType)
    {
        case FAILED_TO_OPEN_TOPIC_CONFIG_FILE: return FAILED_TO_OPEN_TOPIC_CONFIG_FILE; break;
        case UNKNOWN_TOPIC_CONFIG_KEY: return UNKNOWN_TOPIC_CONFIG_KEY; break;
        case INVALID_TOPIC_CONFIG_VALUE: return INVALID_TOPIC_CONFIG_VALUE; break;
        case FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE: return FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE; break;
        default: m_Rkt = kafkaTopicHandle.GetKafkaTopicHandle(); break;
    }

    if(!InitKafkaMetadata())
    {
        return FAILED_TO_ACQUIRE_METADATA;
    }

    return SUCCESS_TO_INIT_PRODUCER;
}

bool KafkaProducer::InitKafkaMetadata()
{
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
	err = rd_kafka_metadata(m_Rk, 0, m_Rkt, &m_Metadata, 5000);
	if(err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
        return false;
	}
	return true;
}

SendMsgStatType KafkaProducer::SendMessageToKafka(const char* kafkaMsg, int32_t msglen)
{
    const char *msg = kafkaMsg;
	int32_t partition = RD_KAFKA_PARTITION_UA;

    const char *key = _GetRandomKeyStr().c_str();
    size_t keylen = strlen(key);

	//if(-1 == rd_kafka_produce(m_Rkt, partition, RD_KAFKA_MSG_F_COPY, (void*)msg, msglen, (const void*)key, keylen, NULL))
	if(-1 == rd_kafka_produce(m_Rkt, partition, 0, (void*)msg, msglen, (const void*)key, keylen, NULL))
	{
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        if(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION == err || RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC == err)
        {
            return UNKNOWN_TOPIC_OR_PARTITION;
        }
        else if(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE == err)//可在全局配置"messages.max.bytes"进行设置
        {
            return MSG_SIZE_TOO_LARGE;
        }
        else if(RD_KAFKA_RESP_ERR__QUEUE_FULL == err)//可在全局配置"queue.buffering.max.messages"进行设置
        {
            m_FaildMsgs.push_back(msg);
        }
#ifdef TEST_PERFORMANCE
            ++tx_err;
#endif
		rd_kafka_poll(m_Rk, 10);
	}

	rd_kafka_poll(m_Rk, 0);
    return SUCCESS_TO_SEND_MSG_TO_KAFKA;
}

SendMsgStatType KafkaProducer::SendMessageToKafka(const std::string& kafkaMsg)
{
    const std::string msg = kafkaMsg;
	int32_t partition = RD_KAFKA_PARTITION_UA;
	int32_t msglen = msg.size();

    const char *key = _GetRandomKeyStr().c_str();
    size_t keylen = strlen(key);

	if(-1 == rd_kafka_produce(m_Rkt, partition, RD_KAFKA_MSG_F_COPY, (void*)msg.c_str(), msglen, (const void*)key, keylen, NULL))
	{
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        if(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION == err || RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC == err)
        {
            return UNKNOWN_TOPIC_OR_PARTITION;
        }
        else if(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE == err)//可在全局配置"messages.max.bytes"进行设置
        {
            return MSG_SIZE_TOO_LARGE;
        }
        else if(RD_KAFKA_RESP_ERR__QUEUE_FULL == err)//可在全局配置"queue.buffering.max.messages"进行设置
        {
            m_FaildMsgs.push_back(msg);
        }
#ifdef TEST_PERFORMANCE
            ++tx_err;
#endif
		rd_kafka_poll(m_Rk, 10);
	}

	rd_kafka_poll(m_Rk, 0);
    return SUCCESS_TO_SEND_MSG_TO_KAFKA;
}

SendMsgStatType KafkaProducer::OnTick()
{
    SendMsgStatType sendMsgStatType;
    while(m_FaildMsgs.size() > 0)
    {
        for(auto iterator = m_FaildMsgs.begin(); iterator != m_FaildMsgs.end(); )
        {
            const std::string failedMsg = *iterator;
            iterator = m_FaildMsgs.erase(iterator);

            sendMsgStatType = SendMessageToKafka(failedMsg);
            switch(sendMsgStatType)
            {
                case UNKNOWN_TOPIC_OR_PARTITION: return UNKNOWN_TOPIC_OR_PARTITION; break;
                case MSG_SIZE_TOO_LARGE: return MSG_SIZE_TOO_LARGE; break;
                default: break;
            }
        }
        WaitMsgDeliver();
    }
    return SUCCESS_TO_SEND_MSG_TO_KAFKA;
}

void KafkaProducer::WaitMsgDeliver()
{
    while(rd_kafka_outq_len(m_Rk) > 0)
    {
        rd_kafka_poll(m_Rk, 1000);
    }
}

const std::string KafkaProducer::_GetRandomKeyStr()
{
    int32_t partitions = m_Metadata->topics->partition_cnt;
	int32_t idx= rand() % partitions;
    std::ostringstream oss;
    oss << idx;
    return oss.str();
}

}// end of namespace librdkafka
