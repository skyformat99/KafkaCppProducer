#include "KafkaTopicHandle.h"
#include "KafkaHandle.h"


namespace librdkafka
{

KafkaTopicHandle::KafkaTopicHandle(rd_kafka_t* rk)
{
    m_Rk = rk;
    m_Topic_conf = rd_kafka_topic_conf_new();
}

KafkaTopicHandle::~KafkaTopicHandle()
{
}

InitKafkaProducerStatType KafkaTopicHandle::Init(const std::string& kafkaTopicConfigFilePath, const std::string& kafkaTopic)
{
    m_ConfFilePath = kafkaTopicConfigFilePath;
    m_Topic = kafkaTopic;

    InitKafkaProducerStatType initType;
    initType = _SetConfProperties();
    if(FAILED_TO_OPEN_TOPIC_CONFIG_FILE == initType)
    {
        return FAILED_TO_OPEN_TOPIC_CONFIG_FILE;
    }
    else if(UNKNOWN_TOPIC_CONFIG_KEY == initType)
    {
        return UNKNOWN_TOPIC_CONFIG_KEY;
    }
    else if(INVALID_TOPIC_CONFIG_VALUE == initType)
    {
        return INVALID_TOPIC_CONFIG_VALUE;
    }

    _SetCallback();

    initType = _CreateKafkaTopicHandle();
    if(FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE == initType)
    {
        return FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE;
    }

    return SUCCESS_TO_INIT_PRODUCER;
}

InitKafkaProducerStatType KafkaTopicHandle::_SetConfProperties()
{
    if(!m_ConfFilePath.empty())
    {
        if(!m_KafkaConfig.ReadFromConfig(m_ConfFilePath))
        {
            return FAILED_TO_OPEN_TOPIC_CONFIG_FILE;
        }

	    const std::map<std::string, std::string> & configMap = m_KafkaConfig.GetConfigMap();
	    for(auto & elem : configMap)
	    {
	    	m_Res = rd_kafka_topic_conf_set(m_Topic_conf, elem.first.c_str(), elem.second.c_str(), m_Errstr, sizeof(m_Errstr));
	    	if(RD_KAFKA_CONF_UNKNOWN == m_Res)
	    	{
                return UNKNOWN_TOPIC_CONFIG_KEY;
	    	}
            else if(RD_KAFKA_CONF_INVALID == m_Res)
	    	{
                return INVALID_TOPIC_CONFIG_VALUE;
	    	}
	    }
    }
	return SUCCESS_TO_INIT_PRODUCER;
}

void KafkaTopicHandle::_SetCallback()
{
	rd_kafka_topic_conf_set_partitioner_cb(m_Topic_conf, _Partitioner);
}

InitKafkaProducerStatType KafkaTopicHandle::_CreateKafkaTopicHandle()
{
	if(!(m_Rkt = rd_kafka_topic_new(m_Rk, m_Topic.c_str(), m_Topic_conf)))
	{
        return FAILED_TO_CREATE_KAFKA_TOPIC_HANDLE;
	}
	m_Topic_conf = NULL;	//避免再被使用

	return SUCCESS_TO_INIT_PRODUCER;
}

int32_t KafkaTopicHandle::_Partitioner(const rd_kafka_topic_t *rkt, const void *key, size_t keylen, int32_t partition_cnt, void *rkt_opaque, void *msg_opaque)
{
#ifdef TEST_POLL_STATE
    std::cout << "%% _Partitioner key = " << *(const char*)key << " keylen = " << keylen << " partCnt = " << partition_cnt << std::endl;
#endif
	return _DJBHash((const char*)key, keylen) % partition_cnt;
}

uint32_t KafkaTopicHandle::_DJBHash(const char *str, size_t len)
{
	uint32_t hash = 5381;
	for(size_t index = 0; index < len; ++index)
	{
		hash += (hash << 5) + str[index];
	}
	return hash;
}

}// end of namespace librdkafka
