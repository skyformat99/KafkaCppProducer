#include "KafkaHandle.h"
#include "KafkaConfig.h"


#ifdef TEST_PERFORMANCE

extern UInt64 t_end;
extern UInt64 msgs_dr_ok;
extern Int32 msgs_dr_err;
extern UInt64 bytes_dr_ok;
extern Int32 last_offset;

#endif

namespace librdkafka
{

KafkaHandle::KafkaHandle()
{
    m_Conf = rd_kafka_conf_new();
}

KafkaHandle::~KafkaHandle()
{
	rd_kafka_destroy(m_Rk);
}

KafkaHandleStatType KafkaHandle::Init(const std::string & kafkaConfigFilePath)
{
    m_ConfFilePath = kafkaConfigFilePath;

    KafkaHandleStatType kafkaHandleStatType;
    kafkaHandleStatType = _SetConfProperties();
    if(FAILED_TO_OPEN_CONFIG_FILE == kafkaHandleStatType)
    {
        return FAILED_TO_OPEN_CONFIG_FILE;
    }
    else if(UNKNOWN_CONFIG_KEY == kafkaHandleStatType)
    {
        return UNKNOWN_CONFIG_KEY;
    }
    else if(INVALID_CONFIG_VALUE == kafkaHandleStatType)
    {
        return INVALID_CONFIG_VALUE;
    }

    _SetCallback();

    kafkaHandleStatType = _CreateKafkaHandle();
    if(FAILED_TO_CREATE_KAFKA_HANDLE == kafkaHandleStatType)
    {
        return FAILED_TO_CREATE_KAFKA_HANDLE;
    }

    return SUCCESS_TO_INIT_KAFKA;
}

KafkaHandleStatType KafkaHandle::_SetConfProperties()
{
    if(!m_KafkaConfig.ReadFromConfig(m_ConfFilePath))
    {
        return FAILED_TO_OPEN_CONFIG_FILE;
    }

    const std::map<std::string, std::string> & configMap = m_KafkaConfig.GetConfigMap();
	for(auto & elem : configMap)
	{
    //    std::cout << "key = " << elem.first << " value = " << elem.second << std::endl;
		m_Res = rd_kafka_conf_set(m_Conf, elem.first.c_str(), elem.second.c_str(), m_Errstr, sizeof(m_Errstr));
		if(RD_KAFKA_CONF_UNKNOWN == m_Res)
		{
            return UNKNOWN_CONFIG_KEY;
		}
        else if(RD_KAFKA_CONF_INVALID == m_Res)
		{
            return INVALID_CONFIG_VALUE;
		}
	}
	return SUCCESS_TO_INIT_KAFKA;
}

void KafkaHandle::_SetCallback()
{
#if defined(TEST_POLL_STATE) || defined(TEST_PERFORMANCE)
	rd_kafka_conf_set_log_cb(m_Conf, _Logger);
	rd_kafka_conf_set_dr_msg_cb(m_Conf, _Msg_delivered);
#endif
}

KafkaHandleStatType KafkaHandle::_CreateKafkaHandle()
{
	if(!(m_Rk = rd_kafka_new(RD_KAFKA_PRODUCER, m_Conf, m_Errstr, sizeof(m_Errstr))))
	{
        return FAILED_TO_CREATE_KAFKA_HANDLE;
	}
    return SUCCESS_TO_INIT_KAFKA;
}

void KafkaHandle::_Logger(const rd_kafka_t *rk, int32_t level, const char *fac, const char *buf)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n", (int32_t)tv.tv_sec, (int32_t)(tv.tv_usec / 1000), level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

void KafkaHandle::_Msg_delivered(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	if(rkmessage->err)
	{
#ifdef TEST_PERFORMANCE
		++msgs_dr_err;
#endif
#ifdef TEST_POLL_STATE
		fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
#endif
	}
    else
	{
#ifdef TEST_PERFORMANCE
		++msgs_dr_ok;
		bytes_dr_ok += rkmessage->len;
#endif
#ifdef TEST_POLL_STATE
		fprintf(stderr, "%% Message delivery (%zd bytes, offset %ld, partition %d): %.*s\n",
				rkmessage->len, rkmessage->offset, rkmessage->partition, (int32_t)rkmessage->len, (const char*)rkmessage->payload);
#endif
	}
#ifdef TEST_PERFORMANCE
	last_offset = rkmessage->offset;
	t_end = getCurrentTime();
#endif
}

}// end of namespace librdkafka
