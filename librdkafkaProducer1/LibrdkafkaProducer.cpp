 ///
 /// @file    LibrdkafkaProducer.cpp
 /// @author  yangwenhao
 /// @date    2017-07-04 02:22:44
 ///

//#include "librdkafka/LibrdkafkaProducer.h"
#include "LibrdkafkaProducer.h"

#include <string.h>
#include <sys/time.h>
#include <sstream>
#include <iostream>


#ifdef TEST_PERFORMANCE

UInt64 t_start = 0;
UInt64 t_end = 0;
UInt64 t_total = 0;

Int32 msgs = 0;
Int32 bytes = 0;
UInt64 msgs_dr_ok = 0;
Int32 msgs_dr_err = 0;
UInt64 bytes_dr_ok = 0;
Int32 last_offset = 0;
Int32 tx_err = 0;

#endif


namespace librdkafka
{

LibrdkafkaProducer::LibrdkafkaProducer(const std::string & confFilePath, const std::string & topicConfFilePath, const std::string & topic)
: m_ConfFilePath(confFilePath)
, m_TopicConfFilePath(topicConfFilePath)
, m_Topic(topic)
, m_Conf(rd_kafka_conf_new())
, m_Topic_conf(rd_kafka_topic_conf_new())
{
}

LibrdkafkaProducer::~LibrdkafkaProducer()
{
	_Destroy();
}

SendMsgStatType LibrdkafkaProducer::SendMessageToKafka(const char * msg, int32_t msglen)
{
	int32_t partition = RD_KAFKA_PARTITION_UA;

    const char *key = _GetRandomKeyStr().c_str();
    size_t keylen = strlen(key);

	if(-1 == rd_kafka_produce(m_Rkt, partition, RD_KAFKA_MSG_F_COPY, (void*)msg, msglen, (const void*)key, keylen, NULL))
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

SendMsgStatType LibrdkafkaProducer::SendMessageToKafka(const std::string & msg)
{
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

SendMsgStatType LibrdkafkaProducer::OnTick()
{
    SendMsgStatType sendMsgStatType;
    while(m_FaildMsgs.size() > 0)
    {
#ifdef TEST_PERFORMANCE
        fprintf(stderr, "OnTick\n");
#endif
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

void LibrdkafkaProducer::WaitMsgDeliver()
{
    while(rd_kafka_outq_len(m_Rk) > 0)
    {
        rd_kafka_poll(m_Rk, 1000);
    }
}

void LibrdkafkaProducer::_Destroy()
{
	rd_kafka_metadata_destroy(m_Metadata);
	rd_kafka_topic_destroy(m_Rkt);
	rd_kafka_destroy(m_Rk);
}

InitKafkaStatType LibrdkafkaProducer::InitProducer()
{
    InitKafkaStatType initKafkaStatType;

    if(!m_ConfFilePath.empty())
    {
        initKafkaStatType = _ConfPropertiesSet();
	    if(FAILED_TO_READ_CONFIGFILE == initKafkaStatType)
	    {
            return FAILED_TO_READ_CONFIGFILE;
	    }
        else if(UNKNOWN_CONFIG_NAME == initKafkaStatType)
        {
            return UNKNOWN_CONFIG_NAME;
        }
        else if(INVALID_CONFIG_VALUE == initKafkaStatType)
        {
            return INVALID_CONFIG_VALUE;
        }
    }

    if(!m_TopicConfFilePath.empty())
    {
        initKafkaStatType = _TopicConfPropertiesSet();
        if(FAILED_TO_READ_TOPIC_CONFIGFILE == initKafkaStatType)
        {
            return FAILED_TO_READ_TOPIC_CONFIGFILE;
        }
        else if(UNKNOWN_TOPIC_CONFIG_NAME == initKafkaStatType)
        {
            return UNKNOWN_TOPIC_CONFIG_NAME;
        }
        else if(INVALID_TOPIC_CONFIG_VALUE == initKafkaStatType)
        {
            return INVALID_TOPIC_CONFIG_VALUE;
        }
    }

    _CallbackSet();

    initKafkaStatType = _RdkafkaObjectCreate();
    if(FAILED_TO_CREATE_PRODUCE == initKafkaStatType)
    {
        return FAILED_TO_CREATE_PRODUCE;
    }
    else if(FAILED_TO_CREATE_TOPIC == initKafkaStatType)
    {
        return FAILED_TO_CREATE_TOPIC;
    }

    initKafkaStatType = _MetadataInit();
    if(FAILED_TO_ACQUIRE_METADATA == initKafkaStatType)
    {
        return FAILED_TO_ACQUIRE_METADATA;
    }

    return INIT_SUCCESS;
}

InitKafkaStatType LibrdkafkaProducer::_ConfPropertiesSet()
{
#if 0
	_MemsetErrstrZero();
	char signalChar[16] = {0};
	snprintf(signalChar, sizeof(signalChar), "%i", SIGIO);
	m_Res = rd_kafka_conf_set(m_Conf, "internal.termination.signal", signalChar, m_Errstr, sizeof(m_Errstr));
	if(RD_KAFKA_CONF_UNKNOWN == m_Res)
	{
		fprintf(stderr, "%% Unkonwn configuration name: %s\n", m_Errstr);
		exit(EXIT_FAILURE);
	}else if(RD_KAFKA_CONF_INVALID == m_Res)
	{
		fprintf(stderr, "%% Invalid configuration value: %s\n", m_Errstr);
		exit(EXIT_FAILURE);
	}
#endif

    if(!m_LibrdkafkaConfig.ReadFromConfig(m_ConfFilePath))
    {
        return FAILED_TO_READ_CONFIGFILE;
    }

    const std::map<std::string, std::string> & configMap = m_LibrdkafkaConfig.GetConfigMap();
	for(auto & elem : configMap)
	{
	    _MemsetErrstrZero();

		m_Res = rd_kafka_conf_set(m_Conf, elem.first.c_str(), elem.second.c_str(), m_Errstr, sizeof(m_Errstr));
		if(RD_KAFKA_CONF_UNKNOWN == m_Res)
		{
            return UNKNOWN_CONFIG_NAME;
		}
        else if(RD_KAFKA_CONF_INVALID == m_Res)
		{
            return INVALID_CONFIG_VALUE;
		}
	}
	return INIT_SUCCESS;
}

InitKafkaStatType LibrdkafkaProducer::_TopicConfPropertiesSet()
{
    if(!m_LibrdkafkaConfig.ReadFromConfig(m_TopicConfFilePath))
    {
        return FAILED_TO_READ_TOPIC_CONFIGFILE;
    }

	const std::map<std::string, std::string> & configMap = m_LibrdkafkaConfig.GetConfigMap();
	for(auto & elem : configMap)
	{
		_MemsetErrstrZero();

		m_Res = rd_kafka_topic_conf_set(m_Topic_conf, elem.first.c_str(), elem.second.c_str(), m_Errstr, sizeof(m_Errstr));
		if(RD_KAFKA_CONF_UNKNOWN == m_Res)
		{
            return UNKNOWN_TOPIC_CONFIG_NAME;
		}
        else if(RD_KAFKA_CONF_INVALID == m_Res)
		{
            return INVALID_TOPIC_CONFIG_VALUE;
		}
	}
	return INIT_SUCCESS;
}

void LibrdkafkaProducer::_CallbackSet()
{
#if defined(TEST_POLL_STATE) || defined(TEST_PERFORMANCE)
	rd_kafka_conf_set_log_cb(m_Conf, _Logger);
	rd_kafka_conf_set_dr_msg_cb(m_Conf, _Msg_delivered);
#endif

	rd_kafka_topic_conf_set_partitioner_cb(m_Topic_conf, _Partitioner);
}

InitKafkaStatType LibrdkafkaProducer::_RdkafkaObjectCreate()
{
	_MemsetErrstrZero();
	if(!(m_Rk = rd_kafka_new(RD_KAFKA_PRODUCER, m_Conf, m_Errstr, sizeof(m_Errstr))))
	{
        return FAILED_TO_CREATE_PRODUCE;
	}

	_MemsetErrstrZero();
	if(!(m_Rkt = rd_kafka_topic_new(m_Rk, m_Topic.c_str(), m_Topic_conf)))
	{
        return FAILED_TO_CREATE_TOPIC;
	}
	m_Topic_conf = NULL;	//避免再被使用

	return INIT_SUCCESS;
}

InitKafkaStatType LibrdkafkaProducer::_MetadataInit()
{
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
	err = rd_kafka_metadata(m_Rk, 0, m_Rkt, &m_Metadata, 5000);
	if(err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		//fprintf(stderr, "%% Failed to acquire metadata: %s\n", rd_kafka_err2str(err));
        return FAILED_TO_ACQUIRE_METADATA;
	}
	return INIT_SUCCESS;
}

void LibrdkafkaProducer::_Logger(const rd_kafka_t *rk, int32_t level, const char *fac, const char *buf)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n", (int32_t)tv.tv_sec, (int32_t)(tv.tv_usec / 1000), level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

void LibrdkafkaProducer::_Msg_delivered(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
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
	t_end = GetCurrentTime();
#endif
}

const std::string LibrdkafkaProducer::_GetRandomKeyStr()
{
	srand(time(NULL));
    int32_t partitions = m_Metadata->topics->partition_cnt;
	int32_t idx= rand() % partitions;
    std::ostringstream oss;
    oss << idx;
    return oss.str();
}

int32_t LibrdkafkaProducer::_Partitioner(const rd_kafka_topic_t *rkt, const void *key, size_t keylen, int32_t partition_cnt, void *rkt_opaque, void *msg_opaque)
{
#ifdef TEST_POLL_STATE
	fprintf(stderr, "%% _Partitioner key = %s keylen = %d partCnt = %d\n", (const char*)key, (int32_t)keylen, partition_cnt);
#endif
	return _DJBHash((const char*)key, keylen) % partition_cnt;
}

uint32_t LibrdkafkaProducer::_DJBHash(const char *str, size_t len)
{
	uint32_t hash = 5381;
	for(size_t index = 0; index < len; ++index)
	{
		hash += (hash << 5) + str[index];
	}
	return hash;
}

void LibrdkafkaProducer::_MemsetErrstrZero()
{
	memset(m_Errstr, 0, sizeof(m_Errstr));
}

#ifdef TEST_PERFORMANCE
int32_t LibrdkafkaProducer::GetCurrentTime()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec * 1000000 + tv.tv_usec);
}

void LibrdkafkaProducer::Print_state()
{
	t_total = t_end - t_start;
	fprintf(stderr, "%% %d messages produced (%d bytes), %lld delivered (offset %d, %d failed) in %lld ms: %lld msgs/s and %.02f MB/s, %d produce failures, %i in queue\n",
			msgs, bytes,
			msgs_dr_ok, last_offset,
			msgs_dr_err, t_total / 1000,
			((msgs_dr_ok * 1000000) / t_total),
			(float)((bytes_dr_ok) / (float)t_total),
			tx_err, rd_kafka_outq_len(m_Rk));
}
#endif

}// end of namespace librdkafka
