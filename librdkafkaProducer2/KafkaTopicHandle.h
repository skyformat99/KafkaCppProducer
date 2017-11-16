#ifndef _KAFKA_TOPIC_HANDLE_H_
#define _KAFKA_TOPIC_HANDLE_H_

#include "KafkaDefine.h"
#include "CommonHandle.h"


namespace librdkafka
{

class KafkaTopicHandle : public CommonHandle
{
public:
    KafkaTopicHandle(rd_kafka_t* rk);
    ~KafkaTopicHandle();

    InitKafkaProducerStatType Init(const std::string& kafkaTopicConfigFilePath, const std::string& kafkaTopic);
    rd_kafka_topic_t* GetKafkaTopicHandle() {   return m_Rkt;   }

private:
    //设置topic配置属性
	InitKafkaProducerStatType _SetConfProperties();

    void _SetCallback();

	//自定义分区
	static int32_t _Partitioner(const rd_kafka_topic_t *rkt, const void *key, size_t leylen, int32_t partition_cnt, void *rkt_opaque, void *msg_opaque);
	static uint32_t _DJBHash(const char *str, size_t len);

    //创建kafkaTopic对象
	InitKafkaProducerStatType _CreateKafkaTopicHandle();

private:
    //设置topic配置属性集
	rd_kafka_topic_conf_t* m_Topic_conf;

    rd_kafka_t* m_Rk;

    //topic对象
	rd_kafka_topic_t* m_Rkt;

    //topic
    std::string m_Topic;
};

}// end of namespace librdkafka

#endif
