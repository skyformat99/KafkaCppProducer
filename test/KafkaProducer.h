#ifndef __KAFKA_PRODUCER_H__
#define __KAFKA_PRODUCER_H__

#include "rdkafka.h"
//#include "gameplatform_types.h"
#include "KafkaDefine.h"
#include "CommonInclude.h"


namespace librdkafka
{

class KafkaProducer
{
public:
	KafkaProducer();
	~KafkaProducer();

    InitKafkaProducerStatType InitKafkaProducer(const std::string & kafkaTopicConfFilePath, const std::string & kafkaTopic);

	SendMsgStatType SendMessageToKafka(const char * kafkaMsg, int32_t msglen);
	SendMsgStatType SendMessageToKafka(const std::string & kafkaMsg);

    //用来将SendMessageToKafka失败的msg重发至kafak
    SendMsgStatType OnTick();

    //将队列中剩余msg发到kafka
    void WaitMsgDeliver();

private:
    bool InitKafkaMetadata();

    const std::string _GetRandomKeyStr();

private:
    //kafka
	rd_kafka_t *m_Rk;

    //topic
	rd_kafka_topic_t *m_Rkt;

    //metadata
	const rd_kafka_metadata_t *m_Metadata;

    //因外部队列达到界值而生产失败的msg
    std::list<std::string> m_FaildMsgs;
};

}// end of namespace librdkafka

#endif
