#ifndef _KAFKA_HANDLE_H_
#define _KAFKA_HANDLE_H_

#include "CommonHandle.h"
#include "CommonSingleton.h"
#include "KafkaDefine.h"


namespace librdkafka
{

class KafkaHandle : public CommonHandle, public Singleton<KafkaHandle>
{
public:
    KafkaHandle();
    ~KafkaHandle();

    KafkaHandleStatType Init(const std::string & kafkaCongigFilePath);

    rd_kafka_t* GetKafkaHandle() {  return m_Rk;  }

private:
    //设置全局配置属性
	KafkaHandleStatType _SetConfProperties();

    void _SetCallback();

    //创建kafka对象
	KafkaHandleStatType _CreateKafkaHandle();

private:
    //kafka回调函数
	static void _Logger(const rd_kafka_t *m_Rk, int32_t level, const char *fac, const char *buf);
	static void _Msg_delivered(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

private:
    //设置全局配置属性集
	rd_kafka_conf_t *m_Conf;

    //kafka对象
	rd_kafka_t *m_Rk;
};

}// end of namespace librdkafka


#endif
