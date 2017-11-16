#ifndef _KAFKA_CONFIG_H_
#define _KAFKA_CONFIG_H_

#include "CommonInclude.h"


namespace librdkafka
{

class KafkaConfig
{
public:
	KafkaConfig();
	~KafkaConfig();

	bool ReadFromConfig(const std::string & configFilePath);

    const std::map<std::string, std::string> & GetConfigMap() const
    {   return m_ConfigMap;   }

private:
    std::map<std::string, std::string> m_ConfigMap;
};

}//end of namespace librdkafka

#endif
