//#include "librdkafka/KafkaConfig.h"
#include "KafkaConfig.h"


namespace librdkafka
{

KafkaConfig::KafkaConfig()
{
}

KafkaConfig::~KafkaConfig()
{
}

bool KafkaConfig::ReadFromConfig(const std::string & configFilePath)
{
    std::string config = configFilePath;
    std::ifstream ifs(config);
	if (!ifs.good())
	{
        return false;
	}

    std::string line;
	while (getline(ifs, line))
	{
        std::istringstream iss(line);
        std::string key, value;
		iss >> key >> value;
		m_ConfigMap[key] = value;
	}
	ifs.close();
    return true;
}

}//end of namespace librdkafka
