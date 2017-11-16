#include "KafkaDefine.h"


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


namespace librdkafka
{

UInt64 getCurrentTime()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec * 1000000 + tv.tv_usec);
}

void print_state()
{
	t_total = t_end - t_start;
	fprintf(stderr, "%% %d messages produced (%d bytes), %lld delivered (offset %d, %d failed) in %lld ms: %lld msgs/s and %.02f MB/s, %d produce failures\n",
			msgs, bytes,
			msgs_dr_ok, last_offset,
			msgs_dr_err, t_total / 1000,
			((msgs_dr_ok * 1000000) / t_total),
			(float)((bytes_dr_ok) / (float)t_total),
			tx_err);
}

}// end of namespace librdkafka

#endif
