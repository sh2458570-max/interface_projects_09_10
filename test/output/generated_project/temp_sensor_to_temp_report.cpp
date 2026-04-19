#include "temp_sensor_to_temp_report.h"
#include <algorithm>
#include <cmath>

namespace {
inline double clamp(double value, double low, double high)
{
    return std::max(low, std::min(value, high));
}
}

Temp_Report convert_temp_sensortotemp_report(const Temp_Sensor& temp_sensor)
{
    Temp_Report target;
    target.temperature_c = static_cast<decltype(target.temperature_c)>(temp_sensor.temperature);
    target.alarm = static_cast<decltype(target.alarm)>(0);
    return target;
}
