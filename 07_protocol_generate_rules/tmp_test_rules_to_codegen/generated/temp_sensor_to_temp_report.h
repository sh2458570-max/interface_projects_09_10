#ifndef TEMP_SENSOR_TO_TEMP_REPORT_H
#define TEMP_SENSOR_TO_TEMP_REPORT_H

#include "temp_sensor_def.h"
#include "temp_report_def.h"

Temp_Report convert_temp_sensortotemp_report(const Temp_Sensor& temp_sensor);

#endif
