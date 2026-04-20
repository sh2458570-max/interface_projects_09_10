#ifndef CODEC_H
#define CODEC_H

#include "temp_sensor_to_temp_report.h"
#include <QByteArray>
#include <QString>

QString decodeMsg(uchar* pData, int len, Temp_Report& value);
void encodeMsg(QByteArray& data, Temp_Report& value);
int checkObjMaps(QString strVerify, QByteArray& data, Temp_Report& value);

QString decodeMsg(uchar* pData, int len, Temp_Sensor& value);
void encodeMsg(QByteArray& data, Temp_Sensor& value);
int checkObjMaps(QString strVerify, QByteArray& data, Temp_Sensor& value);

#endif
