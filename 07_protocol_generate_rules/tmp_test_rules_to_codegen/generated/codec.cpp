#include "codec.h"
#include <QStringList>
#include <QtGlobal>

namespace {
quint64 readBits(const QByteArray& data, int& bitOffset, int bitLength)
{
    quint64 value = 0;
    for (int index = 0; index < bitLength; ++index) {
        const int absoluteBit = bitOffset + index;
        const int byteIndex = absoluteBit / 8;
        const int bitIndex = 7 - (absoluteBit % 8);
        if (byteIndex >= data.size()) return value;
        const quint8 byteValue = static_cast<quint8>(data.at(byteIndex));
        value = (value << 1) | ((byteValue >> bitIndex) & 0x01);
    }
    bitOffset += bitLength;
    return value;
}

quint64 readBitsLE(const QByteArray& data, int& bitOffset, int bitLength)
{
    quint64 value = 0;
    for (int index = 0; index < bitLength; ++index) {
        const quint64 bitValue = readBits(data, bitOffset, 1);
        value |= (bitValue << index);
    }
    return value;
}

void appendBits(QByteArray& data, quint64 value, int bitLength)
{
    const int startBit = data.size() * 8;
    const int totalBits = startBit + bitLength;
    const int requiredBytes = (totalBits + 7) / 8;
    if (data.size() < requiredBytes) data.append(QByteArray(requiredBytes - data.size(), '\0'));
    for (int index = 0; index < bitLength; ++index) {
        const int absoluteBit = startBit + index;
        const int byteIndex = absoluteBit / 8;
        const int bitIndex = 7 - (absoluteBit % 8);
        const quint64 bitValue = (value >> (bitLength - index - 1)) & 0x01ULL;
        char byteValue = data[byteIndex];
        if (bitValue != 0) byteValue = static_cast<char>(byteValue | (1 << bitIndex));
        else byteValue = static_cast<char>(byteValue & ~(1 << bitIndex));
        data[byteIndex] = byteValue;
    }
}

void appendBitsLE(QByteArray& data, quint64 value, int bitLength)
{
    for (int index = 0; index < bitLength; ++index) appendBits(data, (value >> index) & 0x01ULL, 1);
}
}  // namespace

static QString checkTemp_ReportSeqNum(const QString& seqNum)
{
    return seqNum.isEmpty() ? QStringLiteral("Seq_1") : seqNum;
}

static QString VerifyTemp_ReportSeq(Temp_Report& value, const QString& seq)
{
    Q_UNUSED(value);
    return seq.isEmpty() ? QStringLiteral("Seq_1") : seq;
}


static void readOrigin(Temp_Report& value, const QByteArray& raw, int len, int& bitOffset)
{
    if (bitOffset + 10 > len * 8) return;
    value.temperature_c = static_cast<long>(readBits(raw, bitOffset, 10));
    if (bitOffset + 6 > len * 8) return;
    value.alarm = static_cast<long>(readBits(raw, bitOffset, 6));
}

static void writeOrigin(Temp_Report& value, QByteArray& data)
{
    appendBits(data, static_cast<quint64>(value.temperature_c), 10);
    appendBits(data, static_cast<quint64>(value.alarm), 6);
}

static void writeSeq_1(Temp_Report& value, QByteArray& data)
{
    data.clear();
    writeOrigin(value, data);
}

int checkObjMaps(QString strVerify, QByteArray& data, Temp_Report& value)
{
    const QString seq = checkTemp_ReportSeqNum(strVerify);
    if (seq.isEmpty()) {
        data.clear();
        return -1;
    }
    if (VerifyTemp_ReportSeq(value, seq).isEmpty()) {
        data.clear();
        return -1;
    }
    if (seq == QStringLiteral("Seq_1")) { writeSeq_1(value, data); return 0; }
    data.clear();
    return -1;
}

QString decodeMsg(uchar* pData, int len, Temp_Report& value)
{
    QByteArray raw(reinterpret_cast<const char*>(pData), len);
    int bitOffset = 0;
    readOrigin(value, raw, len, bitOffset);
    return VerifyTemp_ReportSeq(value, QStringLiteral("Seq_1"));
}

static QString checkEncodeSeqNumber(Temp_Report& value)
{

    return QStringLiteral("Seq_1");
}

static void VerifyField(Temp_Report& value)
{
    Q_UNUSED(value);
}

static void updateFieldValue(Temp_Report& value)
{
    Q_UNUSED(value);
}

static void updateGroupFlag(Temp_Report& value)
{
    Q_UNUSED(value);
}

void encodeMsg(QByteArray& data, Temp_Report& value)
{
    const QString seq = checkEncodeSeqNumber(value);
    VerifyField(value);
    updateFieldValue(value);
    updateGroupFlag(value);
    if (seq == QStringLiteral("Seq_1")) { writeSeq_1(value, data); return; }
    data.clear();
    writeSeq_1(value, data);
}

static QString checkTemp_SensorSeqNum(const QString& seqNum)
{
    return seqNum.isEmpty() ? QStringLiteral("Seq_1") : seqNum;
}

static QString VerifyTemp_SensorSeq(Temp_Sensor& value, const QString& seq)
{
    Q_UNUSED(value);
    return seq.isEmpty() ? QStringLiteral("Seq_1") : seq;
}


static void readOrigin(Temp_Sensor& value, const QByteArray& raw, int len, int& bitOffset)
{
    if (bitOffset + 12 > len * 8) return;
    value.temperature = static_cast<long>(readBits(raw, bitOffset, 12));
    if (bitOffset + 4 > len * 8) return;
    value.status = static_cast<long>(readBits(raw, bitOffset, 4));
}

static void writeOrigin(Temp_Sensor& value, QByteArray& data)
{
    appendBits(data, static_cast<quint64>(value.temperature), 12);
    appendBits(data, static_cast<quint64>(value.status), 4);
}

static void writeSeq_1(Temp_Sensor& value, QByteArray& data)
{
    data.clear();
    writeOrigin(value, data);
}

int checkObjMaps(QString strVerify, QByteArray& data, Temp_Sensor& value)
{
    const QString seq = checkTemp_SensorSeqNum(strVerify);
    if (seq.isEmpty()) {
        data.clear();
        return -1;
    }
    if (VerifyTemp_SensorSeq(value, seq).isEmpty()) {
        data.clear();
        return -1;
    }
    if (seq == QStringLiteral("Seq_1")) { writeSeq_1(value, data); return 0; }
    data.clear();
    return -1;
}

QString decodeMsg(uchar* pData, int len, Temp_Sensor& value)
{
    QByteArray raw(reinterpret_cast<const char*>(pData), len);
    int bitOffset = 0;
    readOrigin(value, raw, len, bitOffset);
    return VerifyTemp_SensorSeq(value, QStringLiteral("Seq_1"));
}

static QString checkEncodeSeqNumber(Temp_Sensor& value)
{

    return QStringLiteral("Seq_1");
}

static void VerifyField(Temp_Sensor& value)
{
    Q_UNUSED(value);
}

static void updateFieldValue(Temp_Sensor& value)
{
    Q_UNUSED(value);
}

static void updateGroupFlag(Temp_Sensor& value)
{
    Q_UNUSED(value);
}

void encodeMsg(QByteArray& data, Temp_Sensor& value)
{
    const QString seq = checkEncodeSeqNumber(value);
    VerifyField(value);
    updateFieldValue(value);
    updateGroupFlag(value);
    if (seq == QStringLiteral("Seq_1")) { writeSeq_1(value, data); return; }
    data.clear();
    writeSeq_1(value, data);
}
