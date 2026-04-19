#include "messageconvert.h"
#include "codec.h"
#include <QDateTime>
#include <QDebug>
#include <QMutexLocker>
#include <QtConcurrent>

messageConvert::messageConvert(QObject* parent)
    : QObject(parent)
{
}

int messageConvert::start(QVector<std::shared_ptr<NetInfo>> netlist, int maxThread)
{
    _maxThread = maxThread;
    udpSend.reset(new QUdpSocket());
    for (auto serv : netlist) {
        if (serv->bRecvTag == false) {
            udpSendList.push_back(serv);
        } else {
            std::shared_ptr<QUdpSocket> soc(new QUdpSocket);
            connect(soc.get(), &QUdpSocket::readyRead, [serv, soc, this]() {
                while (soc->hasPendingDatagrams()) {
                    QHostAddress sender;
                    quint16 senderPort = 0;
                    qint64 size = soc->pendingDatagramSize();
                    QByteArray buffer(size, 0);
                    soc->readDatagram(buffer.data(), size, &sender, &senderPort);
                    readPendingDatagrams(serv->name, sender, serv->feedBackPort, buffer);
                }
            });
            if (!soc->bind(QHostAddress::Any, serv->port)) return -1;
            udpRecvList.push_back(soc);
        }
    }
    QtConcurrent::run([this]() { this->msgConvertThread(); });
    return 0;
}

int messageConvert::stop()
{
    _threadExit = 1;
    for (auto var : udpRecvList) {
        if (var->isOpen()) var->close();
    }
    if (udpSend && udpSend->isOpen()) udpSend->close();
    udpRecvList.clear();
    return 0;
}

void messageConvert::onSendMessage(QByteArray msg)
{
    for (auto var : udpSendList) udpSend->writeDatagram(msg, QHostAddress(var->ip), var->port);
}

void messageConvert::readPendingDatagrams(QString name, QHostAddress ip, quint16 port, QByteArray data)
{
    std::shared_ptr<msgDataInfo> d(new msgDataInfo);
    d->time.append(QDateTime::currentMSecsSinceEpoch());
    d->name = name;
    d->num = 1;
    d->data = data;
    d->ip = ip.toString();
    d->port = port;
    pushData(d);
}

void messageConvert::pushData(std::shared_ptr<msgDataInfo> data)
{
    QMutexLocker lock(&dataMutex);
    for (int i = 0; i < dataInfo.size(); ++i) {
        if (data->name == dataInfo[i]->name) {
            if (data->data != dataInfo[i]->data) {
                dataInfo[i] = data;
                dataInfo[i]->time = data->time;
                dataInfo[i]->state = 0;
            } else {
                dataInfo[i]->num++;
                dataInfo[i]->state = 0;
            }
            return;
        }
    }
    dataInfo.push_back(data);
}

void messageConvert::getData(QString name, int time, int num, QByteArray& data, QString& ip, int& port, int& outTime)
{
    QMutexLocker lock(&dataMutex);
    for (auto item : dataInfo) {
        if (name == item->name && (num <= item->num) && item->state == 0) {
            for (int i = item->time.size() - 1; i >= 1; --i) {
                if (item->time[i] - item->time[i - 1] <= time) return;
            }
            ip = item->ip;
            port = item->port;
            data = item->data;
            outTime = static_cast<int>(item->time.first());
            item->state = 1;
            return;
        }
    }
}

void messageConvert::cacheGeneratedTarget(const QString& targetName, int num, const QByteArray& data)
{
    std::shared_ptr<msgDataInfo> d(new msgDataInfo);
    d->time.append(QDateTime::currentMSecsSinceEpoch());
    d->name = targetName;
    d->num = num;
    d->data = data;
    d->ip = QStringLiteral("127.0.0.1");
    d->port = 0;
    pushData(d);
}

void messageConvert::Temp_SensordataPro()
{
    QByteArray temp_sensorData;
    Temp_Sensor temp_sensor = {0};
    int temp_sensorFlag = 0;
    QString temp_sensorIp;
    int temp_sensorPort = 0;
    int temp_sensorTime = 0;
    int count_temp_sensor[1] = { 1 };
    int cycle_temp_sensor[1] = { 0 };
    int num_temp_sensor = 1;
    while (num_temp_sensor-- > 0) {
        getData(QStringLiteral("Temp_Sensor"), cycle_temp_sensor[num_temp_sensor], count_temp_sensor[num_temp_sensor], temp_sensorData, temp_sensorIp, temp_sensorPort, temp_sensorTime);
        if (temp_sensorData.isEmpty() == false) {
            QString ret = decodeMsg((uchar*)temp_sensorData.data(), temp_sensorData.size(), temp_sensor);
            if (ret.isEmpty() == false) {
                QByteArray sdata;
                int iret = checkObjMaps(ret, sdata, temp_sensor);
                if (iret == 0) temp_sensorFlag = 1;
            if (iret != -1 && temp_sensorPort > 0) {
                QUdpSocket soc;
                soc.writeDatagram(sdata, QHostAddress(temp_sensorIp), temp_sensorPort);
            }
            }
            break;
                }
            }
            if (1 != temp_sensorFlag) return;
    Temp_Report target = convert_temp_sensortotemp_report(temp_sensor);
    QByteArray sendData;
    encodeMsg(sendData, target);
    onSendMessage(sendData);
}

void messageConvert::msgConvertThread()
{
    while (0 == _threadExit) {
        Temp_SensordataPro();
        _sleep(2);
    }
}
