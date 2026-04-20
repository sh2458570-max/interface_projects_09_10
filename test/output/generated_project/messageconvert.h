#ifndef MESSAGECONVERT_H
#define MESSAGECONVERT_H

#include <QObject>
#include <QHostAddress>
#include <QMutex>
#include <QStringList>
#include <QTimer>
#include <QUdpSocket>
#include <QVector>
#include <memory>

class messageConvert : public QObject
{
    Q_OBJECT
public:
    explicit messageConvert(QObject* parent = nullptr);
    enum NetType { emTCP, emUDP, emDDS };
    class NetInfo { public: QString name; QString ip; int port = 0; quint16 feedBackPort = 0; int netType = emUDP; bool bRecvTag = true; };
    class msgDataInfo { public: QByteArray data; QVector<qulonglong> time; QString name; QString ip; quint16 port = 0; int state = 0; int num = 0; };

signals:
    void showMessage(QString msg);

public slots:
    void readPendingDatagrams(QString name, QHostAddress ip, quint16 port, QByteArray data);
private:
    int _maxThread = 5;
    int _threadExit = 0;
    std::shared_ptr<QUdpSocket> udpSend;
    QVector<std::shared_ptr<NetInfo>> udpSendList;
    QVector<std::shared_ptr<QUdpSocket>> udpRecvList;
    QVector<std::shared_ptr<msgDataInfo>> dataInfo;
    QMutex dataMutex;
    void pushData(std::shared_ptr<msgDataInfo> data);
    void getData(QString name, int time, int num, QByteArray& data, QString& ip, int& port, int& outTime);
    void msgConvertThread();
    void onSendMessage(QByteArray msg);
    void cacheGeneratedTarget(const QString& targetName, int num, const QByteArray& data);
    void Temp_SensordataPro();

public:
    int start(QVector<std::shared_ptr<NetInfo>> netlist, int maxThread = 5);
    int stop();
};

#endif
