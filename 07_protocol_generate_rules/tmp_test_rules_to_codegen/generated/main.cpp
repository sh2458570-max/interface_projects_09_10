#include <QCoreApplication>
#include <QDebug>
#include <QDomDocument>
#include <QFile>
#include <memory>
#include "messageconvert.h"

int readMessageXML(QString path, QVector<std::shared_ptr<messageConvert::NetInfo>>& netlist)
{
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "Cannot open file for reading:" << qPrintable(file.errorString());
        return 1;
    }
    QDomDocument doc;
    if (!doc.setContent(&file)) {
        qDebug() << "Failed to load document";
        file.close();
        return 2;
    }
    file.close();
    QDomElement root = doc.documentElement();
    QDomNodeList childNodes = root.childNodes();
    for (int index = 0; index < childNodes.count(); ++index) {
        QDomNode node = childNodes.at(index);
        auto ip = node.attributes().namedItem("ip");
        auto port = node.attributes().namedItem("port");
        auto type = node.attributes().namedItem("type");
        auto recv = node.attributes().namedItem("recv");
        auto name = node.attributes().namedItem("name");
        auto feedBackPort = node.attributes().namedItem("feedBackPort");
        std::shared_ptr<messageConvert::NetInfo> net(new messageConvert::NetInfo);
        net->ip = ip.nodeValue();
        net->name = name.nodeValue();
        net->port = port.nodeValue().toInt();
        net->feedBackPort = feedBackPort.nodeValue().toInt();
        net->bRecvTag = recv.nodeValue().toInt();
        if (type.nodeValue().toUpper() == "TCP") net->netType = messageConvert::emTCP;
        else if (type.nodeValue().toUpper() == "DDS") net->netType = messageConvert::emDDS;
        else net->netType = messageConvert::emUDP;
        netlist.push_back(net);
    }
    return 0;
}

int main(int argc, char* argv[])
{
    QCoreApplication application(argc, argv);
    QVector<std::shared_ptr<messageConvert::NetInfo>> netlist;
    const QString configPath = QCoreApplication::applicationDirPath() + "/config.xml";
    readMessageXML(configPath, netlist);
    messageConvert converter;
    converter.start(netlist);
    return application.exec();
}
