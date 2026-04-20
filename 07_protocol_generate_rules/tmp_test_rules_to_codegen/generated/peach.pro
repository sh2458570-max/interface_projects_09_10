QT = core xml network concurrent

CONFIG += c++17 cmdline
TARGET = temp_sensor_to_temp_report
SOURCES += \
	main.cpp \
	messageconvert.cpp \
	temp_sensor_to_temp_report.cpp \
	codec.cpp

HEADERS += \
	messageconvert.h \
	temp_report_def.h \
	temp_sensor_def.h \
	temp_sensor_to_temp_report.h \
	codec.h
