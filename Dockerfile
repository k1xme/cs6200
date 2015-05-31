FROM golang

MAINTAINER Kexi Long

VOLUME /ap_data

ADD /ap_data /ap_data

RUN ["go", "get", "github.com/olivere/elastic"]
RUN ["go", "get", "github.com/mattbaird/elastigo"] 
