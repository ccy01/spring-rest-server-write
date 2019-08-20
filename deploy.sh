#!/bin/sh

## 1. export build.xml, setup javac and war
## 2. download ant
## 3. add ant/bin to $PATH
## 4. setup ANT_HOME

rc=0
version=$1

if [ "$version" == "" ] ; then
    echo "usage: ""$0"' svn_version'
    exit 1
fi

if [ -d "WebContent2" ]; then
    echo "ERROR: WebContent2 exists, please check !"
    exit 0
fi

mv WebContent WebContent2 
mkdir WebContent
cp WebContent2/index.html WebContent/
cp WebContent2/WEB-INF WebContent/ -rf
rm WebContent/WEB-INF/lib/* -rf

cd src
mv config.properties config.properties.bak
mv config.properties.hwcloud config.properties
mv spring/applicationContext.xml spring/applicationContext.xml.bak
mv spring/applicationContext.xml.hwcloud spring/applicationContext.xml
cd -

ant clean
ant
if [ $? -ne 0 ] ; then
    echo "ERROR: ant build error !"
else
    ant war
    if [ $? -ne 0 ] ; then
        echo "ERROR: ant war error !"
    else
        rc=1
    fi
fi

rm WebContent -rf
mv WebContent2 WebContent
cd src 
mv config.properties config.properties.hwcloud
mv config.properties.bak config.properties
mv spring/applicationContext.xml spring/applicationContext.xml.hwcloud
mv spring/applicationContext.xml.bak spring/applicationContext.xml
cd -

if [ $rc -ne 0 ] ; then
    scp -v -P62627 releases/SpringRestServerWrite.war gx@112.74.188.238:~/SpringRestServerWrite.war.$version
    if [ $? -ne 0 ] ; then
        echo "ERROR: uploaded war error !"
    else
        ssh -v -p62627 gx@112.74.188.238 "/home/gx/scripts/deploy/deploy-write.sh /home/gx/SpringRestServerWrite.war."$version
    fi
fi
exit 0
