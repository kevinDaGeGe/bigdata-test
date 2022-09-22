#!/bin/bash

#sftp服务器地址
host="115.236.31.83"
#端口
port=54223
sftp_user="jjsrsftp"
#密码
password="Flash@8756"
#日期
if [ $# != 0 ];then
  d=$1
else
  d=$(date -d "yesterday" +%Y%m%d)
fi
echo "handle $d day data"
#下载到本地的目录
localDir="/bigdata/data/wine_exchange/docs/"$d
[ -d "$localDir" ] && rm -rf "$localDir"
[ ! -d "$localDir" ] && mkdir -p "$localDir"
#sftp中待下载文件目录
remoteDir="/docs/"$d
#HDFS文件路径
hdfsDir="/data/wine_exchange/docs/"
#支持的文件类型
fileTypes=("PAYMENT" "FUNDJOUR" "FUND")

lftp -u ${sftp_user},${password} sftp://${host}:${port}<<EOF
cd ${remoteDir}
lcd ${localDir} 
mget *
by
EOF
echo "download file success!"

#mkdir hdfs parent directory
if ! hadoop fs -test -e $hdfsDir
then
  hdfs dfs -mkdir $hdfsDir
fi
for typeStr in "${fileTypes[@]}"
do
  if ! hadoop fs -test -e "$hdfsDir""$typeStr"
  then
    hdfs dfs -mkdir "$hdfsDir""$typeStr"
  fi
done

#遍历已下载文件,并挨个上传
for dir in "$localDir"/*
do
   [[ -e "$dir" || ! -f "$dir" ]] || break  # handle the case of null files
   fileName=$(basename "$dir")
   for typeStr in "${fileTypes[@]}"
   do
     if [[ $fileName == "JJS_${typeStr}_"* ]]
     then
       hdfsFilePath="${hdfsDir}${typeStr}/${fileName}"
       echo "uploading file to $hdfsFilePath"

       #delete already file
       if hadoop fs -test -e "$hdfsFilePath"
         then
           #echo "delete already exists file [$hdfsFilePath]"
           hdfs dfs -rm "$hdfsFilePath"
       fi

       #delete file head
       sed -i '1d' "$dir" > "${dir}_tmp"

       #upload
       hadoop fs -put "${localDir}/${fileName}_tmp" "$hdfsFilePath"
       echo "upload file [$fileName] success "

       #delete tmp file
       rm -f "${dir}_tmp"
     fi
   done



done 


