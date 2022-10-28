# shellcheck disable=SC2164
# 创建测试环境
mkdir -p /opt/test/hadoop

cd /opt/test/hadoop

# 创建文件
echo '三国' >sanguo.txt
echo '魏国' >weiguo.txt
echo '蜀国' >shuguo.txt
echo '吴国' >wuguo.txt

# 上传文件, move相当于剪切粘贴, 此时本地的sanguo.txt文件会被删除
# 同时创建hdfs中的节点/sanguo
hadoop fs -moveFromLocal ./sanguo.txt /sanguo
hadoop fs -copyFromLocal ./weiguo.txt /sanguo/weiguo
hadoop fs -put ./shuguo.txt /sanguo/shuguo
hadoop fs -put ./wuguo.txt sanguo/wuguo