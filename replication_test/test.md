配置5个linux虚拟机 https://blog.csdn.net/weixin_43372529/article/details/105619597
# 添加源：
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
# clickhouse安装
sudo apt-get install -y clickhouse-server clickhouse-client
设置密码默认

sudo apt-get install -y clickhouse-keeper

sudo systemctl enable clickhouse-keeper
sudo systemctl start clickhouse-keeper
sudo systemctl status clickhouse-keeper




clickhouse集群：
01：223.193.36.157
02：223.193.36.52
clickhouse-keeper集群：
01：223.193.36.224
02：223.193.36.225
03：223.193.36.226

2024.05.08 11:15:50.224599 [ 392862 ] {} <Error> db1.table1 (a40660ed-2b18-4064-a4f5-6df49a0bfe47): auto DB::StorageReplicatedMergeTree::processQueueEntry(ReplicatedMergeTreeQueue::SelectedEntryPtr)::(anonymous class)::operator()(LogEntryPtr &) const: Code: 86. DB::HTTPException: Received error from remote server http://Ubuntu-2204:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftables%2Fa40660ed-2b18-4064-a4f5-6df49a0bfe47%2F01%2Freplicas%2F01&part=all_1_1_0&client_protocol_version=8&compress=false. HTTP status code: 500 Internal Server Error, body: Code: 221. DB::Exception: No interserver IO endpoint named DataPartsExchange:/clickhouse/tables/a40660ed-2b18-4064-a4f5-6df49a0bfe47/01/replicas/01. (NO_SUCH_INTERSERVER_IO_ENDPOINT), Stack trace (when copying this message, always include the lines below):

vim /etc/clickhouse-server/config.d/replication.xml

chown clickhouse:clickhouse /etc/clickhouse-server/config.d/replication.xml
service clickhouse-server restart

如果clickhouse-keeper无法启动
mkdir /var/lib/clickhouse-keeper /var/lib/clickhouse-keeper/preprocessed_configs
vim /etc/clickhouse-keeper/keeper_config.xml
systemctl restart clickhouse-keeper
systemctl status clickhouse-keeper


apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | tee /etc/apt/sources.list.d/clickhouse.list

apt-get update





/etc/clickhouse-server/config.xml
<listen_host>::</listen_host>
<interserver_http_host></interserver_http_host>



iterm2 fish





docker的systemctl报错https://blog.csdn.net/m0_46304383/article/details/121483556













卸载：
rm -rf /var/lib/clickhouse-keeper
rm -rf /var/log/clickhouse-keeper
rm -rf /var/lib/clickhouse
rm -rf /etc/clickhouse-*
rm -rf /var/log/clickhouse-server
sudo apt-get --purge remove clickhouse-server
sudo apt-get --purge remove clickhouse-client 
sudo apt-get --purge remove clickhouse-common-static
sudo apt-get --purge remove clickhouse-keeper

