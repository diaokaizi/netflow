/etc/clickhouse-server/config.xml
<max_concurrent_queries>1</max_concurrent_queries>最大并发查询设置为1
<listen_host>::</listen_host>，打开clickhouse的外部访问

配置存储策略
vim /etc/clickhouse-server/config.d/storage_config.xml
```xml
<clickhouse>
  <storage_configuration>
    <disks>
        <my_local_disk> <!-- disk name -->
            <path>/var/lib/clickhouse/disks/my_local_disk/</path>
        </my_local_disk>
        <local_disk> <!-- disk name -->
            <path>/var/lib/clickhouse/disks/local_disk/</path>
        </local_disk>
        <s3_disk>
            <type>s3</type>
            <endpoint>https://aiopss3.cstcloud.cn/netflow-clickhouse/</endpoint>
            <access_key_id>4bee9b5a927f11ee9c2fb4055d678c75</access_key_id>
            <secret_access_key>911e3de735cc0e152c3dfdc471a07978cd84bc96</secret_access_key>
            <metadata_path>/var/lib/clickhouse/disks/s3_disk/</metadata_path>
        </s3_disk>
        <s3_cache>
            <type>cache</type>
            <disk>s3_disk</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    <policies>
        <local_s3>
            <volumes>
                <hot>
                    <disk>local_disk</disk>
                    <max_data_part_size_bytes>26843545600</max_data_part_size_bytes>
                    <!-- <max_data_part_size_bytes>25Gi</max_data_part_size_bytes> -->
                </hot>
                <cold>
                    <disk>s3_disk</disk>
                    <prefer_not_to_merge>true</prefer_not_to_merge>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_s3>
    </policies>
  </storage_configuration>
</clickhouse>
```
重启服务更新配置
service clickhouse-server restart