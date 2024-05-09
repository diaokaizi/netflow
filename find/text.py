import requests
import clickhouse_connect
import dns.resolver
import subprocess
def get_clickhouse_client():
    return clickhouse_connect.get_client(host='223.193.36.52', username='default', send_receive_timeout=60)
def http_test():
    url = "http://223.193.36.11:3002/"
    result = get_clickhouse_client().query(
    f"select \
        dstTransportPort, \
        dstIP \
        from netflow \
        limit 100, 100\
    ")
    host = result.result_set
    # for t in host:
    #     print(t[0], t[1])
    #     proc = subprocess.Popen(['nmap', '-p', str(t[0]), t[1]], stdout=subprocess.PIPE)
    #     output = proc.communicate()[0]
    #     # 输出结果
    #     print(t[0], t[1])
    #     print(output)
    #     print()
    #     print()
    #     print()
    # host = ('223.193.36.121', '443')

    for host in result.result_set:
        try:
            r = requests.get(f"http://{host[1]}:{host[0]}/", timeout=0.5)
            print(r.status_code, host[0], host[1])   
        except:
            print("error")   
# http_test()
r = requests.get("http://46.4.17.85:23665/")
print(r)
def DNS_test():
    result = get_clickhouse_client().query(
    f"select \
        srcIP, \
        srcTransportPort \
        from netflow \
        where srcTransportPort == '53' \
        limit 10\
    ")
    host = result.result_set[3]
    # 定义特定DNS服务器的IP地址
    dns_server = '8.8.8.8'

    # 创建自定义的DNS解析器
    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = [dns_server]

    # 使用自定义DNS解析器发送请求
    response = requests.get('https://www.example.com', resolver=resolver)
    print(response.text)

