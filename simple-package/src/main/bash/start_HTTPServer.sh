TARGET_FILE_NAME=$1
#ifconfig | grep inet | grep netmask
#LOCAL_IP=ifconfig | awk '/inet / {print $2}'
function GET_LOCAL_IP() {
python -c "
def get_local_ip():
    local_ip = ''
    try:
        import socket
        socket_objs = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]
        ip_from_ip_port = [(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in socket_objs][0][1]
        ip_from_host_name = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith('127.')][:1]
        local_ip = [l for l in (ip_from_ip_port, ip_from_host_name) if l][0]
    except (Exception),e:
        print('get_local_ip found exception : %s' % e)
    return local_ip if('' != local_ip and None != local_ip) else socket.gethostbyname(socket.gethostname())

print get_local_ip()
"
}
LOCAL_IP=`ifconfig | awk '/inet / {print $2}' | grep -v "127.0.0.1"`
LOCAL_IP=$(GET_LOCAL_IP)
#read -r -p "input Local IP : " LOCAL_IP
echo "wget -c -t 10 ${LOCAL_IP}:12306/${TARGET_FILE_NAME}"
python -m SimpleHTTPServer 12306
