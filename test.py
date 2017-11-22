from socket import * 
PORT = 9001
HOST = 'localhost'


def fun_receive():
    s = socket(AF_INET, SOCK_DGRAM)
    s.bind((HOST, PORT))
    print ('...waiting for message..')
    while True:
        data, address = s.recvfrom(1024)
        print (data)
        str1=data.decode('utf-8')
        print(str1)
        status=str1.split('#')[1]
        #print(status)
        s.sendto('this is the UDP server'.encode('utf8'), address)
        if status=='stop':
            break

    s.close()
    print('exit')

def fun_send(args):
    db_select=0
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # //#005#2#
    msg =  str(db_select) + '#' + args
    # print(msg)
    s.sendto(msg.encode('utf8'), (HOST, PORT))
    data = s.recv(1024)  
    print (data)
    s.close()

if __name__== "__main__":
    fun_receive()    




