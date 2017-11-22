from socket import *
PORT = 9001
HOST = 'localhost'


def fun_send(args):
    s = socket(AF_INET, SOCK_DGRAM)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    # //#005#2#
    msg = str(db_select) + '#' + args
    print(msg)
    s.sendto(msg.encode('utf8'), (HOST, PORT))
    data = s.recv(1024)
    print(data)
    s.close()
    return data


if __name__== "__main__":
    fun_send('stop')

