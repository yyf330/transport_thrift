import redis
import threading
import time
import sys
from socket import *
import json
import multiprocessing
from multiprocessing import queues
import thriftpy
#print(len(sys.argv))
if len(sys.argv)<2:
    print("Usage:[work][workdb][s_name][cid]")
    exit(0)
else:
    work = sys.argv[1]
    db_select = sys.argv[2]
    s_name = sys.argv[3]
    cid = sys.argv[4]

import consulate
consul = consulate.Consul()

e_que = queues.Queue(5, ctx=multiprocessing)
# print(db_select)
r = redis.Redis(db=str(db_select),password='admin')
if not r.exists("xid_count"):
    xid_count='0'
else:
    xid_count=r.get('xid_count')
# print("redis connect")
exit_flag=False


def get_host_ip():
    try:
        s = socket(AF_INET, SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip

PORT = 9001
HOST = get_host_ip()

gui_client = thriftpy.load("/home/dss/transport/ToWeb.thrift", module_name="gui_client_thrift")
from thriftpy.rpc import make_client

# Add a service to the local agent
def agent_register(name,s_port,s_address,s_tags):
    consul.agent.service.register(name,
                                   address=s_address,
                                   port=s_port,
                                   tags=s_tags,
                                   )

def get_gui_agent_services():
    services = consul.agent.services()
    # print((services))
    re_serv_m={'address':'','port':''}
    for key in services:
        for key1 in key:
            # print(key1)
            if key1 == "gui":
                re_serv_m['address']=key[key1]['Address']
                re_serv_m['port']=key[key1]['Port']
                # re_service.update({key1:re_serv_m})
                # print(re_serv_m)
            # re_serv_m={}
    # print(re_serv_m)
    return re_serv_m

# import thriftpy

# micro_thrift_control = thriftpy.load("redis_control.thrift", module_name="redis_control_thrift")
# micro_thrift_control = thriftpy.load("transport_control.thrift", module_name="transport_control_thrift")
#
#
# from thriftpy.rpc import make_server

# class Dispatcher(object):
#     def transportstart(self,n):
#         print("transportstart")
#         print(n)
#
#         listtest = ['wangdl', 'test']
#
#         return 3
#
#     def transportstop(self,n):
#         print("transportstop")
#         print(n)
#
#         listtest = ['wangdl', 'test']
#
#         return 2
#
#     def transportpause(self,n):
#         print(n)
#         print("transportpause")
#
#         listtest = ['wangdl', 'test']
#
#         return 1

def redis_to_ps(e_que):
    # print("thread  redis_to ps  start!")
    while True:
        global xid_count
        global exit_flag
        if exit_flag:
            print("exit")
            break

        if e_que.qsize() == 0:
            pass
        else:
            status=e_que.get()
            print(status)
            if status=='stop':
                exit(0)
        # time1=time.time()
        # print('spent_time1=', time.clock())
        if not (r.exists("capture_xid")):
            # print("no xid!")
            #time2 = time.time()
            # print('spent_time2=', time.clock())
            time.sleep(1)
            continue
        xid_key=r.rpop("capture_xid")
        # print("xid=",xid_key)
        pid_results = []
        pid_results = r.lrange(xid_key,0,-1)
        # print("pid_results=",pid_results)
        pid_results.sort()
        commit_flag = False
        rollback_flag = False
        if len(pid_results)!=0:
            for key in pid_results:

                hmlist=r.hmget(key,["scn","TIMESTAMP","operation_code"])
                # print(hmlist)
                if hmlist[2] is None:
                    continue
                operation_code=bytes.decode(hmlist[2])
                # print(operation_code)
                # print(key)
                # print("pid_results=",pid_results)
                if operation_code=='6':
                    r.delete(key)
                    r.lrem(xid_key, key, 1)
                    continue
                elif operation_code=='7':

                    if commit_flag:
                        r.lpush("capture_commit", xid_key)
                        xid_count=str(int(xid_count)+1)
                        r.set("xid_count", xid_count)
                    else:
                        for key_delete in pid_results:
                            # print("lajikey=", key_delete)
                            r.delete(key_delete)
                            r.lrem(xid_key, key_delete, 1)
                        # print("lajikey=",key)
                        # r.lrem(xid_key, key, 1)
                        # r.delete(key)
                    break
                elif operation_code=='36':
                    # rollback_flag = True
                    # r.delete(key)
                    # r.lrem(xid_key, key, 1)
                    r.lpush("xid_rollback", xid_key)

                    break
                else:
                    commit_flag=True
                    continue
                    # r.delete(bytes.decode(key))
            # if rollback_flag:
            #     for key_delete in pid_results:
            #         r.delete(key_delete)
            #         r.lrem(xid_key, key_delete, 1)

            pid_results=[]



def rollback_to_delete(args):
    # print("thread  redis_to ps  start!")
    while True:
        # time1=time.time()
        # print('spent_time1=', time.clock())
        if not (r.exists("xid_rollback")):
            # print("no xid!")
            #time2 = time.time()
            # print('spent_time2=', time.clock())
            time.sleep(1)
            continue
        xid_key=r.rpop("xid_rollback")
        # print("xid=",xid_key)

        while r.exists(xid_key):
            key = r.rpop(xid_key)
            r.delete(key)

def service_register():
    now_t=time.time()
    s_tags = ['1.0.2.0', 'T', str(int(now_t)), 'reserve']
    print('---service_register-----',s_tags)
    agent_register(s_name, PORT+int(db_select), HOST, s_tags)

def heart_push(args):
    while True:
        try:
            send_msg={'rate':'','serviceType':'T','serviceid':cid}
            if not (r.exists("xid_count")):
                xid_count = '0'
                send_msg['rate'] = xid_count
            else:
                xid_count = r.get('xid_count')
                send_msg['rate'] = xid_count.decode('utf-8')
                # send_msg['rate'] = bytes.decode(xid_count)

            print(xid_count)
            in_json = json.dumps(send_msg)
            gui_service=get_gui_agent_services()
            if gui_service['address']==''or gui_service['port']=='':
            # if True:#gui_service['address'] == '' or gui_service['port'] == '':
                gui_service['address']='127.0.0.1'
                gui_service['port']='9090'
            client = make_client(gui_client.ToWeb, gui_service['address'], int(gui_service['port']))
            ret=client.pushData(str(in_json))
            print(ret)
            service_register()
        except Exception as e:
            print('---heart push error --',e)
        # except :
        #     print("it's still wrong")
        finally:
            time.sleep(5)


def fun_receive(args):
    try:
        s = socket(AF_INET, SOCK_DGRAM)
        s.bind((HOST, PORT+int(db_select)))
        # print ('...waiting for message..')
        while True:
            data, address = s.recvfrom(1024)
            # print (data)
            str1=data.decode('utf-8')
            # db=str1.split('#')[0]
            status=str1.split('#')[1]
            #print(status)
            s.sendto('server reveive'.encode('utf8'), address)
            # s.sendto('this is the UDP server'.encode('utf8'), address)
            if status=='stop':
                e_que.put('stop')
                break
    finally:
        s.close()


def fun_send(args):
    try:
        s = socket(AF_INET, SOCK_DGRAM)
        s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        # //#005#2#
        msg = str(db_select) + '#' + args
        # print(msg)
        s.sendto(msg.encode('utf8'), (HOST, PORT+int(db_select)))
        data = s.recv(1024)
        # print(data)
    finally:
        s.close()
    return 0

# def msg_check(args):
#     while True:
#         global exit_flag
#         global db_select
#         # print("thread  msg_check start!",exit_flag)
#         HOST = ''  # Symbolic name meaning all available interfaces
#         PORT = 9400+ int(db_select) # Arbitrary non-privileged port
#
#         s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         # print ('Socket created')
#         try:
#             s.bind((HOST, PORT))
#         except Exception as e:
#             print("bind error!")
#         # print ('Socket bind complete')
#         s.listen(10)
#         # print('Socket now listening')
#
#         # wait to accept a connection - blocking call
#         conn, addr = s.accept() #wait here when no msg
#         # print ('Connected with ' + addr[0] )
#
#         # now keep talking with the client
#         data = conn.recv(2048)
#         len_buf = len(data)
#         # Size = struct.calcsize('2i24s')
#         # print("size=",Size)
#         msg_type,cmd_type,source_id= struct.unpack('3i',data[:12])
#         # bf = ""
#         # bf = bytes(bf, 'UTF-8')
#         # bf = struct.unpack("13s", data[8:21])[0]
#         # bb = bf.decode('utf-8')
#         # print(bb)
#
#         if msg_type==0x5c:
#             if cmd_type==0x13:
#                 exit_flag=True
#                 break
#         else:
#             print ('cmd_type=',cmd_type)
#
#         # print (struct.unpack("ii", data)[0])
#         # if struct.unpack("ii",data)[0]==0x5c:
#         # print("ok")
#         # print (struct.unpack("ii", data)[1])
#         conn.sendall(b'ok!')
#         conn.close()
#         s.close()


threads = []
t1 = threading.Thread(target=redis_to_ps,args=(e_que,))
threads.append(t1)
# t2 = threading.Thread(target=heart_push,args=('',))
# threads.append(t2)
# t3 = threading.Thread(target=msg_check,args=('',))
# threads.append(t3)



if __name__== "__main__":
    try:
        if work=='start':

            service_register()

            p_list=[rollback_to_delete,fun_receive,heart_push]
            for pro in p_list:
                p = multiprocessing.Process(target = pro, args = (e_que,))
                p.daemon = True
                p.start()

            try:
                for t in threads:
                    t.setDaemon(True)
                    t.start()
                t.join()
            #        server = make_server(micro_thrift_control.StatusControl, Dispatcher(), '127.0.0.1', port)
                #server.serve()

            except (KeyboardInterrupt,SystemExit):
                print ("exit!")
            finally:
                # print('end')
                consul.agent.service.deregister(s_name)
        elif work=='stop':
            fun_send('stop')
    except (KeyboardInterrupt, SystemExit):
        print("exit!")
    finally:
        # print('end')
        consul.agent.service.deregister(s_name)