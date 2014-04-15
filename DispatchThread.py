from convirt.controllers.AtomRequest import AtomRequest
from convirt.controllers.MessageQueue import MessageQueue
import logging
logger = logging.getLogger("convirt.services.model")
import threading
import time
import re
import pylons
import simplejson as json
import traceback
from random import Random
from tg import expose,flash,require,url,request,redirect,response,session,config
from convirt.controllers.ControllerBase import ControllerBase
from convirt.model.CustomPredicates import authenticate
from convirt.model import *
from convirt.model.SessionList import SessionListManager###
from convirt.model.network import Networks_Manager,Networks
from convirt.viewModel.NodeService import NodeService
from convirt.viewModel.VMService import VMService
from convirt.viewModel.NetworkService import NetworkService
from convirt.viewModel.TaskCreator import TaskCreator
from convirt.core.utils.utils import to_unicode,to_str,print_traceback
import convirt.core.utils.constants
from convirt.viewModel.StorageService import StorageService
constants = convirt.core.utils.constants
import sys
from convirt.model.services import Task, TaskResult
from convirt.model.DBHelper import DBHelper
import transaction
from convirt.model import DBSession
from convirt.viewModel import Basic 
#from processing import Process
class DispatchThread(threading.Thread):
#class DispatchThread:
    storage_service = StorageService()
    def __init__(self,id,job_queue,tc,node_service):
        threading.Thread.__init__(self)
        self.id=id
        self.job_queue = job_queue
        self.tc = tc
        self.node_service = node_service
        logger.debug('DispatchThread initialized')
    def putMessage(self,message):
        self.job_queue.putMessage(message)
    def run(self):
        #logger.debug('Debug/DispatchThread: job_queue size %s',str(self.job_queue.qsize()))
        logger.debug('DispatchThread run')
        while True:
            try:
                logger.debug('Debug/job_queue size %s',str(self.job_queue.getSize()))
                curr_request = self.job_queue.getMessage()
                logger.debug('Debug/job_queue size %s',str(self.job_queue.getSize()))
                image_id = curr_request.image_id
                config = curr_request.config
                mode = curr_request.mode
                node_id = curr_request.node_id
                priority = curr_request.priority
                group_id = curr_request.group_id
                dom_id = curr_request.dom_id
                vm_name = curr_request.vm_name
                date = curr_request.date
                time = curr_request.time
                subuser = curr_request.subuser
                _dc = curr_request._dc
                auth = curr_request.auth
                task_id = curr_request.task_id
                ERROR_MSG = ""
                logger.debug('Debug/config %s',config)
                conf = json.loads(config)
                logger.debug('Debug/config finished')
                ips_for_static =[]
                logger.debug("network_object is %s" %conf["network_object"]["ip"][0])
                ip_type = conf["network_object"]["ip"][0]["iptype"]
                #store the dispatch results
                node_dict = {}
                vm_t_num = int(conf["network_object"]["ip"][0]["number"])
                execute_on_partial = conf['general_object']['execute_on_partial']
                image=None
                if image_id is not None:
                    image_store=Basic.getImageStore()
                    image=image_store.get_image(auth,image_id)
                #update the status of task to STARTED 
                if vm_t_num > 1:
                    task_result	= DBSession.query(TaskResult).filter(TaskResult.task_id==task_id).first()
                    task_result.status = Task.STARTED
                    task_result.results = 'request is being processed' 
                    DBSession.update(task_result)
                    transaction.commit()
                networks = conf['network_object']['network']
                # for static ip, first calculate numbers,and put ip in ips_for_static
                if ip_type=="ip_static":
                    ipstring = conf["network_object"]["ip"][0]["ip"]
                    (ips_for_static,message)=self.change_ips(ipstring)
                    if not ips_for_static:
                        raise Exception(message)
                    vm_t_num = len(ips_for_static)
                mem_threshold = int(conf["general_object"]["memory"])
                disk_thresholds = {} 
                file_thresholds = {}
                network_thresholds = []
                for network in networks:
                    #logger.debug('Debug/bridge %s',network['bridge'])
                    network_thresholds.append(network['bridge'])
                #to support the multiple disk specification in templates
                for d in conf['storage_status_object']['disk_stat']:
                    if d['type'] == 'lvm':
                        vg = d['filename'].split('/')[0]
                        if disk_thresholds.has_key(vg):
                            disk_thresholds[vg] += int(d['size'])
                        else:
                            disk_thresholds[vg] = int(d['size'])
                    elif d['type'] == 'file':
                        index = d['filename'].rfind('/')
                        path = d['filename'][:(index)]
                        if file_thresholds.has_key(path):
                            file_thresholds[path] += int(d['size'])
                        else:
                            file_thresholds[path] = int(d['size'])
                #set the default disk_threshold to 5G
                logger.debug('Debug/mem_threshold %s, disk_threshold %s, file_threshold %s, network_thresholds %s, node_id %s, priority %s, vm_t_num %s',mem_threshold,disk_thresholds,file_thresholds,network_thresholds,node_id,priority,vm_t_num)
                if node_id is not None: #explicit provision, server is specified
                    if vm_t_num > 0:
                        node_dict = self.node_service.get_explicit_alloc_node(auth,group_id,image,node_id,mem_threshold,disk_thresholds,file_thresholds,network_thresholds,vm_t_num,vm_name,task_id)
                elif(priority is not None): #implicit provision, server is not specified
                    if( vm_t_num > 0):
                        node_dict = self.node_service.get_implicit_alloc_node(auth,group_id,image,priority,mem_threshold,disk_thresholds,file_thresholds,network_thresholds,vm_t_num,vm_name,task_id)
                logger.debug('Debug/allocate finished')
                file_name_conf = conf['general_object']['filename']
                ip_in_conf = conf["network_object"]["ip"][0]["ip"]
                name = vm_name
                #
                task_result = ''
                details = ''
                valid_num = 0
                if(len(node_dict) > 0):
                    for node_id, num in node_dict.iteritems():
                        valid_num += num
                    if valid_num < vm_t_num:
                        if execute_on_partial == 'no':
                            task_result += ('VM Name prefix:    '+ vm_name+'\n')
                            task_result += ('Request VM Number:    '+ str(vm_t_num)+'\n')
                            task_result += ('Template name:    '+ image.name+'\n')
                            task_result += ('Number of vms can be meeted:    '+ str(valid_num)+'\n')
                            task_result += ('Task cancelled as system can only match partial needs   '+'\n')
                            task_succ_result = DBSession.query(TaskResult).filter(TaskResult.task_id==task_id).first()
                            if task_succ_result is not None:
                                task_succ_result.results = task_result
                                task_succ_result.endtime = datetime.utcnow()
                                task_succ_result.status = Task.SUCCEEDED
                                DBSession.update(task_succ_result)
                                transaction.commit() 
                            return 
                    task_result += ('VM Name prefix:    '+ vm_name+'\n')
                    task_result += ('Request VM Number:    '+ str(vm_t_num)+'\n')
                    task_result += ('Template name:    '+ image.name+'\n')
                    task_result += ('Actual provisioned vm number:    '+ str(valid_num) +'\n')
                    task_result += 'Details: \n'
                    task_result += 'host name:  vm number\n'
                else:
                    task_result = 'Request cancelled'
                #
                for node_id, node_vm_num in node_dict.iteritems():
                    node = auth.get_entity(node_id)
                    details += '-------------------------\n'
                    if node is not None:
                        details     += (node.name + '    ' + str(node_vm_num) + '\n')
                    else:
                        details     += ('Unknown server' + '    ' + str(node_vm_num) + '\n')
                    details += '-------------------------\n'
                    ips = []
                    conf['general_object']['filename'] = file_name_conf
                    conf["network_object"]["ip"][0]["ip"] = ip_in_conf
                    #for vm_seq in range(vm_todo_num)
                    logger.debug("Debug/DispatchThread.py: key %s value %s, ip_type %s", node_id, node_vm_num,ip_type)
                    if ip_type=="ip_pool":
                        for i in range(0, node_vm_num):
                            ip = NetworkService().get_fixed_ip(auth,conf["network_object"]["ip"][0]["ip"],vm_name,type=ip_type,node_id=node_id)
                            logger.debug("ip is %s " %ip)
                            if not ip['success']:
                                ERROR_MSG += ip['msg']
                                continue
                            ips.append(ip['msg'])
                    if ip_type=="ip_static":
                        for i in range(0, node_vm_num):
                            ips.append(ips_for_static.pop(0))
                    if ip_type=="dhcp":
                        for i in range(0, node_vm_num):
                            ips.append(str(int(Random().random() * 100000)))

                    file_name = conf['general_object']['filename']
                    delay_time = conf['general_object']['delay_time']
                    #name = vm_name
                    delay = 0
                    logger.debug("ips is %s, by YH" %ips)
                    #iterate through the ips, allocate the vm one by one, noted by zhoux
                    for ip in ips:
                        # 这里有问题，在实际测试中，该处的循环其实是一个vm中的，当批量导入vm时候，ips其实只是拿一个ip，
                        # For allocate IP for Static and DHCP mode by YH
                        if ip_type=="ip_static":
                            i = NetworkService().get_fixed_ip(auth,ip,vm_name,type=ip_type,node_id=node_id)
                            logger.debug("ip is %s " %i)
                            if i.get('gateway'):
                                conf["network_object"]["ip"][0]["gateway"]=i['gateway']
                            if not i['success']:
                                ERROR_MSG += '<br>' +i['msg']+'</br>'
                                continue
                        if ip_type=="dhcp":
                            i = NetworkService().get_fixed_ip(auth,None,vm_name+'_'+ip,type=ip_type,node_id=node_id)
                            logger.debug("ip is %s " %i)
                            if not i['success']:
                                ERROR_MSG += '<br>' +i['msg']+'</br>'
                                continue
                        #For set netmask and gateway for Static and IPPool mode
                        if ip_type=="ip_pool":
                            network = DBSession.query(Networks).filter(Networks.id==conf["network_object"]["ip"][0]["ip"]).first()
                            if network:
                                if conf["network_object"]["ip"][0].get("gateway")=="" or not conf["network_object"]["ip"][0].get("gateway"):
                                    conf["network_object"]["ip"][0]["gateway"]=network.gateway

                        conf["network_object"]["ip"][0]["ip"]=ip
                        extra = conf['boot_params_object'].get('extra')
                        if ip_type=="ip_static" or ip_type=="ip_pool":
                            if conf["network_object"]["ip"][0].get("netmask")=="" or not conf["network_object"]["ip"][0].get("netmask"):
                                conf["network_object"]["ip"][0]["netmask"] ="255.255.252.0"
                            if conf["network_object"]["ip"][0].get("gateway")=="" or not conf["network_object"]["ip"][0].get("gateway"):
                                conf["network_object"]["ip"][0]["gateway"] =ip.rsplit('.',1)[0]+'.254'
                             #for ks start
                            if conf['boot_params_object']!="" and extra:
                                for i in extra.split():
                                    if i.find("gateway=")!=-1:
                                        conf['boot_params_object']['extra']=\
                                                   conf['boot_params_object']['extra'].\
                                                   replace(i,("gateway="+conf["network_object"]["ip"][0]["gateway"]))
                                    if i.find("ip=")!=-1:
                                        conf['boot_params_object']['extra'] = \
                                                   conf['boot_params_object']['extra'].replace(i,("ip="+ip))
                                    if i.find("netmask=")!=-1:
                                        conf['boot_params_object']['extra'] = \
                                                   conf['boot_params_object']['extra'].replace(i,("netmask="+conf["network_object"]["ip"][0]["netmask"]))

                                if conf['boot_params_object']['extra'].find("gateway=")==-1:
                                    conf['boot_params_object']['extra'] += \
                                               ' gateway='+conf["network_object"]["ip"][0]["gateway"]

                                if conf['boot_params_object']['extra'].find("ip=")==-1:
                                    conf['boot_params_object']['extra'] += ' ip='+ip

                                if conf['boot_params_object']['extra'].find("netmask=") ==-1:
                                    conf['boot_params_object']['extra'] += \
                                               ' netmask='+conf["network_object"]["ip"][0]["netmask"]
                        file_name_new = file_name
                        vm_name_new = vm_name
                        if ip!="":
                            file_name_new = file_name+"_"+ip
                            vm_name_new = vm_name + "_"+ip
                        conf['general_object']['filename'] = file_name_new
                        disks = conf['storage_status_object']['disk_stat']
                        for d in disks:
                            #logger.debug('Debug/provision old_filename:%s, name:%s',d['filename'],name)
                            d['filename'] = d['filename'].replace(name,vm_name_new)
                        name = vm_name_new
                        logger.debug('Debug/provision on node %s delay %s',node_id,delay)
                        #logger.debug('Debug/provision on node %s delay %s, disks:%s, file_name_new:%s, vm_name_new:%s',node_id,delay,disks,file_name_new,vm_name_new)
                        #Send task
                        self.tc.config_settings(auth, image_id, json.dumps(conf), \
                              mode, node_id, group_id, dom_id, vm_name_new,date,time,delay=delay,subuser=subuser)
                        #set the next task's execution time to be quite later
                        delay += 100000000 
                        #pre-allocate disk and mem resources when task is created
                        self.pre_allocate_res(auth,file_thresholds,disk_thresholds,mem_threshold,node_id,1)
                        details     += (vm_name_new + '\n')
                #update the task result when scheduling finished
                task_result += details
                task_succ_result = DBSession.query(TaskResult).filter(TaskResult.task_id==task_id).first()
                if task_succ_result is not None:
                    task_succ_result.results = task_result
                    task_succ_result.endtime = datetime.utcnow()
                    task_succ_result.status = Task.SUCCEEDED
                    DBSession.update(task_succ_result)
                    transaction.commit() 
                #set current task's result to be success 
                #curr_request.result.put('success')           
                #self.job_queue.messageDone()
            except Exception, e:
                print_traceback()
                print e
                #logger.debug('Exception in DispatchThread: %s',to_str(e).replace("'", " "))
                #update the task status
                task_fail_result = DBSession.query(TaskResult).filter(TaskResult.task_id==task_id).first()
                if task_fail_result is not None:
                    task_fail_result.results = to_str(e).replace("'"," ")
                    task_fail_result.endtime = datetime.utcnow()
                    task_fail_result.status = Task.FAILED
                    DBSession.update(task_fail_result)
                    transaction.commit() 
                #put the exception info in the result of the request
                #curr_request.result.put(to_str(e).replace("'"," "))
                #self = DispatchThread(self.id,self.job_queue,self.tc,self.node_service)
                #self.setDaemon(True)
                #self.start()
    def change_ips(self,s):
        try:
            s=s.replace(' ','')
            test1 = re.compile('[\d\s\.,-]*')
            test2 = re.compile('[\d]+')
            test1_result = (test1.match(s).end() == len(s))
            if not test1_result:
                return (None,"ips should be composed of (num,'.',',','-')")
            test2_result = True
            for i in test2.findall(s):
                if i:
                    if int(i)<0 or int(i)>255:
                        test2_result = False
            if not test2_result:
                return (None,"ips number should between 0 and 255")
            ip_base = ""
            ips = []
            for ip in s.split(','):
                if ip.find('-')!=-1:
                    ip_base = ip.strip().split('-')[0].rsplit('.',1)[0]
                    start=ip.strip().split('-')[0].rsplit('.',1)[-1]
                    end=ip.strip().split('-')[-1].rsplit('.',1)[-1]
                    for ip in range(int(start),int(end)+1):
                        ips.append(ip_base+"."+str(ip))
                else:
                    i = ip.split('.')
                    if len(i)==1:
                        ips.append(ip_base+"."+ip)
                    elif len(i)==4:
                        ip_base = ip.rsplit('.',1)[0]
                        ips.append(ip)
                    else:
                        return (None,"ips: "+s+" not in right expression")
            return (ips,None)
        except Exception, e:
            print_traceback()
            return (None,"ips: "+s+" not in right expression.")
    #to pre-allocate disk and memory resources before the dispatch action,add by zhoux
    def pre_allocate_res(self,auth,file_thresholds,vg_thresholds,mem_threshold,node_id,vm_t_num):
        if(file_thresholds is not None and len(file_thresholds) > 0 ):
            for path, threshold in file_thresholds.iteritems():
                self.storage_service.pre_allocate_storage_size(auth,node_id,path,threshold * vm_t_num)
        if(vg_thresholds is not None and  len(vg_thresholds) > 0):
            for vg, threshold in vg_thresholds.iteritems():
                self.storage_service.pre_allocate_storage_size(auth,node_id,vg,threshold * vm_t_num)
        self.storage_service.pre_allocate_memory_size(auth,node_id,mem_threshold * vm_t_num)
