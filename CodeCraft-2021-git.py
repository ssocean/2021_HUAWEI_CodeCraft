import copy  # 该包用于对象深拷贝，在标准库中


class Nodes:
    def __init__(self,
                 core: int,
                 memory: int):
        '''
        节点类
        :param core: 核心数
        :param memory: 内存大小
        '''
        self.memory = memory
        self.core = core

    def _hostSplit(self):
        '''
        用于初始化服务器类
        :return:
        '''
        half_core = int(self.core / 2)
        half_mem = int(self.memory / 2)
        if half_core % 2 == 0:  # 如果是偶数
            a_core_count = half_core
            b_core_count = half_core
        else:
            a_core_count = half_core + 1
            b_core_count = half_core - 1

        if half_mem % 2 == 0:  # 如果是偶数
            a_mem_count = half_mem
            b_mem_count = half_mem
        else:
            a_mem_count = half_mem + 1
            b_mem_count = half_mem - 1
        return Nodes(a_core_count, a_mem_count), Nodes(b_core_count, b_mem_count)

    def _split(self):
        '''
        返回具有该节点一半的资源的Nodes，仅适用于虚拟机
        :return: 具有该节点一半的资源的Nodes
        '''
        return Nodes(int(self.core / 2), int(self.memory / 2))

    def processingRequest(self,
                          req_core: int,
                          req_memory: int):
        '''
        由于响应用户请求，节点资源减少，如果请求合法，相应地改变该节点数值。如果非法，返回假。
        :param req_core: 用户请求核心数
        :param req_memory: 用户请求内存
        :return: 布尔值，若为真，节点完成对用户请求响应，若为假，节点拒绝用户请求
        '''
        if (self.core > req_core) & (self.memory > req_memory):
            self.core = self.core - req_core
            self.memory = self.memory - req_memory
            return True
        return False

    def releasingRequest(self,
                         req_core: int,
                         req_memory: int):
        '''
        由于响应用户请求，节点资源增加，返回真
        :param req_core:
        :param req_memory:
        :return:
        '''
        self.core = self.core + req_core
        self.memory = self.memory + req_memory
        return True

    def checkContain(self, nodes):
        if self.core > nodes.core and self.memory > nodes.memory:
            return True
        return False


class Vm:
    def __init__(self,
                 type: str,
                 Nodes: Nodes,
                 isDouble: int):
        '''
        虚拟机类
        :param type: 虚拟机名称
        :param Nodes: 虚拟机资源节点
        :param isDouble: 是否为双节点部署，是为1
        '''
        self.isDouble = isDouble
        self.Nodes = Nodes
        self.type = type
        self.name = ''
        self.host = None
        if self.isDouble == 1:  # 如果是双节点部署
            self.name = 'half-' + type
        else:
            self.name = type

    def setHost(self, host):
        self.host = host

    def getHost(self):
        return self.host

    def removeHost(self):
        self.host = None

    def printStatus(self):
        if self.host:
            print(f'----------------虚拟机{self.type}-----------------------\n'
                  f'实际名称:{self.name}\n'
                  f'资源:core {self.Nodes.core};memory {self.Nodes.core}\n'
                  f'部署在主机:{self.host.type}')
        else:
            print(f'----------------虚拟机{self.type}-----------------------\n'
                  f'实际名称:{self.name}\n'
                  f'资源:core {self.Nodes.core};memory {self.Nodes.core}\n'
                  f'该虚拟机实例对象未被部署')


class Request():
    def __init__(self,
                 id: str,
                 action: int,
                 vm: Vm = None):
        '''
        用户请求类
        :param id: 请求编号
        :param action:请求动作，1添加 0删除
        '''
        if action == 1:  # 添加
            self.vm = vm
        self.action = action
        self.id = id

        if vm:
            cm_ratio = vm.Nodes.core / vm.Nodes.memory
            self.hostGroupType = round(cm_ratio, 3)  # 内存优先型服务器

    def printStatus(self):
        print(f'-------------请求编号{self.id}-----------------\n'
              f'-------------请求动作{self.action}-----------------\n'
              f'-------------相关虚拟机{self.vm.type}-----------------\n')


class Host:
    def __init__(self,
                 type: str,
                 Nodes: Nodes,
                 cost: int,
                 electricity: int):
        '''
        服务器类
        :param type: 服务器名称
        :param Nodes: 服务器资源节点
        :param cost: 服务器造价
        :param electricity: 服务器日耗电费
        '''
        self.shadow_id = -1
        self.A_VM_list = []
        self.B_VM_list = []
        self.electricity = electricity
        self.cost = cost
        self.type = type
        self.id = -1
        self.__initANodes, self.__initBNodes = Nodes._hostSplit()

        self.A_Nodes, self.B_Nodes = Nodes._hostSplit()

        self.value = float(cost / (Nodes.core + Nodes.memory))  # 服务器性价比
        cm_ratio = Nodes.core / Nodes.memory

        self.hostGroupType = round(cm_ratio, 3)  # 内存优先型服务器

    def get_usage(self):
        '''
        获取服务器资源使用情况
        :return:
        '''
        total_usage = (((self.A_Nodes.core + self.B_Nodes.core) / (self.__initANodes.core + self.__initBNodes.core))
                       + ((self.A_Nodes.memory + self.B_Nodes.memory) / (
                        self.__initANodes.memory + self.__initBNodes.memory))) / 2
        a_core_usage = (self.A_Nodes.core / self.__initANodes.core)
        b_core_usage = (self.B_Nodes.core / self.__initBNodes.core)
        a_memory_usage = (self.A_Nodes.memory / self.__initANodes.memory)
        b_memory_usage = (self.B_Nodes.memory / self.__initBNodes.memory)
        return 1 - total_usage, 1 - a_core_usage, 1 - b_core_usage, 1 - a_memory_usage, 1 - b_memory_usage

    def _handelRequest(self,
                       request: Request):
        '''
        主机响应用户请求，若主机有资源完成请求响应，返回(bool 真|假，str 请求request中包含虚拟机的部署节点)
        :param request: 用户请求类实例对象
        :return: bool,str(部署的节点'both|failed|A|B')
        '''
        if request.action == 1:  # 如果是部署虚拟机

            request_vm = copy.deepcopy(request.vm)  # 获取独立的深层拷贝的虚拟机实例对象

            status, deployed_node = self._deployVm(request_vm)

            if status:
                request_vm.setHost(self)
                loggedVmDict[request.id] = request_vm
                return True, deployed_node
            else:
                return False, 'Failed'
        else:  # 否则释放虚拟机

            request_vm = loggedVmDict[request.id]

            if self._releaseVm(request_vm):
                return True
            return False, 'Failed'

    def _deployVm(self,
                  vm: Vm):
        '''
        服务器部署请求
        :param vm: 待部署的虚拟机实例对象
        :return: bool,str(部署的节点'both|failed|A|B')
        '''
        if vm.isDouble == 1:  # 如果虚拟机是双节点
            splitedNodes = vm.Nodes._split()
            if self.A_Nodes.processingRequest(splitedNodes.core, splitedNodes.memory) \
                    & self.B_Nodes.processingRequest(splitedNodes.core, splitedNodes.memory):
                self.A_VM_list.append(vm)
                self.B_VM_list.append(vm)
                return True, 'both'
            else:
                return False, 'Failed'

        else:  # 否则单节点
            # 将该节点加入服务器A节点
            if self.A_Nodes.processingRequest(vm.Nodes.core, vm.Nodes.memory):  # 如果A节点未满，加入A节点，返回真
                self.A_VM_list.append(vm)
                vm.setHost(self)
                return True, 'A'
            elif self.B_Nodes.processingRequest(vm.Nodes.core, vm.Nodes.memory):  # 如果A已满，将该节点加入服务器B节点，返回真
                self.B_VM_list.append(vm)
                vm.setHost(self)
                return True, 'B'
            else:  # 如果B也满，返回假
                return False, 'Failed'

    def _releaseVm(self,
                   vm: Vm):
        '''
                服务器释放资源请求
                :param vm: 待释放的虚拟机实例对象
                :return: bool
                '''
        if vm.isDouble == 1:  # 如果虚拟机是双节点
            splitedNodes = vm.Nodes._split()
            if self.A_Nodes.releasingRequest(splitedNodes.core, splitedNodes.memory) \
                    & self.B_Nodes.releasingRequest(splitedNodes.core, splitedNodes.memory):  # 释放两个节点上的虚拟机资源
                # 虚拟机列表删除
                self.A_VM_list.remove(vm)
                self.B_VM_list.remove(vm)
                vm.removeHost()
                return True
            else:
                return False

        else:  # 否则单节点
            # 虚拟机是否在A节点,若是，删除该节点
            if vm in self.B_VM_list:
                self.B_Nodes.releasingRequest(vm.Nodes.core, vm.Nodes.memory)
                self.B_VM_list.remove(vm)
                vm.removeHost()
                return True
            elif vm in self.A_VM_list:
                self.A_Nodes.releasingRequest(vm.Nodes.core, vm.Nodes.memory)
                self.A_VM_list.remove(vm)
                vm.removeHost()
                return True
            else:
                return False

    def isFitVm(self, vm: Vm):
        '''
        用于检测self服务器类对象能否容纳虚拟机类对象vm
        :param vm: 虚拟机类对象
        :return: bool
        '''
        vm_nodes = vm.Nodes
        # 如果是双节点
        if vm.isDouble == 1:
            if self.A_Nodes.checkContain(vm_nodes._split()) and self.B_Nodes.checkContain(vm_nodes._split()):
                return True, 'both'
            return False
        else:
            if self.A_Nodes.checkContain(vm_nodes):
                return True, 'A'
            elif self.B_Nodes.checkContain(vm_nodes):
                return True, 'B'
            else:
                return False

    def printStatus(self):
        print(
            f'--------------------------------------------主机{self.type}----------------------------------------------------------------\n'
            f'A节点使用情况\ncore:{self.A_Nodes.core}/{self.__initANodes.core}; '
            f'memory:{self.A_Nodes.memory}/{self.__initANodes.memory}\n'
            f'B节点使用情况\ncore:{self.B_Nodes.core}/{self.__initBNodes.core}; '
            f'memory:{self.B_Nodes.memory}/{self.__initBNodes.memory}\n'
            f'A节点包含虚拟机列表:{[vm.name for vm in self.A_VM_list]}\n'
            f'B节点包含虚拟机列表:{[vm.name for vm in self.B_VM_list]}\n'
        )
        pass


availableHost = {}  # 当前以购买服务器字典


def get_host_value(elem):
    return elem.value


def get_host_usage(elem):
    return elem.get_usage()


def get_closed_num(defaultnumber, li):
    select = defaultnumber - li[0]
    index = 0
    for i in range(1, len(li) - 1):
        select2 = defaultnumber - li[i]
        if (abs(select) > abs(select2)):
            select = select2
            index = i
    return li[index]


def sortAvailableHostDict():
    for ratio, hostlist in availableHost.items():
        hostlist.sort(key=get_host_value, reverse=True)
        availableHost[ratio] = hostlist


class Handler:
    def __init__(self,
                 dataDict: dict):
        self.shadow_host_id = 0
        self.host_id = 0
        self.dataDict = dataDict
        self.req_dict = dataDict['reqDict']
        self.vmDict = dataDict['vmDict']
        self.vmList = dataDict['vmList']
        self.hostList = dataDict['hostList']
        self.sortedHostDict = {}

    def _init_sortedHostDict_(self):
        '''
        该方法在类初始化后必须手动调用，根据服务器对象的value属性对所有的服务器进行排序
        :return:
        '''
        for host in self.hostList:

            if host.hostGroupType not in self.sortedHostDict.keys():
                self.sortedHostDict[host.hostGroupType] = [host]
            else:
                self.sortedHostDict[host.hostGroupType].append(host)
        for ratio, hostlist in self.sortedHostDict.items():
            hostlist.sort(key=get_host_value, reverse=True)

    def _refresh_availableHost_(self):
        '''
        依据利用率对当前以购买服务器字典进行排序
        :return:
        '''
        for ratio, hostlist in availableHost.items():
            hostlist.sort(key=get_host_usage)

    def _startProcessing(self):
        print('开始相应请求')
        '''
        已删除相关代码
        '''

    def processDayRequest(self,
                          day: int):
        print('处理一天的请求')
        '''
        已删除相关代码
        '''

    def _processSingleRequest(self, req: Request):
        '''
        根据单个请求决定虚拟机部署至何处
        :param req:
        :return:
        '''

        '''
                已删除相关代码
        '''
        if req.action == 1:  # 如果是增加虚拟机请求
            pass

        else:  # 释放资源请求
            pass

    def _getMemoryByMigration(self, reqList):
        '''
        迁移虚拟机，每天进行一次，数量不得超过5‰
        :return: (bool,host,)
        '''
        '''
                        已删除相关代码
        '''
        pass

    def _migrateVm(self,
                   vm: Vm,
                   dstHost: Host):
        '''
        将虚拟机vm迁移至dstHost上
        :param vm:
        :param dstHost:
        :return:
        '''
        if not vm.host:
            return False
        current_host = vm.host
        deployed_status, deployed_node = dstHost._deployVm(vm)
        if (deployed_status & current_host._releaseVm(vm)):
            return True, deployed_node
        return False


mapIdandShadowId = {}  # 用于建立主机真实id与输出id的对应关系


class Logger:
    '''
    日志类
    用于打印输出
    '''

    def __init__(self):
        self.purchaseHistoryDict = {}
        self.deployHistoryList = []
        self.host_id = 0
        self.vm_id = 0
        pass

    def _CLEARCACHE_(self):
        '''
        清除缓存
        :return:
        '''
        self.purchaseHistoryDict = {}
        self.deployHistoryList = []

    def standOut(self):
        '''
        输出一天的操作记录
        :return:
        '''
        self.printPurchaseLog()
        self.printMigrationlog()
        self.printDeploylog()
        self._CLEARCACHE_()
        pass

    def logPurchase(self, host: Host):
        '''
        记录一天购买信息
        :return:
        '''
        if host.type not in self.purchaseHistoryDict.keys():
            self.purchaseHistoryDict[host.type] = []
            self.purchaseHistoryDict[host.type].append(host)
        else:
            self.purchaseHistoryDict[host.type].append(host)

    def printPurchaseLog(self):
        type_count = len(self.purchaseHistoryDict.keys())
        print(f'(purchase, {type_count})')
        for host_type, host_list in self.purchaseHistoryDict.items():
            count = 0
            for host in host_list:
                mapIdandShadowId[host.shadow_id] = self.host_id
                self.host_id += 1
                count += 1
            print(f'({host_type}, {count})')

    def logMigration(self, vm: Vm, dstHost: Host):
        print('(migration, 0)')

    def printMigrationlog(self):
        self.logMigration()

    def logDeploy(self, host: Host, nodes_info: str):
        self.deployHistoryList.append((host, nodes_info))

    def printDeploylog(self):
        for item in self.deployHistoryList:
            if item[1] not in 'A' and item[1] not in 'B':
                print(f'({mapIdandShadowId[item[0].shadow_id]})')
            else:
                print(f'({mapIdandShadowId[item[0].shadow_id]}, {item[1]})')
        pass


log = Logger()


def main():
    # to read standard input
    # process
    # to write standard output
    # sys.stdout.flush()
    handler = Handler(data.get_data_dict())  # 初始化控制类对象
    handler._init_sortedHostDict_()  # 一次型初始化服务器列表
    handler._startProcessing()
    # showPurchasedHost(handler)


#

def showPurchasedHost(handler: Handler):
    host_dict = availableHost
    total_cost = 0
    for ratio, hostlist in host_dict.items():
        hostlist.sort(key=get_host_usage)
        print(len(hostlist))
        print(f'-----{ratio}----')

        for elem in hostlist:
            total_cost += elem.cost
            total_usage, a_core_usage, b_core_usage, a_memory_usage, b_memory_usage = elem.get_usage()
            print(f'名称：{elem.type}；利用率{total_usage};A节点核心利用率{a_core_usage}；内存利用率{a_memory_usage}'
                  f'B节点核心利用率{b_core_usage}；内存利用率{b_memory_usage}')
        print(total_cost)


class Dataset:
    def __init__(self, pth):  #
        '''
        数据集加载类,该类负责加载数据集
        :param pth: txt文件路径
        '''
        with open(pth, "r") as f:  # 打开文件
            self.data = f.readlines()  # 读取文件

        # self.data = sys.stdin.readlines()

    def getVmByType(self, type: str):
        '''
        根据虚拟机的类型获取对应虚拟机对象
        :param type: 虚拟机类型
        :return: 虚拟机类实例对象
        '''
        if type:
            vm = self.vmDict[type]
            return vm
        return None

    def get_data_dict(self):
        hostCount = int(self.data[0])  # 主机数量
        hostList = []  # 主机列表
        for i in range(hostCount):
            h = self.data[i + 1][1:-2].split(',')
            type = h[0]
            n = Nodes(int(h[1]), int(h[2]))
            cost = int(h[3])
            e_cost = int(h[4])
            hostList.append(Host(type, n, cost, e_cost))
        self.hostList = hostList
        vmCount = int(self.data[hostCount + 1])

        vmDict = {}
        vmList = []
        for i in range(vmCount):
            v = self.data[i + hostCount + 2][1:-2].split(',')
            type = v[0]
            n = Nodes(int(v[1]), int(v[2]))
            isDouble = int(v[3])
            vmDict[v[0]] = Vm(type, n, isDouble)
            vmList.append(vmDict[v[0]])
        self.vmDict = vmDict
        self.vmList = vmList
        day = int(self.data[hostCount + vmCount + 2])
        curRequests = 0
        requests_dict = {}
        for k in range(day):
            dayIndex = hostCount + vmCount + 3 + k + curRequests
            curRequests += int(self.data[dayIndex])
            dayRequest = int(self.data[dayIndex])
            cur_request_list = []
            for j in range(dayRequest):
                request = self.data[dayIndex + 1 + j][1:-2].replace(' ', '')  # request类似为 add, vm6BEFF, 170942704
                request_list = request.split(',')
                action = 1 if (request_list[0] == 'add') else 0
                vm_type = request_list[1] if (action == 1) else None
                id = request_list[-1]
                cur_vm = self.getVmByType(vm_type)
                cur_request_list.append(Request(id, action, cur_vm))
            requests_dict[k] = cur_request_list

        return {'hostList': hostList,
                'vmDict': vmDict,
                'vmList': vmList,
                'reqDict': requests_dict}


if __name__ == "__main__":
    loggedVmDict = {}  # 记录请求id与虚拟机实体对应关系
    data = Dataset('training-1.txt')  #


    def getVmByType(type: str,
                    vm_dict: dict):
        '''
        根据虚拟机的类型获取对应虚拟机对象
        :param type: 虚拟机类型
        :return: 虚拟机类实例对象
        '''
        vm = vm_dict[type]
        return vm


    vmDict = data.get_data_dict()['vmDict']
    main()
