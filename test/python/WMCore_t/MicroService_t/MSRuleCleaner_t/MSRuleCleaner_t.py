"""
Unit tests for Unified/MSRuleCleaner.py module

"""
# pylint: disable=W0212

from __future__ import division, print_function

import json
# system modules
import os
import unittest

# WMCore modules
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner, MSRuleCleanerArchivalSkip
from WMCore.MicroService.MSRuleCleaner.MSRuleCleanerWflow import MSRuleCleanerWflow
from WMCore.Services.Rucio import Rucio
from rucio.common.exception import RuleNotFound

from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS

#from WMCore.WMSpec.StdSpecs.StdBase import StdSpecMaker
#from WMCore.WMSpec.StdSpecs.TaskChain import createTaskChain

from WMCore.WMSpec import StdSpecs
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

#from WMCore.WMSpec.StdSpecs import TaskChain
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
#from WMCore.WMSpec.WMWorkloadTools import uploadWorkloadSpec
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE

from WMCore.WMSpec.StdSpecs.ReReco  import ReRecoWorkloadFactory

import json

def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


class MSRuleCleanerTest(EmulatedUnitTestCase):
#class MSRuleCleanerTest(unittest.TestCase):
    "Unit test for MSruleCleaner module"

    def setUp(self):
        "init test class"
        self.maxDiff = None
        self.msConfig = {"verbose": True,
                         "interval": 1 * 60,
                         "services": ['ruleCleaner'],
                         "rucioWmaAcct": 'wma_test',
                         "rucioAccount": 'wma_test',
                         'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'msOutputUrl': 'https://cmsweb-testbed.cern.ch/ms-output',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
                         'rucioUrl': 'http://cms-rucio-int.cern.ch',
                         'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                         "wmstatsUrl": "https://cmsweb-testbed.cern.ch/wmstatsserver",
                         "logDBUrl": "https://cmsweb-testbed.cern.ch/couchdb/wmstats_logdb",
                         'logDBReporter': 'reqmgr2ms_ruleCleaner',
                         'archiveDelayHours': 8,
                         'archiveAlarmHours': 24,
                         'enableRealMode': False}

        self.creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                      "client_key": os.getenv("X509_USER_KEY", "Unknown")}
        self.rucioConfigDict = {"rucio_host": self.msConfig['rucioUrl'],
                                "auth_host": self.msConfig['rucioAuthUrl'],
                                "auth_type": "x509",
                                "account": self.msConfig['rucioAccount'],
                                "ca_cert": False,
                                "timeout": 30,
                                "request_retries": 3,
                                "creds": self.creds}

        self.reqStatus = ['announced', 'aborted-completed', 'rejected']
        
        self.specGenerator = WMSpecGenerator("WMSpecs")
        self.schema = []
        self.couchApps = ["WorkQueue"]
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=self.schema,
                                useDefault=False)
        self.testInit.setupCouch('workqueue_t', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_inbox', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t_inbox', *self.couchApps)
        self.testInit.generateWorkDir()

        self.msConfig.update({'QueueURL':self.testInit.couchUrl})
        print("msConfig: ", json.dumps(self.msConfig, indent=2))
        
        
        self.msRuleCleaner = MSRuleCleaner(self.msConfig)
        self.msRuleCleaner.resetCounters()
        self.msRuleCleaner.rucio = Rucio.Rucio(self.msConfig['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)
        
        

        self.queueParams = {}
        self.queueParams['log_reporter'] = "Services_WorkQueue_Unittest"
        self.queueParams['rucioAccount'] = "wma_test"
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"

        #specName = "RerecoSpec"
        #specUrl = self.specGenerator.createReRecoSpec(specName, "file",
        #                                              assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]})
        #globalQ = globalQueue(DbName='workqueue_t',
        #                      QueueURL=self.testInit.couchUrl,
        #                      UnittestFlag=True, **self.queueParams)
        #globalQ.queueWork(specUrl, "RerecoSpec", "teamA")
        #wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
#
        #gqList=globalQ.backend.getElementsForWorkflow(specName)
        #wqSList=wqService.getWQElementsByWorkflow(specName)

        #self.msRuleCleaner.globalQueue = globalQueue(
        #    DbName="workqueue",
        #    InboxDbName="workqueue_inbox",
        #    QueueURL=self.msConfig.get("globalQueueUrl", "http://localhost:5984/workqueue"),
        #    central_logdb_url=self.msConfig.get("logDBUrl", "http://localhost:5984/logdb"),
        #    RequestDBURL=self.msConfig.get("reqmgr2Url", "http://localhost:5984/reqmgr2"),
        #    UnittestFlag=False,
        #    rucioAccount=self.msConfig.get("rucioAccount"),  # Explicitly pass rucioAcct
        #    rucioUrl=self.msConfig.get("rucioUrl"),  # Explicitly pass rucioUrl
        #    rucioAuthUrl=self.msConfig.get("rucioAuthUrl"),  # Explicitly pass rucioAuthUrl
        #    rucioConfigDict=self.rucioConfigDict  # Explicitly pass rucioConfigDict
        #)
        
        print("X509_USER_CERT:", os.getenv("X509_USER_CERT"))
        print("X509_USER_KEY:", os.getenv("X509_USER_KEY"))

        #self.msRuleCleaner.globalQueue = globalQueue(
        #    DbName="workqueue",
        #    InboxDbName="workqueue_inbox",
        #    QueueURL=self.msConfig.get("globalQueueUrl", "http://localhost:5984/workqueue"),
        #    central_logdb_url=self.msConfig.get("logDBUrl", "http://localhost:5984/logdb"),
        #    RequestDBURL=self.msConfig.get("reqmgr2Url", "http://localhost:5984/reqmgr2"),
        #    UnittestFlag=False,
        #    
        #    rucioUrl=self.msConfig.get("rucioUrl"),  # Explicitly pass rucioUrl
        #    rucioAuthUrl=self.msConfig.get("rucioAuthUrl"),  # Explicitly pass rucioAuthUrl
        #    rucio = self.msRuleCleaner.rucio
        #    #auth_type="x509",  # Explicitly set the authentication type to X.509
        #    #cert=self.creds["client_cert"],  # X.509 certificate
        #    #key=self.creds["client_key"]     # X.509 key
        #)

        self.taskChainFile = getTestFile('data/ReqMgr/requests/Static/TaskChainRequestDump.json')
        self.stepChainFile = getTestFile('data/ReqMgr/requests/Static/StepChainRequestDump.json')
        self.reqRecordsFile = getTestFile('data/ReqMgr/requests/Static/BatchRequestsDump.json')
        with open(self.reqRecordsFile, encoding="utf-8") as fd:
            self.reqRecords = json.load(fd)
        with open(self.taskChainFile, encoding="utf-8") as fd:
            self.taskChainReq = json.load(fd)
        with open(self.stepChainFile, encoding="utf-8") as fd:
            self.stepChainReq = json.load(fd)
        super(MSRuleCleanerTest, self).setUp()

    def testGetLastStatusTransitionTime(self):
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        lastStatusTransition = self.msRuleCleaner._getLastStatusTransitionTime(wflow)
        self.assertEqual(lastStatusTransition, 1607359514)

    def testIsStatusAdvanceExpired(self):
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.assertTrue(self.msRuleCleaner._checkStatusAdvanceExpired(wflow))

    def testPipelineAgentBlock(self):
        # Test plineAgentBlock:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineAgentBlock'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1606723304},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1606723305},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1606723306},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1606723461},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1606723590},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1606723968},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1606724572},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1606724573},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1607018413},
                                               {'DN': '', 'Status': 'closed-out', 'UpdateTime': 1607347706},
                                               {'DN': '', 'Status': 'announced', 'UpdateTime': 1607359514}],
                         'RequestType': 'TaskChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineAgentBlock': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineAgentCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentCont': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineAgentCont'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1606723304},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1606723305},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1606723306},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1606723461},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1606723590},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1606723968},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1606724572},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1606724573},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1607018413},
                                               {'DN': '', 'Status': 'closed-out', 'UpdateTime': 1607347706},
                                               {'DN': '', 'Status': 'announced', 'UpdateTime': 1607359514}],
                         'RequestType': 'TaskChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineAgentCont': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineMSTrBlock(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        print(wflow)
        self.msRuleCleaner.plineMSTrBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineMSTrBlock': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineMSTrBlock'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '',
                                                'Status': 'new',
                                                'UpdateTime': 1606723304},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1606723305},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1606723306},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1606723461},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1606723590},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1606723968},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1606724572},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1606724573},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1607018413},
                                               {'DN': '', 'Status': 'closed-out', 'UpdateTime': 1607347706},
                                               {'DN': '', 'Status': 'announced', 'UpdateTime': 1607359514}],
                         'RequestType': 'TaskChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineMSTrBlock': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        assert False
        self.assertDictEqual(wflow, expectedWflow)
     
    def testPipelineMSTrBlock1(self):
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]})
        print(">>>>>>>>>",specUrl,self.testInit.couchUrl)
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, **self.queueParams)
        globalQ.queueWork(specUrl, "RerecoSpec", "teamA")
        gqList=globalQ.backend.getElementsForWorkflow(specName)

        workflowSpec = globalQ.backend.getWMSpec(specName)
        print("Workflow Specification:")
        #print(workflowSpec.data)
        workloadHelper = WMWorkloadHelper(workflowSpec.data)
        #workloadHelper.load(workflowSpec.data)
        # Extract workflow details
        #print("Workflow Details:")
        #print(workloadHelper.data)
        workflowDescription = {
            "RequestName": workloadHelper.name(),
            "RequestType": workloadHelper.getRequestType(),
            "InputDataset": workloadHelper.listInputDatasets(),
            "OutputDatasets": workloadHelper.listOutputDatasets(),
            "Tasks": workloadHelper.listAllTaskNames(),
            "Priority": workloadHelper.priority(),
            "Campaign": workloadHelper.getCampaign(),
            "SiteWhitelist": workloadHelper.getSiteWhitelist(),
            "SiteBlacklist": workloadHelper.getSiteBlacklist(),
            "PrepID": workloadHelper.getPrepID(),
            "AcquisitionEra": workloadHelper.getAcquisitionEra(),
            "ProcessingString": workloadHelper.getProcessingString(),
            "ProcessingVersion": workloadHelper.getProcessingVersion(),
            #"RequestStatus":workflowSpec.data.request.status
            #"TimePerEvent": workloadHelper.getTimePerEvent(),
            #"SizePerEvent": workloadHelper.getSizePerEvent(),
            #"EventsPerJob": workloadHelper.getEventsPerJob(),
            #"TotalEstimatedJobs": workloadHelper.getEstimatedJobCount(),
        }
        #
        print(json.dumps(workflowDescription, indent=2))

        print(json.dumps(self.taskChainReq, indent=2))
        
        wflow = MSRuleCleanerWflow(workflowDescription)
        print("Workflow for MSRuleCleaner:")
        print(json.dumps(wflow, indent=2))

        #specGenerator1 = StdSpecMaker()
        #specUrl1 = specGenerator1.createTaskChainSpec(self.taskChainReq)
        
        # taskChainReq is your dictionary (from ReqMgr2 or test)
        #workload = createTaskChain(taskChainReq)
        
        # workload is a WMWorkload instance
        #helper = WMWorkloadHelper(workload)
        
        # You can inspect it, e.g.,
        #print(helper.listAllTaskNames())
        
        # Write it to disk if you want to save the spec:
        #specFilePath = "/tmp/TaskChain-%s-Spec.xml" % taskChainReq["RequestName"]
        #helper.save(specFilePath)
        
        #print("Spec saved to", specFilePath)

        #specClass = StdSpecs.loadSpecClass(self.taskChainReq["RequestType"])

        # This returns the spec module (e.g., WMCore.WMSpec.StdSpecs.TaskChain)
        # and you use its factory function:
        #workload = specClass.factoryWorkloadConstruction(self.taskChainReq)
        
        # Wrap in a helper
        #helper = WMWorkloadHelper(workload)
        
        # Optionally save to file
        #specFile = "/tmp/%s-TaskChain-Spec.xml" % self.taskChainReq["RequestName"]
        #helper.save(specFile)
        
        #print("Spec created and saved to:", specFile)

        # Use the module's factory
        #factory = TaskChainWorkloadFactory()
        #workload = factory.factoryWorkloadConstruction('test',self.taskChainReq)

        #helper = WMWorkloadHelper(workload)

        #print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        #print(specUrl)
        #print(self.taskChainReq)
        #with open(specUrl, 'r') as specFile:
        #    specContent = specFile.read()
        #    print("Workflow Specification Content:")
        #    print(specContent)

        #print(gqList)
        # Test plineAgentCont
        #wflow = MSRuleCleanerWflow(self.taskChainReq)
        #self.msRuleCleaner.plineMSTrBlock1.run(wflow)
        #couchServer = CouchServer(self.testInit.couchUrl)
        #couchDb = couchServer.connectDatabase('workqueue_t')
        #specContent = couchDb.get(specName)
        #print(specContent)
        # Initialize WorkQueueDS with CouchDB URL and database name
        #wqApi = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
    
        # Query the database for the workflow specification
        #workflowSpec = wqApi.getWorkflowSpec(specName)
        #print(workflowSpec)
        
        #workflowSpec = globalQ.backend.getWMSpec(specName)
        #print(workflowSpec.data)
        #print(self.taskChainReq)
        
        assert False
    def testPipelineMSTrBlock2(self):
        
        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        workflowDescription = ReRecoWorkloadFactory.getTestArguments()
        workflowDescription['RequestName'] = specName
        print("Workflow Description:")
        print(json.dumps(workflowDescription, indent=2))
        wflow = MSRuleCleanerWflow(workflowDescription)
        print("Workflow for MSRuleCleaner:")
        print(json.dumps(wflow, indent=2))
        
        #Create ReRecoSpec as stored in GlobalQueue
        tmp = {"InputDataset": "/MinimumBias/ComissioningHI-v1/RAW"}
        #tmp = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
        #tmp = {"InputDataset": "/RelValProdMinBias/Integ_Test-RECOPROD1_TC_Drop_PhEDEx_Ext_HG2105_Val_Alanv12-v11/GEN-SIM-RECO"}

        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=tmp["InputDataset"])
        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        #self.msRuleCleaner.globalQueue = globalQ
        
        #Get the list of elements in GlobalQueue for the workflow (this uses backend that might not work if there is no direct access to CouchDB, which is often available at WMAgent)
        #gqList=globalQ.backend.getElementsForWorkflow(specName)
        #print("GlobalQueue List for Workflow:")
        #print(json.dumps(gqList, indent=2))
        #elements = globalQ.backend.getElements(WorkflowName=specName)
        #print("Elements in GlobalQueue:")
        #for e in elements:
        #    print(e.id, e['Status'], e["PercentComplete"], e["PercentSuccess"])

        #get the first element in the list for setup test
        #gq_element = gqList[0]
        #print("GlobalQueue Element used to setup test:")

        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #Use this instead of wqService.getWQElementsByWorkflow(workflowName) to have the element'id'
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        
        print("Elements in GlobalQueue:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
        #for e in elements:
        #    print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
        #elements=wqService.getWQElementsByWorkflow(specName)
        #print("Elements in GlobalQueue:")
        #for e in elements:
        #    #print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
        #    print(e["id"], e['Status'], e["PercentComplete"], e["PercentSuccess"])
        #    print("Element: ", json.dumps(e, indent=2))
            
    
        #let update the PercentComplete and PercentSuccess
        element_id = [elements[0]['id']]  # Get the first element's ID
        print("Updating element:", element_id)
        wqService.updateElements(*element_id, PercentComplete=100, PercentSuccess=100)
        # Re-fetch the elements to see the update
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        elements = data.get('rows', [])
        #elements=wqService.getWQElementsByWorkflow(specName)
        print("Updated Elements in GlobalQueue:")
        for e in elements:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
            #print(e["id"], e['Status'], e["PercentComplete"], e["PercentSuccess"])
        
        #now let try to create Rucio rule for the block
        #create a rule and inject it in wma_test account
        blockNames = list(elements[0]['value']['Inputs'].keys())  # Get the block name from the first element
        print("Block Name:", blockNames[0])
        ruleAttributes = {
            "names": blockNames[0],  # Block name
            "rseExpression": "FAKE",  # RSE expression
            "scope": "cms",  # Scope for CMS datasets
            "copies": 1,  # Number of copies
            "grouping": "DATASET",  # Grouping type (ALL, DATASET, NONE)
            "account": "wma_test",  # Rucio account
            "activity": "Production Input",  # Transfer activity
            "comment": "WMCore test block rule creation",  # Optional comment
            "ask_approval": False,  # Whether approval is required
            "lifetime": 3600,  # Lifetime of the rule in seconds
        }
        
        #rule_id = self.msRuleCleaner.rucio.createReplicationRule(
        #    #names=blockNames[0],
        #    names='/JetHT/Run2022A-v1/RAW#062f326a-7151-4cea-9cec-8cd1571dd4fa',
        #    rseExpression="T2_US_Nebraska",
        #    copies=1,
        #    grouping="DATASET",
        #    lifetime=3600,
        #    account="wma_test",
        #    ask_approval=False,
        #    activity="Production Input",
        #    comment="WMCore test block rule creation"
        #)

        #print("Created Rucio rule with ID:", rule_id)

        #try:
        #    rule_info = self.msRuleCleaner.rucio.getRule(rule_id[0])
        #    print("Rule exists:", rule_info)
        #    #now delete it
        #    self.msRuleCleaner.rucio.deleteRule(rule_id[0])
        #    print("Deleted Rucio rule with ID:", rule_id)
        #except RuleNotFound:
        #    print("Rule not found.")
        #except Exception as e:
        #    print("Error checking rule:", e)
        


        #elements = [x['id'] for x in data.get('rows', [])]
        #print("Elements in GlobalQueue:")
        #for e in elements:
        #    print(e.id, e['Status'], e["PercentComplete"], e["PercentSuccess"])

        #create a rule and inject it in wma_test account
        #ruleAttributes = {
        #    "names": blockName,  # Block name
        #    "rseExpression": "cms_type=real&rse_type=DISK",  # RSE expression
        #    "scope": "cms",  # Scope for CMS datasets
        #    "copies": 1,  # Number of copies
        #    "grouping": "DATASET",  # Grouping type (ALL, DATASET, NONE)
        #    "account": "wma_test",  # Rucio account
        #    "activity": "Production Output",  # Transfer activity
        #    "comment": "WMCore test block rule creation",  # Optional comment
        #    "ask_approval": False,  # Whether approval is required
        #    "lifetime": 3600,  # Lifetime of the rule in seconds
        #}

        #Now perform the cleaning
        self.msRuleCleaner.plineMSTrBlockGlobalQueue.run(wflow)
        print(json.dumps(wflow, indent=2))
#
        ##specName, returnType="spec"
        #print(json.dumps(self.taskChainReq, indent=2))
        #tmpReq = {}
        #for k,v in self.taskChainReq.items():
        #    if k in ['SiteBlacklist', 'RequestWorkflow', 'GracePeriod', 'TrustSitelists', 'OutputDatasets', \
        #             'BlockCloseMaxEvents', 'Dashboard', 'BlockCloseMaxFiles', 'SiteWhitelist', 'CustodialSites', \
        #             'HardTimeout', 'Override', 'InitialPriority', 'SubscriptionPriority', 'NonCustodialSites', \
        #             'AllowOpportunistic', 'Team', 'BlockCloseMaxSize', 'SoftTimeout', 'BlockCloseMaxWaitTime', 'TrustPUSitelists', 'OutputModulesLFNBases', '_id']:
        #        continue
        #    tmpReq[k] = v
        #tmpReq['RequestStatus'] = REQUEST_START_STATE
        #tmpReq['DbsUrl'] = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
        #tmpReq['Task1']['InputDataset'] = "/MinimumBias/ComissioningHI-v1/RAW"
        ##specUrl = self.specGenerator.createTaskChainSpec(specName="TaskChain", returnType="file",splitter = None,
        ##                                              assignKwargs={'SiteWhitelist':['T2_XX_SiteA']}, **tmpReq)
        #specUrl = self.specGenerator.createTaskChainSpec(specName="TaskChain", returnType="file",
        #                                              assignKwargs={'SiteWhitelist':['T2_XX_SiteA']})
        #globalQ = globalQueue(DbName='workqueue_t',
        #                      QueueURL=self.testInit.couchUrl,
        #                      UnittestFlag=True, **self.queueParams)
        #globalQ.queueWork(specUrl, "TaskChain", "teamA")
        #gqList=globalQ.backend.getElementsForWorkflow("TaskChain")

        #specName = "RerecoSpec"
        #specUrl = self.specGenerator.createReRecoSpec(specName, "file",
        #                                              assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]})
        #args = TaskChainWorkloadFactory.getTestArguments()
        #print(args)
        #args.update(self.taskChainReq)
        #print(args)

        #args["ConfigCacheID"] = createConfig(args["CouchDBName"])
        #factory = TaskChainWorkloadFactory()
        #spec = factory.factoryWorkloadConstruction("Test", args)

        #if assignKwargs:
        #    args = ReRecoWorkloadFactory.getAssignTestArguments()
        #    args.update(assignKwargs)
        #    spec.updateArguments(args)

        # Create workload object
        #from WMCore.Database.CMSCouch import CouchServer
        #factory = TaskChainWorkloadFactory()
        ##workload = factory("MyRequest", self.taskChainReq)
        #workload = factory("MyRequest", args)
        #workload.save("TaskChainTest.pkl")
        #server = CouchServer(os.environ["COUCHURL"])
        #db = server.connectDatabase("workqueue_t")
#
        ## Read pickled content
        #with open("TaskChainTest.pkl", "rb") as f:
        #    data = f.read()
        #
        ## Create a CouchDB document
        #doc = {"_id": "TaskChainTest", "type": "WMWorkload"}
        #db.commitOne(doc, attachment={"name": "spec.pkl", "data": data})
        
        # Wrap in helper
        #helper = WMWorkloadHelper(workload)

        #specUrl = helper.save(couchUrl=self.testInit.couchUrl,
        #              couchDbName="workqueue_t")
        
        #specUrl = uploadWorkloadSpec(
        #    couchUrl=self.testInit.couchUrl,
        #    workload=workload,
        #    couchDBName="workqueue_t"
        #)
#
        #globalQ = globalQueue(DbName='workqueue_t',
        #                      QueueURL=self.testInit.couchUrl,
        #                      UnittestFlag=True, **self.queueParams)
        
        #globalQ.queueWork(specUrl, "RerecoSpec", "teamA")
        
        #specGen = WorkQueueSpecGenerator(
        #            couchUrl="http://localhost:5984",
        #            couchDBName="workqueue_t"
        #)
        #specUrl = specGen.createTaskChainSpec(
        #    "TaskChainTest",
        #    "file",
        #    assignKwargs={"SiteWhitelist": ["T2_XX_SiteA"]}
        #)

        assert False

    def testPipelineMSTrCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineMSTrCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineMSTrCont': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineMSTrCont'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '',
                                                'Status': 'new',
                                                'UpdateTime': 1606723304},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1606723305},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1606723306},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1606723461},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1606723590},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1606723968},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1606724572},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1606724573},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1607018413},
                                               {'DN': '', 'Status': 'closed-out', 'UpdateTime': 1607347706},
                                               {'DN': '', 'Status': 'announced', 'UpdateTime': 1607359514}],
                         'RequestType': 'TaskChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineMSTrCont': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineArchive(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)

        # Try archival of a skipped workflow:
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        self.msRuleCleaner.plineAgentCont.run(wflow)

        # Try archival of a cleaned workflow:
        # NOTE: We should always expect an MSRuleCleanerArchivalSkip exception
        #       here because the 'enableRealRunMode' flag is set to False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': True,
                         'IsClean': True,
                         'IsLogDBClean': True,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineArchive',
                                          'plineAgentBlock',
                                          'plineAgentCont',
                                          'plineArchive'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1606723304},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1606723305},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1606723306},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1606723461},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1606723590},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1606723968},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1606724572},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1606724573},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1607018413},
                                               {'DN': '', 'Status': 'closed-out', 'UpdateTime': 1607347706},
                                               {'DN': '', 'Status': 'announced', 'UpdateTime': 1607359514}],
                         'RequestType': 'TaskChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineAgentBlock': [], 'plineAgentCont': []},
                         'TargetStatus': 'normal-archived',
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': "Not properly cleaned workflow: TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891"}
        self.assertDictEqual(wflow, expectedWflow)

        # Try archival of an uncleaned workflow
        wflow['CleanupStatus']['plineAgentBlock'] = False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)

    def testPipelineArchiveStepChain(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.stepChainReq)

        # Try archival of a skipped workflow:
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        self.msRuleCleaner.plineAgentCont.run(wflow)

        # Try archival of a cleaned workflow:
        # NOTE: We should always expect an MSRuleCleanerArchivalSkip exception
        #       here because the 'enableRealRunMode' flag is set to False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': None,
                         'IsArchivalDelayExpired': True,
                         'IsClean': True,
                         'IsLogDBClean': True,
                        'OutputDatasets': [
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep1-GENSIM_StepChain_Tasks_HG2011_Val_Todor_v1-v20/GEN-SIM',
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep1-GENSIM_StepChain_Tasks_HG2011_Val_Todor_v1-v20/LHE',
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep2-DIGI_StepChain_Tasks_HG2011_Val_Todor_v1-v20/GEN-SIM-RAW',
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep3-RECO_StepChain_Tasks_HG2011_Val_Todor_v1-v20/AODSIM'],
                         'ParentDataset': [],
                         'ParentageResolved': False,
                         'PlineMarkers': ['plineArchive',
                                          'plineAgentBlock',
                                          'plineAgentCont',
                                          'plineArchive'],
                         'RequestName': 'StepChain_Tasks_HG2011_Val_201029_112731_6371',
                         'RequestStatus': 'aborted-completed',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1603967251},
                                               {'DN': '', 'Status': 'assignment-approved', 'UpdateTime': 1603967253},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1603967254},
                                               {'DN': '', 'Status': 'aborted', 'UpdateTime': 1604931587},
                                               {'DN': '', 'Status': 'aborted-completed', 'UpdateTime': 1604931737}],
                         'RequestType': 'StepChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineAgentBlock': [], 'plineAgentCont': []},
                         'TargetStatus': 'aborted-archived',
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ("Not properly cleaned workflow: StepChain_Tasks_HG2011_Val_201029_112731_6371"
                                                     " - 'ParentageResolved' flag set to false.\n"
                                                     "Not properly cleaned workflow: StepChain_Tasks_HG2011_Val_201029_112731_6371\n"
                                                     "Not properly cleaned workflow: StepChain_Tasks_HG2011_Val_201029_112731_6371"
                                                     " - 'ParentageResolved' flag set to false.")}
        self.assertDictEqual(wflow, expectedWflow)

        # Try archival of an uncleaned workflow
        wflow['CleanupStatus']['plineAgentBlock'] = False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)

    def testRunning(self):
        result = self.msRuleCleaner._execute(self.reqRecords)
        self.assertEqual(result, (3, 2, 0, 0))

    def testCheckClean(self):
        # NOTE: All of the bellow checks are well visualized at:
        #       https://github.com/dmwm/WMCore/pull/10023#discussion_r520070925

        # 1. MaskList shorter than FlagList
        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True, 'plineMStrCont': False},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont']}
        self.assertTrue(self.msRuleCleaner._checkClean(wflowFlags))

        wflowFlags = {'CleanupStatus': {'plineAgentBlock': False, 'plineAgentCont': True, 'plineMStrCont': True},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont']}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

        # 2. MaskList Empty
        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                      'PlineMarkers': []}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

        # 3. MaskList longer than FlagList
        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont', 'plineMStrCont', 'plineArchive']}
        self.assertTrue(self.msRuleCleaner._checkClean(wflowFlags))

        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': False},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont', 'plineMStrCont', 'plineArchive']}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

        # 4. FlagList Empty
        wflowFlags = {'CleanupStatus': {},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont']}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))


if __name__ == '__main__':
    unittest.main()
