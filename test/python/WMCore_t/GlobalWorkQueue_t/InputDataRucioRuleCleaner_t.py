from WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner import InputDataRucioRuleCleaner

from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

import cherrypy
from cherrypy.process import plugins

# WMCore modules
from WMCore.Services.Rucio import Rucio
from rucio.common.exception import RuleNotFound

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

import json
# system modules
import os
import time

import unittest
from unittest.mock import MagicMock

class DummyREST:
    def __init__(self):
        self.logger = None  # Optional: add logger if needed
        self.config = None

#MSRuleCleaner requires plain dictionary to be passed as config while CherryPyPeriodic requires attributes, so we create a DictWithAttrs class
class DictWithAttrs(dict):
        def __getattr__(self, key):
            try:
                return self[key]
            except KeyError:
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'")

class InputDataRucioRuleCleanerTest(EmulatedUnitTestCase):

    def setUp(self):   
        self.msConfig = {"verbose": True,
                         "interval": 1 * 60,
                         "services": ['ruleCleaner'],
                         #"rucioWmaAcct": 'wma_test', #wma_transfer for transferor
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
                         'enableRealMode': True}
        
        #The Rucio account used for InputDataRucioRuleCleaner is the same as the one used for WMAgent                 
        #self.msConfig['rucioAccount'] = self.msConfig['rucioWmaAcct']

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
           
        self.queueParams = {}
        self.queueParams['log_reporter'] = "Services_WorkQueue_Unittest"
        self.queueParams['rucioAccount'] = self.msConfig['rucioAccount']
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"
        self.queueParams['_internal_name'] = 'GlobalWorkQueueTest'
        self.queueParams['log_file'] = 'test.log'

                
        print("X509_USER_CERT:", os.getenv("X509_USER_CERT"))
        print("X509_USER_KEY:", os.getenv("X509_USER_KEY"))

        # Mock the REST and config objects
        self.mockRest = MagicMock()
        #self.mockConfig = MagicMock() #can not be used as it does not support plain dictionary access used in MSRuleCleaner
        #self.mockConfig._internal_name = "GlobalWorkQueueTest"
        #self.mockConfig.log_file = "test.log"
        #self.mockConfig.reqmgr2Url = self.msConfig['reqmgr2Url']
        #self.mockConfig.queueParams = self.queueParams
        #self.mockConfig.cleanInputDataRucioRuleDuration = 60  # Example duration in seconds
        #
        #self.mockConfig.__getitem__.side_effect = lambda key: {
        #  'reqmgr2Url': 'https://cmsweb.cern.ch/reqmgr2',
        #  "logDBUrl": "https://cmsweb-testbed.cern.ch/couchdb/wmstats_logdb"
        #}.get(key, MagicMock())  # Provide fallback if other keys accessed
        
        # Create object with attributes
        self.config_obj = DictWithAttrs(self.msConfig)
        #additional attributes needed by cherrypy periodic task
        self.config_obj._internal_name = "GlobalWorkQueueTest"
        self.config_obj.log_file = "test.log"
        #additional attributes needed by global workqueue
        self.config_obj.queueParams = self.queueParams
        #duration for the periodic task
        self.config_obj.cleanInputDataRucioRuleDuration = 10  # Example duration in seconds

        super(InputDataRucioRuleCleanerTest, self).setUp()
    
    def testInputDataRucioRuleCleaner(self):
        """
        Test the InputDataRucioRuleCleaner task
        """
        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        inputdataset = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
                
        #Create ReRecoSpec as stored in GlobalQueue       
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])          
        
        #cleaner = InputDataRucioRuleCleaner(rest=self.mockRest, config=self.config_obj)
        cleaner = InputDataRucioRuleCleaner(rest=DummyREST(), config=self.config_obj)
        
        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, logger=cleaner.logger, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        cleaner.globalQ = globalQ

        #Make MSRuleCleaner
        msRuleCleaner = MSRuleCleaner(self.config_obj,logger=cleaner.logger)
        msRuleCleaner.resetCounters()
        msRuleCleaner.rucio = Rucio.Rucio(self.msConfig['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)
        
        cleaner.msRuleCleaner = msRuleCleaner        
        
        #Let try to modify the element in GlobalQueue to have PercentComplete and PercentSuccess set to 100
        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #Use this instead of wqService.getWQElementsByWorkflow(workflowName) to have the element'id'
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        
        print("Elements in GlobalQueue:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
        
        #let update the PercentComplete and PercentSuccess and Status='Done' of the first elements
        element_id = [elements[0]['id']]  # Get the first element's ID
        print("Updating element:", element_id)
        wqService.updateElements(*element_id, PercentComplete=100, PercentSuccess=100, Status='Done')
        
        #create a rule and inject it in wma_test account
        blockNames = list(elements[0]['value']['Inputs'].keys())  # Get the block name from the first element
        print("Block Name:", blockNames[0])
        
        #need to create rule here otherwise we do not know which element was updated since the element order changes each time re-fetching (of course we can use the element_id)
        rule_id = cleaner.msRuleCleaner.rucio.createReplicationRule(
            names=blockNames[0],
            rseExpression="T2_US_Nebraska",
            copies=1,
            grouping="DATASET",
            lifetime=360,
            account="wma_test",
            ask_approval=False,
            activity="Production Input",
            comment="WMCore test block rule creation"
        )

        print("Created Rucio rule with ID:", rule_id)
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        print(rule_info)

        # Re-fetch the elements to see the update
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        #element order changes each time, so we need to re-fetch the elements
        elements = data.get('rows', [])
        #elements=wqService.getWQElementsByWorkflow(specName)
        print("Updated Elements in GlobalQueue:")
        for e in elements:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
            #print(e["id"], e['Status'], e["PercentComplete"], e["PercentSuccess"])
              
        
        #print("Available dataset in Rucio")
        #datasets = self.msRuleCleaner.rucio.cli.list_dids(scope='cms', filters={'name': '/JetHT*'}, type='dataset')
        ##datasets = self.msRuleCleaner.rucio.cli.list_dids(scope='cms', filters={'name': '/MinimumBias*'}, type='dataset')
        ##datasets = self.msRuleCleaner.rucio.cli.list_dids(scope='cms', filters={'name': '/JetHT/*/RAW'}, type='dataset')
        #for ds in datasets:
        #    print(ds)

        # Mock the Rucio listDataRules method
        #cleaner.rucio.listDataRules = lambda dataset, account: [{'id': 'rule1', 'state': 'OK'}]

        # Call the cleanRucioRules method
        #results = cleaner.cleanRucioRules(self.msConfig['rucioWmaAcct'])
        results = cleaner.cleanRucioRules(self.config_obj)
        print("Results from cleanRucioRules:", json.dumps(results, indent=2))
        #now make sure the rule is cleaned
        #keep deleting until success or timeout
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        delResult = False
        timeleft = 0
        start_time = time.time()
        while rule_info and not delResult and timeleft < 300:
            #now delete it
            print('Manually deleting rucio rules: ', blockNames[0], cleaner.msRuleCleaner.rucio.listDataRules(blockNames[0], account=self.msConfig['rucioAccount']))
            delResult = cleaner.msRuleCleaner.rucio.deleteRule(rule_id[0])
            print("Deleted Rucio rule with ID:", rule_id, delResult)
            if delResult: break
            time.sleep(60)
            timeleft = time.time() - start_time
        
        if not delResult and timeleft >= 300:
            print("Failed to delete the rule after 5 minutes, exiting...")
        
        #try:
        #    rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        #    #print("Rule exists:", json.dumps(rule_info, indent=2))
        #    #now delete it
        #    print('List of rucio rules: ', blockNames[0], cleaner.msRuleCleaner.rucio.listDataRules(blockNames[0], account=self.msConfig['rucioAccount']))
        #    delResult = cleaner.msRuleCleaner.rucio.deleteRule(rule_id[0])
        #    print("Deleted Rucio rule with ID:", rule_id, delResult)
        #except RuleNotFound:
        #    print("Rule not found.")
        #except Exception as e:
        #    print("Error checking rule:", e)


        # Check that the method executed without errors
        self.assertTrue(results['CleanupStatus']['Current'])
        # You can add assertions here to verify the expected behavior)

    def testInputDataRucioRuleCleanerWithThreading(self):
        """
        Test the InputDataRucioRuleCleaner task with threading
        """
        
        #cleaner = InputDataRucioRuleCleaner(rest=self.mockRest, config=self.config_obj)
        cleaner = InputDataRucioRuleCleaner(rest=DummyREST(), config=self.config_obj)
        
        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        inputdataset = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
                
        #Create ReRecoSpec as stored in GlobalQueue       
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])

        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, logger=cleaner.logger, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        cleaner.globalQ = globalQ

        #Make MSRuleCleaner
        msRuleCleaner = MSRuleCleaner(self.config_obj,logger=cleaner.logger)
        msRuleCleaner.resetCounters()
        msRuleCleaner.rucio = Rucio.Rucio(self.msConfig['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)
        cleaner.msRuleCleaner = msRuleCleaner        
        
        # Start CherryPy engine
        print('CherryPy engine starting...')
        cherrypy.engine.start()
        time.sleep(5)  # Give CherryPy a moment to start and modify the element in GlobalQueue after 5 seconds and before the next run of the periodic task
        
        #Let try to modify the element in GlobalQueue to have PercentComplete and PercentSuccess set to 100
        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #Use this instead of wqService.getWQElementsByWorkflow(workflowName) to have the element'id'
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        
        print("Elements in GlobalQueue:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
        
        #let update the PercentComplete and PercentSuccess and Status='Done' of the first elements
        element_id = [elements[0]['id']]  # Get the first element's ID
        print("Updating element:", element_id)
        wqService.updateElements(*element_id, PercentComplete=100, PercentSuccess=100, Status='Done')
        
        #create a rule and inject it in wma_test account
        blockNames = list(elements[0]['value']['Inputs'].keys())  # Get the block name from the first element
        print("Block Name:", blockNames[0])
        
        #need to create rule here otherwise we do not know which element was updated since the element order changes each time re-fetching (of course we can use the element_id)
        rule_id = cleaner.msRuleCleaner.rucio.createReplicationRule(
            names=blockNames[0],
            rseExpression="T2_US_Nebraska",
            copies=1,
            grouping="DATASET",
            lifetime=360,
            account="wma_test",
            ask_approval=False,
            activity="Production Input",
            comment="WMCore test block rule creation"
        )

        print("Created Rucio rule with ID:", rule_id)
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        print(rule_info)

        # Re-fetch the elements to see the update
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        #element order changes each time, so we need to re-fetch the elements
        elements = data.get('rows', [])
        #elements=wqService.getWQElementsByWorkflow(specName)
        print("Updated Elements in GlobalQueue:")
        for e in elements:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
            #print(e["id"], e['Status'], e["PercentComplete"], e["PercentSuccess"])

        #total waiting time ~25 (5+20) seconds to let the periodic task run 3 times (0, 10, 20) total. 
        #The second time it runs it should find the updated element and clean the rule.
        #Nothing happens in the third run.      
        time.sleep(20)
        
        #print("Available dataset in Rucio")
        #datasets = self.msRuleCleaner.rucio.cli.list_dids(scope='cms', filters={'name': '/JetHT*'}, type='dataset')
        ##datasets = self.msRuleCleaner.rucio.cli.list_dids(scope='cms', filters={'name': '/MinimumBias*'}, type='dataset')
        ##datasets = self.msRuleCleaner.rucio.cli.list_dids(scope='cms', filters={'name': '/JetHT/*/RAW'}, type='dataset')
        #for ds in datasets:
        #    print(ds)

        # Mock the Rucio listDataRules method
        #cleaner.rucio.listDataRules = lambda dataset, account: [{'id': 'rule1', 'state': 'OK'}]

        # Call the cleanRucioRules method
        #results = cleaner.cleanRucioRules(self.msConfig['rucioAccount'])
        #print("Results from cleanRucioRules:", json.dumps(results, indent=2))
        
        #cherrypy.engine.block()
        print('CherryPy engine exiting...')
        cherrypy.engine.exit()

        #now continuously check the rule status until it is cleaned and exit after 10 minutes
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        timeleft = 0
        start_time = time.time()
        while rule_info and timeleft < 600:  # Check for 10 minutes
            print("Rule still exists:", rule_id[0], rule_info)
            time.sleep(60)
            timeleft = time.time() - start_time
            rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        
        rule_info_for_check = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        
        #now make sure the rule should be cleaned (note that the rule may not be cleaned immediately after the periodic task execution (~5 mins), but we just clean it again here)
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        delResult = False
        if not rule_info:
            print("Rule not found.")
        
        #keep deleting until success or timeout
        timeleft = 0
        start_time = time.time()
        while rule_info and not delResult and timeleft < 300:
            #now delete it
            print('Manually deleting rucio rules: ', blockNames[0], cleaner.msRuleCleaner.rucio.listDataRules(blockNames[0], account=self.msConfig['rucioAccount']))
            delResult = cleaner.msRuleCleaner.rucio.deleteRule(rule_id[0])
            print("Deleted Rucio rule with ID:", rule_id, delResult)
            if delResult: break
            time.sleep(60)
            timeleft = time.time() - start_time
        
        if not delResult and timeleft >= 300:
            print("Failed to delete the rule after 5 minutes, exiting...")
        
        self.assertTrue(not rule_info_for_check, "Rule not deleted successfully after periodic task execution.")

        #while rule_info:
        #    print("Rule still exists:", rule_info)
        #    rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        #    time.sleep(60)  # Wait for a while before checking again

        # Check that the method executed without errors
        #self.assertTrue(results['CleanupStatus']['Current'])
        # You can add assertions here to verify the expected behavior)
        
        #self.assertTrue(True, "Periodic task started successfully, check the logs for details")
        
if __name__ == '__main__':
    unittest.main()