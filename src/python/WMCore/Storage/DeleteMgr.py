#!/usr/bin/env python
"""
DeleteMgr

Util class to provide delete functionality as an interface object.

Based on StageOutMgr class

"""
from __future__ import print_function

from builtins import object
from future.utils import viewitems

import logging

from WMCore.Storage.Registry import retrieveStageOutImpl
# do we want seperate exceptions - for the moment no
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.WMException import WMException


from WMCore.Storage.SiteLocalConfig import stageOutStr

from WMCore.Storage.RucioFileCatalog import storageJsonPath,readRFC 

class DeleteMgrError(WMException):
    """
    _DeleteMgrError_

    Specific exception class to work out file deletion exception details
    """

    def __init__(self, message, **data):
        WMException.__init__(self, message, **data)
        self.data.setdefault("ErrorCode", 60313)
        self.data.setdefault("ErrorType", self.__class__.__name__)


class DeleteMgr(object):
    """
    _DeleteMgr_

    Object that can be used to delete a set of files
    using TFC or an override.

    """

    def __init__(self, **overrideParams):
        
        self.override = False
        self.logger = overrideParams.pop("logger", logging.getLogger())
        self.overrideParams = overrideParams
        if overrideParams != {}:
            self.override = True

        self.bypassImpl = False

        self.stageOuts_rfcs = [] #pairs of stageOut and Rucio file catalog

        self.overrideConf = None #will be initialized later in initialiseOverride

        self.numberOfRetries = 3
        self.retryPauseTime = 600

        from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if not self.override:
            self.siteCfg = loadSiteLocalConfig()

        if self.override:
            self.initialiseOverride()
        else:
            self.initialiseSiteConf()

    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and TFC

        """
        
        self.stageOuts = self.siteCfg.stageOuts

        msg = ""
        msg += "There are %s stage out definitions.\n" % len(self.stageOuts)
        
        for stageOut in self.stageOuts:
            msg = ""
            for k in ['phedex-node','command','storageSite','volume','protocol']:
                v = stageOut.get(k)
                if v is None:
                    msg += "Unable to retrieve "+k+" of this stageOut: \n"
                    msg += stageOutStr(stageOut) + "\n"
                    msg += "From site config file.\n"
                    msg += "Continue to the next stageOut.\n"
                    logging.info(msg)
                    continue

            storageSite = stageOut.get("storageSite")
            volume = stageOut.get("volume")
            protocol = stageOut.get("protocol")
            command = stageOut.get("command")
            pnn = stageOut.get("phedex-node")
            
            msg += "\tStage out to : %s using: %s \n" % (pnn, command)
            
            try:
                aPath = storageJsonPath(self.siteCfg.siteName,self.siteCfg.subSiteName,storageSite)
                rfc = readRFC(aPath,storageSite,volume,protocol)
                self.stageOuts_rfcs.append((stageOut,rfc))
                msg += "Rucio File Catalog has been loaded:\n"
                msg += str(self.stageOuts_rfcs[-1][1])
            except Exception as ex:
                msg += "Unable to load Rucio File Catalog:\n"
                msg += "This stage out will not be attempted:\n"
                msg += stageOutStr(stageOut) + '\n'
                msg += str(ex)
                logging.info(msg)
                continue

        #no Rucio file catalog is initialized
        if not self.stageOuts_rfcs:
            raise StageOutInitError(msg)

        self.logger.info(msg)
        
        return

    def initialiseOverride(self):
        """
        _initialiseOverride_

        Extract and verify that the Override parameters are all present

        """
        self.overrideConf = {
            "command": None,
            "option": None,
            "phedex-node": None,
            "lfn-prefix": None,
        }

        try:
            self.overrideConf['command'] = self.overrideParams['command']
            self.overrideConf['phedex-node'] = self.overrideParams['phedex-node']
            self.overrideConf['lfn-prefix'] = self.overrideParams['lfn-prefix']
        except Exception as ex:
            msg = "Unable to extract override parameters from config:\n"
            msg += str(ex)
            raise StageOutInitError(msg)
        if 'option' in self.overrideParams:
            if len(self.overrideParams['option']) > 0:
                self.overrideConf['option'] = self.overrideParams['option']
            else:
                self.overrideConf['option'] = ""

        msg = "=======Delete Override Initialised:================\n"
        for key, val in viewitems(self.overrideConf):
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"

        self.logger.info(msg)
        
        return

    def __call__(self, fileToDelete):
        """
        _operator()_

        Use call to delete a file

        """
        self.logger.info("==>Working on file: %s" % fileToDelete['LFN'])

        lfn = fileToDelete['LFN']

        deleteSuccess = False
        
        if not self.override:
            logging.info("===> Attempting to delete with %s stage outs", len(self.stageOuts))
            for stageOut_rfc in self.stageOuts_rfcs:
                if not deleteSuccess:
                    try:
                        fileToDelete['PNN'] = stageOut_rfc[0]['phedex-node']
                        fileToDelete['PFN'] = self.deleteLFN(lfn, stageOut_rfc)
                        deleteSuccess = True
                        break
                    except Exception as ex:
                        continue
        else:
            logging.info("===> Attempting stage outs from override")
            try:
                fileToDelete['PNN'] = self.overrideConf['phedex-node']
                fileToDelete['PFN'] = self.deleteLFN_override(lfn)
                deleteSuccess = True
            except Exception as ex:
                self.logger.error("===> Local file deletion failure. Exception:\n%s", str(ex))
        if deleteSuccess:
            msg = "===> Delete Successful:\n"
            msg += "====> LFN: %s\n" % fileToDelete['LFN']
            msg += "====> PFN: %s\n" % fileToDelete['PFN']
            msg += "====> PNN:  %s\n" % fileToDelete['PNN']
            self.logger.info(msg)
            return fileToDelete
        else:
            msg = "Unable to delete file:\n"
            msg += fileToDelete['LFN']
            raise StageOutFailure(msg, **fileToDelete)
    
    def deleteLFN(self, lfn, stageOut_rfc):
        """
        deleteLFN

        Given the lfn and an stageOut Rucio file catalog pair, invoke the delete

        """
        from WMCore.Storage.StageOutMgr import searchRFC 
        command = stageOut_rfc[0]['command']
        pfn = searchRFC(stageOut_rfc[1],lfn)

        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN=lfn, STAGEOUT=stageOutStr(stageOut_rfc[0]))

        return self.deletePFN(pfn, lfn, command)
    

    def deleteLFN_override(self, lfn):
        """
        deleteLFN_override

        Given the lfn invoke the delete using override config

        the follwoing params should be defined for override
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred
        """

        command = self.overrideConf['command']
        pfn = None
        if self.overrideConf['lfn-prefix'] is not None:
            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)

        if pfn is None:
            msg = "Unable to match lfn to pfn using lfn-prefix: \n %s" % lfn
            raise StageOutFailure(msg, LFN=lfn, LFNPREFIX=self.overrideConf['lfn-prefix'])

        return self.deletePFN(pfn, lfn, command)

    def deletePFN(self, pfn, lfn, command):
        """
        Delete the given PFN
        """
        try:
            impl = retrieveStageOutImpl(command)
        except Exception as ex:
            msg = "Unable to retrieve impl for file deletion in:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            if not self.bypassImpl:
                impl.removeFile(pfn)
        except Exception as ex:
            self.logger.error("Failed to delete file: %s", pfn)
            ex.addInfo(Protocol=command, LFN=lfn, TargetPFN=pfn)
            raise ex

        return pfn
